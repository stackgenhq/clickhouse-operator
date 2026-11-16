// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chinit/infra/operator"
)

// remoteServersFile is the render's cluster-inventory output — the
// file this convergence owns.
const remoteServersFile = "chop-generated-remote_servers.xml"

// remoteServersDir is the SEAM the tests redirect (never /etc
// privileges in the suite): where the convergence installs the file.
var remoteServersDir = func() string { return ConfigDir }

// remoteServersBootstrapDir is the seam for the credential file read
// (BootstrapDir in production).
var remoteServersBootstrapDir = func() string { return BootstrapDir }

// ConvergeRemoteServers re-renders the node's remote_servers section
// from the CURRENT live-claims inventory and installs it ONLY when
// the content drifted — then hot-reloads ClickHouse (remote_servers
// changes apply live; no restart, verified against system.clusters).
//
// Why periodic, not boot-only: remote_servers is rendered at boot from
// the lease-table snapshot of the moment, and fleet churn after the
// boot (a scale-in removing a peer, a scale-out adding one) leaves the
// rendered file naming peers that no longer exist — ON CLUSTER DDL
// fan-outs and clusterAllReplicas reads keep retrying the dead names
// until the next boot/refresh re-renders (the 2026-11-15 soak finding:
// a scaled-in reader's entry wedged cluster-wide DDL for hours). The
// /etc/hosts and DNS convergences heal the RESOLUTION of the names;
// this convergence heals the LIST itself.
//
// The render is the SAME one the boot runs (renderNodeConfig's
// inventory half — live-claims discovery, the vacant-slot filter,
// credential injection): a converged file is byte-identical to what
// the next boot --refresh would write, so the convergence can never
// fight the render contract.
//
// Failure semantics: best-effort, never fatal — the same contract as
// the hosts/DNS convergences. A render or install failure logs and
// keeps the current file; the next tick retries. The RELOAD is only
// issued after a successful install (reloading an unchanged or
// failed file is pointless — and a reload failure leaves the file in
// place for the server's own config-reload watchdog).
func (b *Boot) ConvergeRemoteServers(ctx context.Context) error {
	if b.Spec.Role == model.RoleKeeper {
		return nil // keepers render no CHI section
	}

	content, err := b.renderRemoteServersContent(ctx)
	if err != nil {
		slog.Warn("remote-servers: render failed; keeping the installed file", "err", err)
		return nil
	}
	dest := filepath.Join(remoteServersDir(), remoteServersFile)
	installed, err := os.ReadFile(dest)
	if err == nil && string(installed) == content {
		return nil // converged — nothing to install, nothing to reload
	}
	if err := os.MkdirAll(remoteServersDir(), 0o755); err != nil {
		slog.Warn("remote-servers: mkdir failed", "err", err)
		return nil
	}
	if err := os.WriteFile(dest, []byte(content), 0o640); err != nil {
		slog.Warn("remote-servers: install failed; keeping the previous file", "err", err)
		return nil
	}
	slog.Info("remote-servers: converged (installed + reload pending)",
		"path", dest)
	// Hot-reload: ClickHouse re-reads remote_servers live (verified:
	// system.clusters reflects a config.d file change after RELOAD
	// CONFIG — no restart). Best-effort; a failure leaves the file in
	// place and the server picks it up on its own next reload/restart.
	if out, err := runCommand(ctx, "clickhouse-client", "--query", "SYSTEM RELOAD CONFIG"); err != nil {
		slog.Warn("remote-servers: reload failed (the file is installed; the server reloads it on restart)",
			"err", err, "out", strings.TrimSpace(out))
	}
	return nil
}

// renderRemoteServersContent produces the exact remote_servers XML the
// node renderer emits — the same generator, namer, inventory and
// credential-injection path renderNodeConfig runs — but as CONTENT
// only, so the convergence can compare before installing instead of
// touching the managed tree on every tick.
//
// Why content-first (not a dry render + installPlan): the convergence
// must NOT rewrite users.d or the host sections on its tick — those
// are the refresher's contract (config_refresher invokes boot
// --refresh for them). A byte-compare against the installed file is
// the minimal write discipline.
func (b *Boot) renderRemoteServersContent(ctx context.Context) (string, error) {
	// The chi.yaml read goes through the bootstrap-dir SEAM (tests
	// redirect it; production reads the fetched bundle's copy) — the
	// same file LoadCHI reads, without LoadCHI's constant path.
	data, err := os.ReadFile(filepath.Join(remoteServersBootstrapDir(), "chi.yaml"))
	if err != nil {
		return "", fmt.Errorf("read chi.yaml: %w", err)
	}
	loader := operator.NewSpecLoader()
	crAny, err := loader.LoadCHI(ctx, port.LoadCHIRequest{
		Spec: domain.SpecReference{
			Name: b.Spec.ClusterName,
			Data: data,
		},
	})
	if err != nil {
		return "", fmt.Errorf("load chi.yaml: %w", err)
	}
	cr := crAny
	hosts, note := b.clusterHostsWithSource()
	if note != "" {
		slog.Warn(note)
	}
	replicas := map[string]string{}
	for _, h := range hosts {
		replicas[h.Name] = strings.SplitN(h.Address, ".", 2)[0]
	}
	request := port.RenderCHIRequest{
		NormalizedCR: cr,
		HostName:     b.Identity.CRName,
		Hosts:        hosts,
		Replicas:     replicas,
		OutputDir:    ConfigDir,
		DryRun:       true, // the convergence owns the write, not the renderer
	}
	if pw, err := os.ReadFile(filepath.Join(remoteServersBootstrapDir(), "default-password")); err == nil {
		request.RemoteUser = "default"
		request.RemotePassword = strings.TrimSpace(string(pw))
	}
	renderer := operator.NewConfigRenderer()
	rendered, err := renderer.RenderCHI(ctx, request)
	if err != nil {
		return "", fmt.Errorf("render remote_servers: %w", err)
	}
	for _, file := range rendered {
		if filepath.Base(file.Path) == remoteServersFile {
			return file.Content, nil
		}
	}
	return "", fmt.Errorf("render produced no %s", remoteServersFile)
}
