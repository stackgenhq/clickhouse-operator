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
	"strings"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// ConvergeHostsArgs is the input to ConvergePeerHosts: everything the
// hosts sweep needs that is not in the provider/lease table (the spec
// prefixes come from chinit.yaml, the lease table + DNS zone from the
// settled identity).
type ConvergeHostsArgs struct {
	// LeaseTable is the node's own lease table (the claim scan reads
	// the same table the boot read).
	LeaseTable string
	// WriterPrefix/ReaderPrefix are the hostname prefixes the claims
	// must match to earn a hosts line (defaults writer-/reader-).
	WriterPrefix string
	ReaderPrefix string
	// DNSZoneName is the FQDN suffix the line's third field carries
	// (writer-0 writer-0.zone) so FQDN resolution benefits too.
	DNSZoneName string
}

// ConvergePeerHosts is the periodic /etc/hosts convergence for server
// nodes: sweep every prior writer/reader line, then append one line
// per live lease claim.
//
// Why periodic, not boot-only: the bash-era step rendered hosts ONCE
// at boot from the live claims — but an ASG fleet churns (the
// 2026-11-15 finding: the reader booted mid-churn with zero live
// writer claims, the 0s boot step wrote an empty hosts file with
// onceFatal semantics, and every bare-hostname part fetch died on
// DNS_ERROR for hours until a human noticed). The replication queue
// addresses fetch sources by the SHORT registered name ({replica}
// macro), and systemd-resolved's stub resolver does not answer short
// names — /etc/hosts is the only reliable short-name path. This runs
// on the lease-renew timer (every 5 minutes), so a hosts file written
// empty at boot converges within one renewal cycle of the writers
// re-claiming.
//
// Failure semantics: best-effort, never fatal — the same contract as
// the boot step (a listing outage must not take the lease renewal
// down with it; the renewal is the more critical of the two).
func ConvergePeerHosts(ctx context.Context, provider port.Provider, args ConvergeHostsArgs) error {
	lister := port.AsLeaseLister(provider)
	if lister == nil || args.LeaseTable == "" {
		// Same degradation as the boot step: a provider without the
		// lease-listing capability (or a state file without the
		// table) keeps whatever DNS gives; never a fatal error.
		slog.Warn("peer-hosts: no lease-listing capability; short names resolve via DNS")
		return nil
	}
	claims, err := lister.ListActiveClaims(ctx, port.ListClaimsRequest{Table: args.LeaseTable})
	if err != nil {
		// Best-effort: a listing outage must not abort the renewal.
		slog.Warn("peer-hosts: live-claims listing failed; keeping current hosts file", "err", err)
		return nil
	}
	return writePeerHostsFile(claims, args)
}

// writePeerHostsFile is the sweep-and-append core shared by the boot
// step and the periodic convergence: stale writer/reader lines are
// swept first (a re-run must converge on the CURRENT claims, not
// preserve whatever an older boot wrote — the old contains-check kept
// a wrong entry forever), then one line per live claim.
//
// The hosts path is a seam the tests redirect (never /var/lib
// privileges in the suite). PeerHostsPath exports the seam for the
// app-layer specs (the lease-renew timer drives the convergence —
// its specs must redirect the same path the service writes).
var peerHostsPath = func() string { return "/etc/hosts" }

// PeerHostsPath returns the current hosts-file path (test seam).
func PeerHostsPath() func() string { return peerHostsPath }

// SetPeerHostsPath redirects the hosts-file path (test seam).
func SetPeerHostsPath(f func() string) { peerHostsPath = f }

func writePeerHostsFile(claims []port.Claim, args ConvergeHostsArgs) error {
	writerPrefix := args.WriterPrefix
	if writerPrefix == "" {
		writerPrefix = "writer-"
	}
	readerPrefix := args.ReaderPrefix
	if readerPrefix == "" {
		readerPrefix = "reader-"
	}

	if existing, err := os.ReadFile(peerHostsPath()); err == nil {
		var kept []string
		for _, line := range strings.Split(string(existing), "\n") {
			stale := false
			for _, prefix := range []string{writerPrefix, readerPrefix} {
				for _, field := range strings.Fields(line) {
					if strings.HasPrefix(field, prefix) {
						stale = true
						break
					}
				}
				if stale {
					break
				}
			}
			if !stale {
				kept = append(kept, line)
			}
		}
		if err := os.WriteFile(peerHostsPath(), []byte(strings.Join(kept, "\n")+"\n"), 0644); err != nil {
			return fmt.Errorf("rewrite %s: %w", peerHostsPath(), err)
		}
	} else {
		return fmt.Errorf("read %s: %w", peerHostsPath(), err)
	}

	var b strings.Builder
	entries := 0
	for _, claim := range claims {
		writer := strings.HasPrefix(claim.Hostname, writerPrefix)
		reader := strings.HasPrefix(claim.Hostname, readerPrefix)
		if !writer && !reader {
			continue // foreign row (backup-0, keeper-N): not fetch sources
		}
		if claim.IP == "" {
			continue
		}
		entries++
		if args.DNSZoneName != "" {
			fmt.Fprintf(&b, "%s %s %s.%s\n", claim.IP, claim.Hostname, claim.Hostname, args.DNSZoneName)
		} else {
			fmt.Fprintf(&b, "%s %s\n", claim.IP, claim.Hostname)
		}
	}
	if b.Len() == 0 {
		return nil
	}
	f, err := os.OpenFile(peerHostsPath(), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open %s: %w", peerHostsPath(), err)
	}
	if _, err := f.WriteString(b.String()); err != nil {
		_ = f.Close()
		return fmt.Errorf("append %s: %w", peerHostsPath(), err)
	}
	_ = f.Close()
	slog.Info("peer-hosts: converged", "entries", entries)
	return nil
}
