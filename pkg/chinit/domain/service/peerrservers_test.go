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
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// This file pins ConvergeRemoteServers — the remote_servers half of
// the lease-renew convergence (hosts + DNS heal the RESOLUTION of
// peer names; this heals the MEMBERSHIP list the rendered inventory
// carries). The contracts:
//   - drift installs: a rendered inventory that differs from the
//     installed file replaces it (the 2026-11-15 soak finding: a
//     scaled-in peer's entry kept wedging ON CLUSTER DDL until the
//     next boot).
//   - convergence is quiet: an unchanged inventory installs NOTHING
//     (the tick must not touch the managed tree when nothing drifted).
//   - keepers never render (no CHI section to converge).
//   - a render failure keeps the installed file (best-effort contract
//     — same as the hosts/DNS convergences).

const convergeCHISpec = `apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: logexplorer
spec:
  configuration:
    clusters:
      - name: logexplorer
        layout:
          shards:
            - name: "01"
              replicasCount: 6
    zookeeper:
      nodes:
        - host: keeper-0.ch.internal
          port: 9181
        - host: keeper-1.ch.internal
          port: 9181
        - host: keeper-2.ch.internal
          port: 9181
    profiles:
      default/async_insert: "1"
    users:
      default/profile: readonly
`

// convergeBoot builds a lease-mode writer Boot over the fake lister
// with the CHI fixture + password in a scratch bootstrap dir.
func convergeBoot(lister *fakeLeaseLister, identity model.NodeIdentity) *Boot {
	spec := inventoryTestSpec()
	spec.Role = model.RoleWriter
	spec.IdentityMode = model.IdentityLease
	return &Boot{
		Spec:     &spec,
		Provider: &listerProvider{fakeLeaseLister: lister},
		Meta:     port.InstanceMetadata{Provider: "aws", Region: "us-east-1"},
		Identity: identity,
	}
}

var _ = Describe("ConvergeRemoteServers", func() {
	var dir, bootstrapDir string
	var lister *fakeLeaseLister
	var b *Boot

	BeforeEach(func() {
		var err error
		dir, err = os.MkdirTemp("", "chinit-remote-servers-*")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = os.RemoveAll(dir) })
		bootstrapDir = filepath.Join(dir, "bootstrap")
		Expect(os.MkdirAll(bootstrapDir, 0755)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(bootstrapDir, "chi.yaml"), []byte(convergeCHISpec), 0644)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(bootstrapDir, "default-password"), []byte("sekrit\n"), 0600)).To(Succeed())

		origDir, origBoot := remoteServersDir, remoteServersBootstrapDir
		remoteServersDir = func() string { return filepath.Join(dir, "config.d") }
		remoteServersBootstrapDir = func() string { return bootstrapDir }
		DeferCleanup(func() { remoteServersDir, remoteServersBootstrapDir = origDir, origBoot })

		lister = &fakeLeaseLister{}
		b = convergeBoot(lister, model.NodeIdentity{
			Role:        model.RoleWriter,
			Hostname:    "writer-0",
			InstanceID:  "i-w0",
			ClusterName: "logexplorer",
			DNSZone:     "ch.internal",
			CRName:      "01-0",
			LeaseTable:  "node-claims",
		})
	})

	It("installs a drifted inventory and converges quietly on the next tick", func() {
		// Live fleet: writer-0 + reader-0 (writer-1 scaled in — its
		// rendered entry is the drift to remove).
		lister.claims = []port.Claim{
			{Hostname: "writer-0", InstanceID: "i-w0", IP: "10.0.1.10"},
			{Hostname: "reader-0", InstanceID: "i-r0", IP: "10.0.1.40"},
		}

		Expect(b.ConvergeRemoteServers(context.Background())).To(Succeed())

		dest := filepath.Join(dir, "config.d", remoteServersFile)
		first, err := os.ReadFile(dest)
		Expect(err).NotTo(HaveOccurred(), "the drifted inventory installed the file")
		Expect(string(first)).To(ContainSubstring("writer-0.ch.internal"))
		Expect(string(first)).To(ContainSubstring("reader-0.ch.internal"))
		// The vacant slot (writer-1) is NOT in the rendered list — the
		// vacant-slot filter held through the convergence render.
		Expect(string(first)).NotTo(ContainSubstring("writer-1"))
		// Credentials ride the replica entries (the Code 516 finding).
		Expect(string(first)).To(ContainSubstring("<password>"))

		// Second tick: same claims — byte-identical render, NOTHING
		// installs (the quiet convergence). The mtime proves it.
		stat1, err := os.Stat(dest)
		Expect(err).NotTo(HaveOccurred())
		Expect(b.ConvergeRemoteServers(context.Background())).To(Succeed())
		stat2, err := os.Stat(dest)
		Expect(err).NotTo(HaveOccurred())
		Expect(stat2.ModTime()).To(Equal(stat1.ModTime()), "an unchanged inventory must not rewrite the file")
	})

	It("removes a scaled-in peer from the rendered inventory (the soak finding)", func() {
		// Tick 1: the full fleet INCLUDING reader-1 (about to die).
		lister.claims = []port.Claim{
			{Hostname: "writer-0", InstanceID: "i-w0", IP: "10.0.1.10"},
			{Hostname: "reader-0", InstanceID: "i-r0", IP: "10.0.1.40"},
			{Hostname: "reader-1", InstanceID: "i-r1", IP: "10.0.1.41"},
		}
		Expect(b.ConvergeRemoteServers(context.Background())).To(Succeed())

		dest := filepath.Join(dir, "config.d", remoteServersFile)
		first, _ := os.ReadFile(dest)
		Expect(string(first)).To(ContainSubstring("reader-1.ch.internal"))

		// Tick 2: reader-1's lease expired (scale-in) — the inventory
		// must converge WITHOUT it.
		lister.claims = []port.Claim{
			{Hostname: "writer-0", InstanceID: "i-w0", IP: "10.0.1.10"},
			{Hostname: "reader-0", InstanceID: "i-r0", IP: "10.0.1.40"},
		}
		Expect(b.ConvergeRemoteServers(context.Background())).To(Succeed())

		second, _ := os.ReadFile(dest)
		Expect(string(second)).NotTo(ContainSubstring("reader-1.ch.internal"),
			"the scaled-in peer's entry must leave the rendered inventory on the next tick")
		Expect(string(second)).To(ContainSubstring("reader-0.ch.internal"))
	})

	It("never renders for keepers", func() {
		// The role gate reads the SPEC's role (the same gate the boot
		// pipeline keys on), not the identity's.
		kb := convergeBoot(lister, model.NodeIdentity{Role: model.RoleKeeper, Hostname: "keeper-0"})
		kb.Spec.Role = model.RoleKeeper
		Expect(kb.ConvergeRemoteServers(context.Background())).To(Succeed())

		_, err := os.Stat(filepath.Join(dir, "config.d", remoteServersFile))
		Expect(err).To(HaveOccurred(), "keepers render no CHI section — nothing installs")
	})

	It("keeps the installed file when the render fails (best-effort)", func() {
		lister.claims = []port.Claim{
			{Hostname: "writer-0", InstanceID: "i-w0", IP: "10.0.1.10"},
		}
		Expect(b.ConvergeRemoteServers(context.Background())).To(Succeed())
		dest := filepath.Join(dir, "config.d", remoteServersFile)
		before, _ := os.ReadFile(dest)

		// Break the CHI fixture: the loader refuses, the convergence
		// keeps the previous file.
		Expect(os.WriteFile(filepath.Join(bootstrapDir, "chi.yaml"), []byte("not: a: valid: chi"), 0644)).To(Succeed())
		Expect(b.ConvergeRemoteServers(context.Background())).To(Succeed())

		after, _ := os.ReadFile(dest)
		Expect(string(after)).To(Equal(string(before)), "a failed render must keep the installed file")
	})
})
