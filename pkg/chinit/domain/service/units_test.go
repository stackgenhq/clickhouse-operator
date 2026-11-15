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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	"github.com/altinity/clickhouse-operator/pkg/chinit/infra/system"
)

// This suite pins the recurring-unit matrix for writer ASG (A5): which
// units install for which role/identity-mode combination, and the
// descriptions the timers log. chinitUnits is the pure assembler —
// installUnits only writes what it returns, so the matrix specs need
// no filesystem.
//
// The contracts:
//   - lease-renew is IDENTITY-mode gated, never role gated: lease
//     writers and lease readers both install it (a role gate would
//     let a leased writer's 15-min TTL expire and another instance
//     claim its ordinal mid-flight).
//   - the maintenance units install on EVERY writer: with ASG writers
//     the ordinal-0 designation is a lease, not a machine, so the
//     unit set must survive ordinal handoffs. The subcommands no-op
//     on non-ordinal-0 holders (the app package's specs pin that
//     gate); readers never install them.

// unitNames renders the assembler's answer as a name list.
func unitNames(units []system.UnitFile) []string {
	out := make([]string, 0, len(units))
	for _, u := range units {
		out = append(out, u.Name)
	}
	return out
}

var _ = Describe("chinitUnits", func() {
	// baseSpec is the leased-writer shape; the matrix mutates role and
	// identity mode.
	baseSpec := func() model.BootSpec {
		spec := ceilingSpec()
		spec.IdentityMode = model.IdentityLease
		return spec
	}

	Context("writer, lease identity", func() {
		It("installs lease-renew and the maintenance units alongside the shared server units", func() {
			b := &Boot{Spec: &model.BootSpec{}}
			*b.Spec = baseSpec()
			Expect(b.Spec.Role).To(Equal(model.RoleWriter))

			names := unitNames(b.chinitUnits())
			Expect(names).To(ContainElements(
				"lease-renew.service",
				"lease-renew.timer",
				"dead-replica-gc.service",
				"dead-replica-gc.timer",
				"orphaned-data-gc.service",
				"orphaned-data-gc.timer",
				"schema-sync.service",
				"replica-sync.service",
			))
		})

		It("describes the maintenance jobs as ordinal-0-gated", func() {
			// The unit description is what `systemctl list-units` and
			// the boot log print — the operator's first clue why a
			// non-0 writer's timer ran without dropping anything.
			b := &Boot{Spec: &model.BootSpec{}}
			*b.Spec = baseSpec()
			for _, u := range b.chinitUnits() {
				switch u.Name {
				case "dead-replica-gc.service", "orphaned-data-gc.service":
					Expect(u.Content).To(ContainSubstring("ordinal-0 writer only"))
				}
			}
		})
	})

	Context("writer, static identity (the legacy writer path)", func() {
		It("installs the maintenance units but no lease-renew", func() {
			// Static writers keep the writer-0 machine contract (no
			// lease to renew) — the maintenance units still install
			// and the hostname gate still holds (writer-0 is the
			// machine's name in this mode).
			spec := baseSpec()
			spec.IdentityMode = model.IdentityStatic
			spec.WriterMax = 0 // static writers carry no ceiling
			b := &Boot{Spec: &spec}

			names := unitNames(b.chinitUnits())
			Expect(names).To(ContainElements("dead-replica-gc.service", "orphaned-data-gc.service"))
			Expect(names).NotTo(ContainElement("lease-renew.service"))
			Expect(names).NotTo(ContainElement("lease-renew.timer"))
		})
	})

	Context("reader, lease identity", func() {
		It("installs lease-renew and disk-autoscale but never the maintenance units", func() {
			spec := baseSpec()
			spec.Role = model.RoleReader
			spec.WriterMax = 0
			spec.ReaderMax = 10
			spec.ReaderCount = 3
			b := &Boot{Spec: &spec}

			names := unitNames(b.chinitUnits())
			Expect(names).To(ContainElements("lease-renew.service", "lease-renew.timer"))
			// Self-healing contract: every server node with a data
			// disk keeps its own headroom (the 2026-11-14 merge stall
			// was a READER's disk).
			Expect(names).To(ContainElements("disk-autoscale.service", "disk-autoscale.timer"))
			Expect(names).NotTo(ContainElement("dead-replica-gc.service"))
			Expect(names).NotTo(ContainElement("orphaned-data-gc.service"))
		})

		It("runs disk-autoscale every five minutes", func() {
			spec := baseSpec()
			spec.Role = model.RoleReader
			spec.WriterMax = 0
			spec.ReaderMax = 10
			b := &Boot{Spec: &spec}
			for _, u := range b.chinitUnits() {
				if u.Name == "disk-autoscale.timer" {
					Expect(u.Content).To(ContainSubstring("OnCalendar=*-*-* *:00/5:00"))
				}
			}
		})

		It("describes lease-renew as node-scoped, not reader-scoped", func() {
			// The writer-ASG rollout made the lease a both-roles
			// mechanism; the description must not claim reader-only
			// (the log would tell the operator the wrong story when a
			// writer's timer fires).
			spec := baseSpec()
			spec.Role = model.RoleReader
			spec.WriterMax = 0
			spec.ReaderMax = 10
			b := &Boot{Spec: &spec}
			for _, u := range b.chinitUnits() {
				if u.Name == "lease-renew.service" {
					Expect(u.Content).To(ContainSubstring("Description=Node ordinal lease renewal"))
				}
				if u.Name == "lease-renew.timer" {
					Expect(u.Content).To(ContainSubstring("Renew the ordinal lease every 5 minutes"))
					Expect(u.Content).NotTo(ContainSubstring("reader ordinal lease"))
				}
			}
		})
	})

	Context("keeper", func() {
		It("installs the keeper-health probe AND its every-minute timer", func() {
			spec := baseSpec()
			spec.Role = model.RoleKeeper
			spec.IdentityMode = model.IdentityStatic
			b := &Boot{Spec: &spec}

			// Self-healing contract (the 2026-11-13 blind window): the
			// probe without a timer never ran again after boot, and
			// keeper_down sat INSUFFICIENT_DATA. The timer is the fix.
			names := unitNames(b.chinitUnits())
			Expect(names).To(Equal([]string{"keeper-health.service", "keeper-health.timer"}))

			for _, u := range b.chinitUnits() {
				if u.Name == "keeper-health.timer" {
					Expect(u.Content).To(ContainSubstring("OnCalendar=*-*-* *:*:00"))
					Expect(u.Content).To(ContainSubstring("keeper_up"))
				}
			}
		})
	})

	Context("backup replica", func() {
		It("installs nothing", func() {
			spec := baseSpec()
			spec.Role = model.RoleBackup
			spec.IdentityMode = model.IdentityStatic
			b := &Boot{Spec: &spec}

			Expect(unitNames(b.chinitUnits())).To(BeEmpty())
		})
	})
})

var _ = Describe("shutdownDrainUnit", func() {
	// The in-node shutdown drain unit (the fix for the 2026-11-15
	// soak finding: reader scale-ins left stale DNS + lease residue
	// because the SSM-dispatched drain races the agent's teardown, and
	// a health-check termination never fires the hook at all). These
	// specs pin the unit-content contract; the role gating lives in
	// installShutdownDrain's switch (writers + readers only).

	It("orders ExecStop after clickhouse-server so the drain runs against a live server", func() {
		// The ordering IS the design: on shutdown systemd stops units
		// in reverse dependency order, so After=clickhouse-server
		// means this unit's ExecStop runs BEFORE the server stops —
		// the peer drop and SYSTEM STOP queries need a live server.
		u := (&Boot{Spec: &model.BootSpec{}}).shutdownDrainUnit()
		Expect(u.Name).To(Equal("chinit-shutdown-drain.service"))
		Expect(u.Content).To(ContainSubstring("After=network-online.target clickhouse-server.service"))
	})

	It("runs the drain-shutdown subcommand at ExecStop and stays quiescent at start", func() {
		// ExecStart=/bin/true + RemainAfterExit=yes: the unit exists
		// solely to be an active unit whose ExecStop systemd invokes
		// during the shutdown transaction.
		u := (&Boot{Spec: &model.BootSpec{}}).shutdownDrainUnit()
		Expect(u.Content).To(ContainSubstring("ExecStart=/bin/true"))
		Expect(u.Content).To(ContainSubstring("ExecStop=/usr/local/bin/chinit drain-shutdown"))
		Expect(u.Content).To(ContainSubstring("RemainAfterExit=yes"))
	})

	It("bounds the drain under EC2's shutdown window", func() {
		// The subcommand's own budget is 150s; the unit bound (170s)
		// must sit ABOVE it so the wrapper never cuts the drain short,
		// and both must stay under EC2's 180s API-driven window — a
		// drain that overruns the window gets the instance killed
		// mid-drain, the exact outcome the unit prevents.
		u := (&Boot{Spec: &model.BootSpec{}}).shutdownDrainUnit()
		Expect(u.Content).To(ContainSubstring("TimeoutStopSec=170"))
	})
})
