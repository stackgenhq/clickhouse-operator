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

package app

import (
	"errors"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// This file pins `chinit drain-shutdown` — the in-node shutdown drain
// the chinit-shutdown-drain.service unit's ExecStop invokes. Two
// contracts make it safe:
//   - the REBOOT GUARD: a reboot reloads the same node with the same
//     identity (lease, DNS record, replica registration) — the drain
//     must decline on reboot.target, or every `reboot` would strip a
//     healthy node's identity and force a full re-claim cycle.
//   - SkipServerShutdown: the drain runs INSIDE systemd's shutdown
//     transaction (ordered After=clickhouse-server, so the server is
//     still up). Issuing SYSTEM SHUTDOWN from inside would fight the
//     very transaction that invoked the drain — the sequence must end
//     at DNS + lease release, with the OS teardown owning the stop.

var _ = Describe("runDrainShutdown", func() {
	var f *drainFakes
	var statePath string

	BeforeEach(func() {
		var restore func()
		f, restore = drainFixtures()
		DeferCleanup(restore)

		statePath = filepath.Join(drainDir, "shutdown-state.json")
		state := `{"identity":{"role":"writer","hostname":"writer-1","instance_id":"i-w1","private_ip":"10.0.1.21","cluster_name":"logexplorer","dns_zone":"ch.internal","cr_name":"01-1","zone_id":"z-1","lease_table":"node-claims"}}`
		if err := os.WriteFile(statePath, []byte(state), 0644); os.WriteFile(statePath, []byte(state), 0644) != nil {
			Fail(err.Error())
		}

		// The default fake answers "termination": no reboot job in
		// the recorder's systemctl output. Each spec that needs the
		// reboot path overrides the seam with a canned list-jobs.
		origReboot := shutdownIsReboot
		DeferCleanup(func() { shutdownIsReboot = origReboot })
		shutdownIsReboot = func() (bool, error) { return false, nil }
	})

	Context("termination (the default)", func() {
		It("drains but never issues SYSTEM SHUTDOWN (the OS teardown owns it)", func() {
			Expect(runDrainShutdown([]string{"-state", statePath})).To(Succeed())

			lines := f.runner.lines()
			Expect(lines).To(ContainElement("clickhouse-client --query SYSTEM STOP REPLICATION QUEUES"))
			Expect(lines).To(ContainElement("clickhouse-client --query SYSTEM STOP DISTRIBUTED SENDS"))
			// SkipServerShutdown is the shutdown drain's whole
			// distinction: the drain ends at DNS + lease release.
			Expect(lines).NotTo(ContainElement("clickhouse-client --query SYSTEM SHUTDOWN"))
			Expect(lines).NotTo(ContainElement("systemctl stop clickhouse-server"))
			// The DNS record and the lease WERE released — the residue
			// the soak found on reader-1 is exactly what this releases.
			Expect(f.dns.deletes).To(HaveLen(1))
			Expect(f.dns.deletes[0].RecordName).To(Equal("writer-1"))
			Expect(f.claimer.releases).To(HaveLen(1))
		})

		It("exits with the missing-state contract when state is absent", func() {
			missing := filepath.Join(drainDir, "missing-shutdown-state.json")
			err := runDrainShutdown([]string{"-state", missing})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot load node state"))
		})
	})

	Context("reboot guard", func() {
		It("declines on reboot.target — identity survives a reboot", func() {
			shutdownIsReboot = func() (bool, error) { return true, nil }

			Expect(runDrainShutdown([]string{"-state", statePath})).To(Succeed())

			// NOTHING ran: no queues stopped, no DNS deleted, no lease
			// released — a reboot must leave the identity intact.
			Expect(f.runner.lines()).To(BeEmpty())
			Expect(f.dns.deletes).To(BeEmpty())
			Expect(f.claimer.releases).To(BeEmpty())
		})

		It("proceeds on a reboot-check failure (best-effort check)", func() {
			// A systemd IPC failure must not turn into a skipped drain
			// on a real termination — the drain runs, loudly warned.
			shutdownIsReboot = func() (bool, error) { return false, os.ErrDeadlineExceeded }

			Expect(runDrainShutdown([]string{"-state", statePath})).To(Succeed())
			Expect(f.dns.deletes).To(HaveLen(1), "the drain proceeded despite the check failure")
		})
	})

	Context("the parting sweep (other nodes' stale records)", func() {
		// The shutdown drain's last act: release every OTHER
		// expired-slot record in the zone. The terminating node's OWN
		// record the drain released above; an undrained predecessor's
		// stale record (its drain could not dispatch — the
		// health-check class) would otherwise wedge peers'
		// schema-sync/fan-outs until the next renewal tick elsewhere.
		It("sweeps the zone's expired slots on the way out", func() {
			// writer-1 (self) is draining; writer-0 and writer-3 live;
			// writer-2's lease is gone — its record is the stale one
			// the sweep must release.
			f.lister.claims = []port.Claim{
				{Hostname: "writer-0", IP: "10.0.1.45", InstanceID: "i-w0"},
				{Hostname: "writer-1", IP: "10.0.1.21", InstanceID: "i-w1"},
				{Hostname: "writer-3", IP: "10.0.1.24", InstanceID: "i-w3"},
			}

			Expect(runDrainShutdown([]string{"-state", statePath})).To(Succeed())

			deleted := map[string]bool{}
			for _, req := range f.dns.deletes {
				deleted[req.RecordName] = true
			}
			// Self's own release (the drain's leg 3) plus the parting
			// sweep's release of the expired writer-2. Crucially NO
			// upsert: a self-heal would re-create self's record after
			// the drain deleted it.
			Expect(deleted).To(Equal(map[string]bool{
				"writer-1": true, // the drain's own DNS release
				"writer-2": true, // the parting sweep's catch
			}))
			Expect(f.dns.upserts).To(BeEmpty(),
				"the parting sweep must never self-heal — it would re-create the record the drain just deleted")
		})

		It("never fails the shutdown when the sweep's listing fails", func() {
			f.lister.err = errors.New("dynamodb outage mid-shutdown")

			Expect(runDrainShutdown([]string{"-state", statePath})).To(Succeed(),
				"the sweep is best-effort: its failure must not fail the drain")
			// The drain's own DNS release still ran (the state-backed
			// leg precedes the claims-listing sweep).
			Expect(f.dns.deletes).To(HaveLen(1))
			Expect(f.dns.deletes[0].RecordName).To(Equal("writer-1"))
		})
	})
})
