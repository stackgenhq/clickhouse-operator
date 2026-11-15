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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// This file pins the CRITICALITY MATRIX — the boot's answer to "is a
// node that boots without this step SAFE as a cluster member, or a
// liability?" The classification is load-bearing:
//
//   - CRITICAL steps are identity/availability-carrying: a node that
//     would boot without them holds an ordinal it cannot renew, a
//     keeper seat with a stale config, or a termination path that
//     leaks identity residue. The ASG replacement a Critical failure
//     triggers IS the remedy.
//   - BestEffort steps are environment-shaped or observable-by-
//     absence: the health gate (a reader joining mid-churn), the
//     prometheus reload (the server serves queries without metrics),
//     log shipping (the dashboards show the gap), the spot watcher
//     (the shutdown drain + lease TTL cover a missed notice).
//
// The 2026-11-15 reclassification moved four steps from BestEffort to
// Critical; these specs pin the full matrix so a future edit answers
// for its failure mode instead of defaulting to "continue".

// criticalityOf renders the pipeline's step→criticality map, with the
// group's children flattened (the engine runs them as one phase).
func criticalityOf(steps []Step) map[string]Criticality {
	out := map[string]Criticality{}
	for _, st := range steps {
		if group, ok := st.(ParallelGroup); ok {
			for _, c := range group.Children {
				out[c.Name()] = c.Criticality()
			}
			continue
		}
		out[st.Name()] = st.Criticality()
	}
	return out
}

// matrixBoot is a minimal Boot whose Steps() runs — the steps are
// closures over the spec/provider, but none executes in these specs
// (only the classification is read).
func matrixBoot() *Boot {
	spec := ceilingSpec()
	return &Boot{
		Spec: &spec,
		Provider: &listerProvider{fakeLeaseLister: &fakeLeaseLister{}},
		Meta:     port.InstanceMetadata{Provider: "aws", Region: "us-east-1"},
	}
}

var _ = Describe("the criticality matrix", func() {
	var classes map[string]Criticality

	BeforeEach(func() {
		steps, err := matrixBoot().Steps(context.Background())
		Expect(err).NotTo(HaveOccurred())
		classes = criticalityOf(steps)
	})

	It("classifies identity-carrying installation steps Critical", func() {
		// install_units carries lease-renew (an expired claim is
		// identity theft — another instance claims the same ordinal),
		// schema-sync (the node's tables) and disk-autoscale (the
		// merge-stall early warning).
		Expect(classes["install_units"]).To(Equal(Critical))
		// install_shutdown_drain: a node without it terminates cold
		// and leaks the stale-DNS/lease residue the soak found.
		Expect(classes["install_shutdown_drain"]).To(Equal(Critical))
		// configure_keeper no-ops for every role EXCEPT keepers —
		// and for a keeper a stale --config disrupts the raft
		// ensemble itself.
		Expect(classes["configure_keeper"]).To(Equal(Critical))
		// configure_backup no-ops when backups are not configured;
		// when the operator EXPLICITLY pinned a bucket, a silent
		// no-backup node is a restore-time data-loss discovery.
		Expect(classes["configure_backup"]).To(Equal(Critical))
	})

	It("keeps environment-shaped steps BestEffort", func() {
		// The health-gate wedge was a real ASG boot-loop incident: a
		// reader joining while a peer is down MUST still join.
		Expect(classes["health_gate"]).To(Equal(BestEffort))
		// Metrics absence is observable; queries still serve.
		Expect(classes["prometheus_reload"]).To(Equal(BestEffort))
		// Log shipping is opt-in by spec and cloud-shaped.
		Expect(classes["configure_monitoring"]).To(Equal(BestEffort))
		Expect(classes["configure_cloudwatch"]).To(Equal(BestEffort))
		// The spot watcher optimizes the drain path; the shutdown
		// drain + lease TTL cover a missed notice.
		Expect(classes["install_spot_watch"]).To(Equal(BestEffort))
		// The legacy shell contract: nothing identity-carrying.
		Expect(classes["write_node_env"]).To(Equal(BestEffort))
	})

	It("runs the identity-carrying Critical installs in the parallel phase 1", func() {
		// Phase 1's children are the boot's independent poles; every
		// one is Critical because phase 2 depends on ALL of them.
		steps, err := matrixBoot().Steps(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(steps).NotTo(BeEmpty())
		group, ok := steps[0].(ParallelGroup)
		Expect(ok).To(BeTrue(), "phase 1 must be a parallel group")
		for _, c := range group.Children {
			Expect(c.Criticality()).To(Equal(Critical),
				"phase-1 child %s must be Critical — a failure cancels the siblings", c.Name())
		}
	})
})
