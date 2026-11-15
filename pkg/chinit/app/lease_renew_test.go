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
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/service"
)

// This file pins `chinit lease-renew` — the every-5-minute timer the
// units matrix installs on BOTH lease-mode roles (writers since the
// writer-ASG rollout). The subcommand renews through the SETTLED
// identity's lease table and hostname; it never re-derives them.

// leaseRenewDir is this suite's scratch dir (state files) — never
// /var/lib.
var leaseRenewDir = filepath.Join(os.TempDir(), "chinit-app-lease-renew-test")

// writeLeaseState seeds a state file with the given settled identity.
func writeLeaseState(identity model.NodeIdentity) string {
	data, err := json.Marshal(nodeState{Identity: identity})
	Expect(err).NotTo(HaveOccurred())
	path := filepath.Join(leaseRenewDir, "state.json")
	Expect(os.WriteFile(path, data, 0644)).To(Succeed())
	return path
}

var _ = Describe("runLeaseRenew", func() {
	var claimer *drainClaimer

	// fixtures wires the provider seam at a full-capability fake and
	// the claimer records what the renew drove. The subcommand shells
	// nothing out (a provider call, not a chctl call), so no exec
	// recorder is needed.
	fixtures := func() func() {
		claimer = &drainClaimer{}
		origBuild := buildProvider
		buildProvider = func(string, string) (chinitport.Provider, error) {
			return &drainProvider{name: "aws", claimer: claimer}, nil
		}
		Expect(os.MkdirAll(leaseRenewDir, 0755)).To(Succeed())
		// Every renewal now converges /etc/hosts; redirect the seam so
		// the suite never touches the dev machine's real hosts file
		// (the convergence specs' own BeforeEach layers on top).
		hostsPath := filepath.Join(leaseRenewDir, "hosts")
		Expect(os.WriteFile(hostsPath, []byte("127.0.0.1 localhost\n"), 0644)).To(Succeed())
		origHosts := service.PeerHostsPath()
		service.SetPeerHostsPath(func() string { return hostsPath })
		return func() {
			buildProvider = origBuild
			service.SetPeerHostsPath(origHosts)
		}
	}

	BeforeEach(func() {
		DeferCleanup(fixtures())
	})

	// The renew is identity-mode gated, never role gated: BOTH leased
	// roles' timers fire this one subcommand, and it must carry the
	// SETTLED identity (hostname, instance, table the boot claimed
	// from) — never a role assumption. The writer entry is the
	// writer-ASG addition; the reader entry pins that nothing
	// special-cased it back.
	DescribeTable("renews through the claimer with the settled identity",
		func(identity model.NodeIdentity) {
			statePath := writeLeaseState(identity)

			Expect(runLeaseRenew([]string{"-state", statePath})).To(Succeed())
			Expect(claimer.renews).To(Equal([]chinitport.RenewRequest{
				{
					Table:        identity.LeaseTable,
					Hostname:     identity.Hostname,
					InstanceID:   identity.InstanceID,
					LeaseSeconds: 900,
				},
			}))
		},
		Entry("a writer holding a lease (the writer-ASG timer)", model.NodeIdentity{
			Role:        model.RoleWriter,
			Hostname:    "writer-1",
			InstanceID:  "i-w1",
			ClusterName: "logexplorer",
			DNSZone:     "ch.internal",
			CRName:      "01-1",
			LeaseTable:  "node-claims",
		}),
		Entry("a reader holding a lease (the pre-writer timer)", model.NodeIdentity{
			Role:        model.RoleReader,
			Hostname:    "reader-3",
			InstanceID:  "i-r3",
			ClusterName: "logexplorer",
			DNSZone:     "ch.internal",
			CRName:      "01-7",
			LeaseTable:  "node-claims",
		}),
	)

	Context("refusal paths", func() {
		It("is fatal when the identity holds no lease table", func() {
			statePath := writeLeaseState(model.NodeIdentity{
				Role:        model.RoleWriter,
				Hostname:    "writer-0",
				ClusterName: "logexplorer",
			})

			err := runLeaseRenew([]string{"-state", statePath})
			Expect(err).To(HaveOccurred())
			Expect(fail.IsFatal(err)).To(BeTrue(), "a node with no lease cannot renew — retrying masks a state bug")
			Expect(err.Error()).To(ContainSubstring("holds no lease"))
		})

		It("is fatal when the cloud has no lease capability", func() {
			statePath := writeLeaseState(model.NodeIdentity{
				Role:       model.RoleWriter,
				Hostname:   "writer-1",
				LeaseTable: "node-claims",
			})
			origBuild := buildProvider
			buildProvider = func(string, string) (chinitport.Provider, error) {
				return &bareProvider{providerName: "azure"}, nil
			}
			DeferCleanup(func() { buildProvider = origBuild })

			err := runLeaseRenew([]string{"-state", statePath})
			Expect(err).To(HaveOccurred())
			Expect(fail.IsFatal(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("no lease capability"))
		})
	})

	// The renewal converges /etc/hosts on the same live claims — the
	// 2026-11-15 finding's durable fix: a reader booting mid-churn
	// wrote an empty hosts file at boot and sat DNS_ERROR-blind for
	// hours; the every-5-minute renewal is the convergence point.
	Context("peer-hosts convergence", func() {
		var hostsPath string

		BeforeEach(func() {
			dir, err := os.MkdirTemp("", "chinit-lease-hosts-*")
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = os.RemoveAll(dir) })
			hostsPath = filepath.Join(dir, "hosts")
			Expect(os.WriteFile(hostsPath, []byte("127.0.0.1 localhost\n"), 0644)).To(Succeed())
			orig := service.PeerHostsPath()
			service.SetPeerHostsPath(func() string { return hostsPath })
			DeferCleanup(func() { service.SetPeerHostsPath(orig) })
		})

		It("writes live claims' short names after renewing", func() {
			statePath := writeLeaseState(model.NodeIdentity{
				Role:        model.RoleReader,
				Hostname:    "reader-0",
				InstanceID:  "i-r0",
				ClusterName: "logexplorer",
				DNSZone:     "ch.internal",
				LeaseTable:  "node-claims",
			})
			lister := &recordingLister{claims: []chinitport.Claim{
				{Hostname: "writer-0", IP: "10.0.1.69", InstanceID: "i-w0"},
				{Hostname: "reader-0", IP: "10.0.1.211", InstanceID: "i-r0"},
			}}
			origBuild := buildProvider
			buildProvider = func(string, string) (chinitport.Provider, error) {
				return &drainProvider{name: "aws", claimer: &drainClaimer{}, lister: lister}, nil
			}
			DeferCleanup(func() { buildProvider = origBuild })

			Expect(runLeaseRenew([]string{"-state", statePath})).To(Succeed())

			data, err := os.ReadFile(hostsPath)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(data)).To(ContainSubstring("10.0.1.69 writer-0"))
			Expect(string(data)).To(ContainSubstring("10.0.1.211 reader-0"))
		})

		It("renews even when the claims listing fails (best-effort)", func() {
			statePath := writeLeaseState(model.NodeIdentity{
				Role:       model.RoleWriter,
				Hostname:   "writer-1",
				InstanceID: "i-w1",
				LeaseTable: "node-claims",
			})
			claimer := &drainClaimer{}
			lister := &recordingLister{err: errors.New("dynamodb outage")}
			origBuild := buildProvider
			buildProvider = func(string, string) (chinitport.Provider, error) {
				return &drainProvider{name: "aws", claimer: claimer, lister: lister}, nil
			}
			DeferCleanup(func() { buildProvider = origBuild })

			Expect(runLeaseRenew([]string{"-state", statePath})).To(Succeed())
			Expect(claimer.renews).To(HaveLen(1))
			data, _ := os.ReadFile(hostsPath)
			Expect(string(data)).To(ContainSubstring("127.0.0.1 localhost"))
		})
	})

	// The renewal also converges the private zone's server-role A
	// records — the 2026-11-15 stale-record finding's durable fix: a
	// writer terminated by an EC2 health check never ran its drain
	// (SSM cannot dispatch to a stopping instance) and its record kept
	// a dead IP for hours, hanging every clusterAllReplicas fan-out
	// and schema-sync on the fleet. The every-5-minute renewal is the
	// convergence point: self upsert + the expired-slot sweep.
	Context("peer-dns convergence", func() {
		It("self-heals its own record and releases the expired slot", func() {
			statePath := writeLeaseState(model.NodeIdentity{
				Role:        model.RoleWriter,
				Hostname:    "writer-2",
				InstanceID:  "i-w2",
				PrivateIP:   "10.0.1.218",
				ClusterName: "logexplorer",
				DNSZone:     "ch.internal",
				LeaseTable:  "node-claims",
			})
			lister := &recordingLister{claims: []chinitport.Claim{
				{Hostname: "writer-0", IP: "10.0.1.45", InstanceID: "i-w0"},
				{Hostname: "writer-2", IP: "10.0.1.218", InstanceID: "i-w2"},
			}}
			dns := &drainDNS{}
			origBuild := buildProvider
			buildProvider = func(string, string) (chinitport.Provider, error) {
				return &drainProvider{name: "aws", claimer: &drainClaimer{}, lister: lister, dns: dns}, nil
			}
			DeferCleanup(func() { buildProvider = origBuild })

			Expect(runLeaseRenew([]string{"-state", statePath})).To(Succeed())

			// Self-heal: the node's own record is upserted.
			Expect(dns.upserts).To(HaveLen(1))
			Expect(dns.upserts[0].RecordName).To(Equal("writer-2"))

			// The expired slot inside the claimed space: writer-1.
			Expect(dns.deletes).To(Equal([]chinitport.ARecordRequest{
				{
					ZoneID:     "z-1",
					ZoneName:   "ch.internal",
					RecordName: "writer-1",
					TTL:        60,
				},
			}))
		})

		It("is a no-op for keepers (no lease to renew through)", func() {
			statePath := writeLeaseState(model.NodeIdentity{
				Role:        model.RoleKeeper,
				Hostname:    "keeper-0",
				InstanceID:  "i-k0",
				ClusterName: "logexplorer",
				DNSZone:     "ch.internal",
			})
			Expect(runLeaseRenew([]string{"-state", statePath})).NotTo(Succeed())
		})
	})
})

// recordingLister is the lease-listing fake for the renewal specs: it
// reuses the drainClaimer recorder pattern (canned claims or a canned
// failure).
type recordingLister struct {
	claims []chinitport.Claim
	err    error
}

func (l *recordingLister) ListActiveClaims(context.Context, chinitport.ListClaimsRequest) ([]chinitport.Claim, error) {
	if l.err != nil {
		return nil, l.err
	}
	return l.claims, nil
}
