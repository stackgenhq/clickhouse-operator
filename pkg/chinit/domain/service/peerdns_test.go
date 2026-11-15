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
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// This file pins the periodic DNS convergence — the lease-renew timer's
// third half (after the lease renewal and the /etc/hosts convergence).
// The 2026-11-15 incident: a writer terminated by an EC2 health check
// never ran its drain (SSM cannot dispatch to a stopping instance), and
// its A record kept pointing at a dead IP for hours — every
// clusterAllReplicas fan-out and schema-sync pass hung to the 60s query
// timeout on the name, fresh writers stayed table-less, and the whole
// ingest/merge load pinned a single writer. The convergence heals the
// zone within one renewal cycle (5 minutes).

// dnsClaimsProvider is a provider fake carrying both capabilities the
// DNS convergence needs: the lease lister (embedded, the same shape
// inventory_test.go's listerProvider uses) and the DNS registrar. The
// base Provider surface is stubbed the same way the other suite fakes
// stub theirs (Name/Metadata/GetSecretValue/FetchBundle).
type dnsClaimsProvider struct {
	*fakeLeaseLister
	*fakeDNSRegistrar
}

func (p *dnsClaimsProvider) Name() string { return "aws" }

func (p *dnsClaimsProvider) Metadata(context.Context) (port.InstanceMetadata, error) {
	return port.InstanceMetadata{}, nil
}

func (p *dnsClaimsProvider) GetSecretValue(context.Context, port.SecretRequest) (string, error) {
	return "", nil
}

func (p *dnsClaimsProvider) FetchBundle(context.Context, model.ConfigBundle, string) ([]string, error) {
	return nil, nil
}

// fakeDNSRegistrar records the A-record operations.
type fakeDNSRegistrar struct {
	zoneID   string
	zoneName string
	upserts  []port.ARecordRequest
	deletes  []port.ARecordRequest
	// deleteFailNames names records whose delete fails (a Route53
	// outage on one name must not stop the sweep of the others).
	deleteFailNames map[string]bool
	// resolveErr drives the zone-resolution degradation.
	resolveErr error
}

func (d *fakeDNSRegistrar) ResolveZoneID(_ context.Context, zoneName string) (string, error) {
	if d.resolveErr != nil {
		return "", d.resolveErr
	}
	return d.zoneID, nil
}

func (d *fakeDNSRegistrar) UpsertARecord(_ context.Context, req port.ARecordRequest) error {
	d.upserts = append(d.upserts, req)
	return nil
}

func (d *fakeDNSRegistrar) DeleteARecord(_ context.Context, req port.ARecordRequest) error {
	d.deletes = append(d.deletes, req)
	return nil
}

// convergeDNSArgs is the canonical convergence input the specs mutate.
func convergeDNSArgs() ConvergeDNSArgs {
	return ConvergeDNSArgs{
		LeaseTable:  "ch-int-dev-node-claims",
		DNSZoneName: "ch-int-dev.clickhouse.internal",
		Hostname:    "writer-2",
		PrivateIP:   "10.0.1.218",
	}
}

var _ = Describe("ConvergePeerDNS", func() {
	Context("with live claims and a resolvable zone", func() {
		It("self-heals its own record and releases expired-slot records", func() {
			lister := &fakeLeaseLister{claims: []port.Claim{
				{Hostname: "writer-0", IP: "10.0.1.45"},
				{Hostname: "writer-2", IP: "10.0.1.218"},
				{Hostname: "writer-3", IP: "10.0.1.24"},
			}}
			dns := &fakeDNSRegistrar{zoneID: "Z07073102YAGFP019SAW"}
			provider := &dnsClaimsProvider{lister, dns}

			Expect(ConvergePeerDNS(context.Background(), provider, convergeDNSArgs())).To(Succeed())

			// Self-heal: exactly one upsert, for the node's own name.
			Expect(dns.upserts).To(HaveLen(1))
			Expect(dns.upserts[0].RecordName).To(Equal("writer-2"))
			Expect(dns.upserts[0].IP).To(Equal("10.0.1.218"))

			// The expired slot: writer-1 is inside the claimed ordinal
			// space (writer-0..3) but holds no live claim — its record
			// is the dead holder's and must be released.
			Expect(dns.deletes).To(HaveLen(1))
			Expect(dns.deletes[0].RecordName).To(Equal("writer-1"))
			Expect(dns.deletes[0].IP).To(BeEmpty(),
				"the sweep delete carries no IP — the provider lists the live value set")
		})

		It("sweeps every vacant ordinal inside the claimed space", func() {
			lister := &fakeLeaseLister{claims: []port.Claim{
				{Hostname: "writer-0", IP: "10.0.1.45"},
				{Hostname: "writer-3", IP: "10.0.1.24"},
			}}
			dns := &fakeDNSRegistrar{zoneID: "Z07073102YAGFP019SAW"}
			provider := &dnsClaimsProvider{lister, dns}

			Expect(ConvergePeerDNS(context.Background(), provider, convergeDNSArgs())).To(Succeed())

			deleted := map[string]bool{}
			for _, req := range dns.deletes {
				deleted[req.RecordName] = true
			}
			Expect(deleted).To(Equal(map[string]bool{
				"writer-1": true,
				"writer-2": true,
			}),
				"the sweep deletes the vacant ordinals between the claimed bounds")
		})
	})

	Context("degradation", func() {
		It("keeps the zone untouched when the zone does not resolve", func() {
			lister := &fakeLeaseLister{claims: []port.Claim{{Hostname: "writer-0", IP: "10.0.1.45"}}}
			dns := &fakeDNSRegistrar{resolveErr: errors.New("no such zone")}
			provider := &dnsClaimsProvider{lister, dns}

			Expect(ConvergePeerDNS(context.Background(), provider, convergeDNSArgs())).To(Succeed())
			Expect(dns.upserts).To(BeEmpty())
			Expect(dns.deletes).To(BeEmpty())
		})

		It("self-heals but skips the sweep when the claims listing fails", func() {
			lister := &fakeLeaseLister{err: errBoom}
			dns := &fakeDNSRegistrar{zoneID: "Z07073102YAGFP019SAW"}
			provider := &dnsClaimsProvider{lister, dns}

			Expect(ConvergePeerDNS(context.Background(), provider, convergeDNSArgs())).To(Succeed())
			Expect(dns.upserts).To(HaveLen(1),
				"the self-heal runs before the claims listing")
			Expect(dns.deletes).To(BeEmpty())
		})

		It("no-ops without a lease table (static-identity node)", func() {
			lister := &fakeLeaseLister{claims: []port.Claim{{Hostname: "writer-0", IP: "10.0.1.45"}}}
			dns := &fakeDNSRegistrar{zoneID: "Z07073102YAGFP019SAW"}
			provider := &dnsClaimsProvider{lister, dns}
			args := convergeDNSArgs()
			args.LeaseTable = ""

			Expect(ConvergePeerDNS(context.Background(), provider, args)).To(Succeed())
			Expect(dns.upserts).To(BeEmpty())
			Expect(dns.deletes).To(BeEmpty())
		})
	})

	Context("per-slot failure isolation", func() {
		It("continues the sweep when one delete fails", func() {
			lister := &fakeLeaseLister{claims: []port.Claim{
				{Hostname: "writer-0", IP: "10.0.1.45"},
				{Hostname: "writer-3", IP: "10.0.1.24"},
			}}
			dns := &fakeDNSRegistrar{
				zoneID:          "Z07073102YAGFP019SAW",
				deleteFailNames: map[string]bool{"writer-1": true},
			}
			provider := &dnsClaimsProvider{lister, dns}

			Expect(ConvergePeerDNS(context.Background(), provider, convergeDNSArgs())).To(Succeed())

			deleted := map[string]bool{}
			for _, req := range dns.deletes {
				deleted[req.RecordName] = true
			}
			Expect(deleted).To(Equal(map[string]bool{
				"writer-1": true,
				"writer-2": true,
			}),
				"a failing delete must not stop the sweep of the remaining expired slots")
		})
	})
})

var _ = Describe("claimOrdinalSpace", func() {
	It("ignores foreign roles and non-ordinal names", func() {
		claims := []port.Claim{
			{Hostname: "keeper-0", IP: "10.0.1.20"},
			{Hostname: "backup-0", IP: "10.0.1.30"},
			{Hostname: "writer-0", IP: "10.0.1.45"},
			{Hostname: "reader-1", IP: "10.0.1.207"},
		}
		slots := claimOrdinalSpace(claims)
		names := map[string]bool{}
		for _, slot := range slots {
			names[slot.hostname] = true
		}
		Expect(names).To(Equal(map[string]bool{
			"writer-0": true,
			"reader-0": true,
			"reader-1": true,
		}),
			"keeper/backup have no ASG ordinal; reader-1's space includes vacant reader-0")
	})

	It("is empty with no ASG-role claims", func() {
		Expect(claimOrdinalSpace([]port.Claim{{Hostname: "keeper-0"}})).To(BeEmpty())
	})
})
