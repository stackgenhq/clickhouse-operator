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

package azure

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// TestAzureSuite is the package's single Ginkgo entrypoint: the Table
// Storage live-claims lister specs (claims_list_suite_test.go bodies)
// and the managed-disk grower specs (volumes_test.go bodies) both run
// here — one RunSpecs per package, per the repo's BDD mandate.
func TestAzureSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Azure Provider Suite")
}

// fakeTableHTTP answers Table Storage Query Entities calls from a
// canned lease table (the same leaseEntity shape the claimer writes).
// It implements only the httpClient seam the claimer/lister share.
type fakeTableHTTP struct {
	rows map[string]leaseEntity // keyed by RowKey (hostname)
}

func (f *fakeTableHTTP) Do(req *http.Request) (*http.Response, error) {
	if !strings.Contains(req.URL.Path, "(") {
		// Entity-addressed GET (claim path); the lister only queries.
		return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader(""))}, nil
	}
	// The lister queries <table>()?$filter=... — respond with every
	// row the server-side filter would keep: lease_until > now.
	now := time.Now().Unix()
	var kept []leaseEntity
	for _, row := range f.rows {
		if row.LeaseUntil > now {
			kept = append(kept, row)
		}
	}
	body, err := json.Marshal(queryResponse{Value: kept})
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(string(body))),
		Header:     http.Header{},
	}, nil
}

var _ = Describe("TableClaimLister.ListActiveClaims", func() {
	var (
		claimer *TableClaimer
		lister  *TableClaimLister
	)

	BeforeEach(func() {
		claimer = &TableClaimer{
			client:      &fakeTableHTTP{},
			accountName: "storageacct",
			TokenSource: func(context.Context, string) (string, error) {
				return "token", nil
			},
		}
		lister = NewTableClaimLister(claimer)
	})

	It("returns active leases as the live member set", func() {
		fake := &fakeTableHTTP{rows: map[string]leaseEntity{
			"writer-0": {PartitionKey: "readers", RowKey: "writer-0", InstanceID: "i-0", IP: "10.0.1.10", LeaseUntil: time.Now().Add(10 * time.Minute).Unix()},
			"reader-1": {PartitionKey: "readers", RowKey: "reader-1", InstanceID: "i-1", IP: "10.0.1.11", LeaseUntil: time.Now().Add(10 * time.Minute).Unix()},
		}}
		claimer.client = fake
		claims, err := lister.ListActiveClaims(context.Background(), chinitport.ListClaimsRequest{Table: "claims"})
		Expect(err).NotTo(HaveOccurred())
		Expect(claims).To(HaveLen(2))
		byHost := map[string]chinitport.Claim{}
		for _, c := range claims {
			byHost[c.Hostname] = c
		}
		Expect(byHost).To(HaveKey("writer-0"))
		Expect(byHost).To(HaveKey("reader-1"))
		Expect(byHost["writer-0"].InstanceID).To(Equal("i-0"))
	})

	It("drops rows the server filter let through but expired on decode (clock skew)", func() {
		// A row that expires mid-flight: LeaseUntil is in the past by
		// the time the response is decoded. The fake answers with the
		// filter already stale (server now == decode now), so only the
		// code-side guard keeps it out of the inventory.
		fake := &fakeTableHTTP{rows: map[string]leaseEntity{
			"writer-0": {PartitionKey: "readers", RowKey: "writer-0", InstanceID: "i-0", LeaseUntil: time.Now().Add(-1 * time.Second).Unix()},
		}}
		claimer.client = fake
		claims, err := lister.ListActiveClaims(context.Background(), chinitport.ListClaimsRequest{Table: "claims"})
		Expect(err).NotTo(HaveOccurred())
		Expect(claims).To(BeEmpty(), "an expired row must never render into remote_servers")
	})

	It("fails loudly when the account is not configured", func() {
		claimer.accountName = ""
		_, err := lister.ListActiveClaims(context.Background(), chinitport.ListClaimsRequest{Table: "claims"})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("storage account"))
	})
})
