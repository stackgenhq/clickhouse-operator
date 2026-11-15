// Copyright 2026 Altinity Ltd and/or its affiliates. All rights reserved.
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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// refreshClaimer records both Claim and Renew calls so specs can assert
// that the refresh path renews instead of re-claiming.
type refreshClaimer struct {
	claimOrdinal int
	claimErr     error
	renewErr     error
	claims       int
	renews       []port.RenewRequest
}

func (c *refreshClaimer) ClaimOrdinal(_ context.Context, req port.ClaimRequest) (int, error) {
	c.claims++
	if c.claimErr != nil {
		return -1, c.claimErr
	}
	return c.claimOrdinal, nil
}

func (c *refreshClaimer) RenewLease(_ context.Context, req port.RenewRequest) error {
	c.renews = append(c.renews, req)
	return c.renewErr
}

func (c *refreshClaimer) ReleaseLease(_ context.Context, _ port.ReleaseRequest) error { return nil }

type refreshLeaseProvider struct{ *refreshClaimer }

func (p *refreshLeaseProvider) Name() string { return "aws" }

func (p *refreshLeaseProvider) Metadata(context.Context) (port.InstanceMetadata, error) {
	return port.InstanceMetadata{}, nil
}

func (p *refreshLeaseProvider) GetSecretValue(context.Context, port.SecretRequest) (string, error) {
	return "", nil
}

func (p *refreshLeaseProvider) FetchBundle(context.Context, model.ConfigBundle, string) ([]string, error) {
	return nil, nil
}

// writeStateFile seeds a state.json the way a real boot leaves it.
func writeStateFile(dir string, identity bootIdentity) string {
	data := fmt.Sprintf(`{"identity":%s,"steps":[],"spec_hash":"hash-1"}`, mustJSON(identity))
	path := filepath.Join(dir, "state.json")
	Expect(os.WriteFile(path, []byte(data), 0o600)).To(Succeed())
	return path
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	Expect(err).NotTo(HaveOccurred())
	return string(b)
}

var _ = Describe("restoreIdentity (refresh path)", func() {
	specFor := func() model.BootSpec {
		spec := ceilingSpec()
		spec.Role = model.RoleReader
		spec.ReaderHostPrefix = "reader-"
		spec.ReaderMax = 3
		spec.ReaderCount = 1
		spec.WriterMax = 4
		return spec
	}
	settled := bootIdentity{
		Role:        "reader",
		Hostname:    "reader-1",
		InstanceID:  "i-self",
		PrivateIP:   "10.0.1.51",
		Region:      "us-east-1",
		ClusterName: "logexplorer",
		DNSZone:     "ch.internal",
		CRName:      "01-5",
		CROrdinal:   1,
		LeaseTable:  "node-claims",
	}

	newBoot := func(dir string, claimer *refreshClaimer) (*Boot, *int) {
		persisted := 0
		spec := specFor()
		b := &Boot{
			Spec:      &spec,
			Provider:  &refreshLeaseProvider{refreshClaimer: claimer},
			Refresh:   true,
			StatePath: writeStateFile(dir, settled),
			Meta: port.InstanceMetadata{
				Provider:   "aws",
				Region:     "us-east-1",
				InstanceID: "i-self",
				PrivateIP:  "10.0.1.51",
			},
		}
		b.SetHostname = func(context.Context, string) error { return nil }
		b.PersistIdentity = func(model.NodeIdentity) { persisted++ }
		return b, &persisted
	}

	It("renews the persisted lease and NEVER calls ClaimOrdinal", func() {
		dir := GinkgoT().TempDir()
		claimer := &refreshClaimer{}
		b, persisted := newBoot(dir, claimer)

		Expect(b.settleIdentity(context.Background())).To(Succeed())
		Expect(claimer.claims).To(Equal(0), "refresh must not re-claim a lease the node already holds")
		Expect(claimer.renews).To(HaveLen(1))
		Expect(claimer.renews[0].Hostname).To(Equal("reader-1"))
		Expect(b.Identity.Hostname).To(Equal("reader-1"))
		Expect(b.Identity.CRName).To(Equal("01-5"))
		Expect(*persisted).To(BeNumerically(">=", 1))
	})

	It("fails closed when the renew reports a stolen lease (the port sentinel)", func() {
		dir := GinkgoT().TempDir()
		claimer := &refreshClaimer{renewErr: fmt.Errorf("%w: renew reader-1 as i-self", port.ErrLeaseStolen)}
		b, _ := newBoot(dir, claimer)

		err := b.settleIdentity(context.Background())
		Expect(err).To(HaveOccurred())
		Expect(fail.IsFatal(err)).To(BeTrue(), "a stolen lease is fatal: the node must not serve under a recycled name")
		Expect(err.Error()).To(ContainSubstring("reader-1"))
		Expect(err.Error()).To(ContainSubstring("re-image"))
	})

	It("answers retryable (not Fatal) when the renew fails transiently", func() {
		dir := GinkgoT().TempDir()
		// A throttling-class blip carries NO stolen-lease sentinel —
		// the claim policy owns the retry; only an exhausted sweep
		// fails the refresh (the same Fatal-vs-retryable split the
		// disk-attach race needed).
		claimer := &refreshClaimer{renewErr: errors.New("provisioned throughput exceeded for table")}
		b, _ := newBoot(dir, claimer)

		err := b.settleIdentity(context.Background())
		Expect(err).To(HaveOccurred())
		Expect(fail.IsFatal(err)).To(BeFalse(),
			"a transient API blip must stay retryable: Fatal would abort a healthy refresh on one throttled call")
		Expect(err.Error()).To(ContainSubstring("lease renew"))
	})

	It("refuses to refresh when the state file belongs to a different instance", func() {
		dir := GinkgoT().TempDir()
		claimer := &refreshClaimer{}
		b, _ := newBoot(dir, claimer)
		b.Meta.InstanceID = "i-someone-else"

		err := b.settleIdentity(context.Background())
		Expect(err).To(HaveOccurred())
		Expect(fail.IsFatal(err)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("foreign identity"))
		Expect(claimer.renews).To(BeEmpty(), "no lease traffic may happen for a foreign state file")
	})

	It("refuses to refresh when the spec role differs from the persisted role", func() {
		dir := GinkgoT().TempDir()
		claimer := &refreshClaimer{}
		b, _ := newBoot(dir, claimer)
		b.Spec.Role = model.RoleWriter

		err := b.settleIdentity(context.Background())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("role"))
	})

	It("fails closed when the state file is missing (nothing to refresh against)", func() {
		dir := GinkgoT().TempDir()
		claimer := &refreshClaimer{}
		b, _ := newBoot(dir, claimer)
		b.StatePath = filepath.Join(dir, "absent.json")

		err := b.settleIdentity(context.Background())
		Expect(err).To(HaveOccurred())
		Expect(fail.IsFatal(err)).To(BeTrue())
	})
})
