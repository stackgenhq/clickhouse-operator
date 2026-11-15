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
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// This file pins the periodic /etc/hosts convergence — the half of the
// peer-hosts contract the boot step cannot own alone. The 2026-11-15
// incident: a reader booted mid-churn, the boot step's live-claims
// scan legitimately found ZERO writers, and the empty hosts file sat
// for hours while every part fetch died on bare-hostname DNS_ERROR.
// The convergence is the lease-renew timer's second half.

// errBoom is the canned listing failure (a DynamoDB outage).
var errBoom = errors.New("dynamodb outage")

// splitLines splits file content on newlines (blank lines collapse).
func splitLines(s string) []string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		if strings.TrimSpace(line) != "" {
			out = append(out, line)
		}
	}
	return out
}

// containsField reports whether a space-separated line carries the
// exact field.
func containsField(line, field string) bool {
	for _, f := range strings.Fields(line) {
		if f == field {
			return true
		}
	}
	return false
}

var _ = Describe("ConvergePeerHosts", func() {
	var hostsPath string

	BeforeEach(func() {
		dir, err := os.MkdirTemp("", "chinit-hosts-*")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = os.RemoveAll(dir) })
		hostsPath = filepath.Join(dir, "hosts")
		orig := peerHostsPath
		peerHostsPath = func() string { return hostsPath }
		DeferCleanup(func() { peerHostsPath = orig })

		// A realistic starting file: the localhost lines stay, one
		// stale writer line from a prior boot gets swept.
		Expect(os.WriteFile(hostsPath, []byte("127.0.0.1 localhost\n10.9.9.9 writer-1 writer-1.old.zone\n"), 0644)).To(Succeed())
	})

	Context("with live writer claims", func() {
		It("writes short + FQDN lines and sweeps stale peer lines", func() {
			lister := &fakeLeaseLister{claims: []port.Claim{
				{Hostname: "writer-0", IP: "10.0.1.69"},
				{Hostname: "writer-1", IP: "10.0.1.79"},
				{Hostname: "reader-0", IP: "10.0.1.211"},
			}}
			err := ConvergePeerHosts(context.Background(), &listerProvider{lister}, ConvergeHostsArgs{
				LeaseTable:   "claims",
				WriterPrefix: "writer-",
				ReaderPrefix: "reader-",
				DNSZoneName:  "zone.internal",
			})
			Expect(err).NotTo(HaveOccurred())

			data, _ := os.ReadFile(hostsPath)
			Expect(string(data)).To(ContainSubstring("10.0.1.69 writer-0 writer-0.zone.internal"))
			Expect(string(data)).To(ContainSubstring("10.0.1.79 writer-1 writer-1.zone.internal"))
			Expect(string(data)).To(ContainSubstring("10.0.1.211 reader-0 reader-0.zone.internal"))
			// The stale line from a prior boot is gone, the localhost
			// line stays.
			Expect(string(data)).NotTo(ContainSubstring("10.9.9.9"))
			Expect(string(data)).To(ContainSubstring("127.0.0.1 localhost"))
		})

		It("skips foreign and empty-IP claims", func() {
			lister := &fakeLeaseLister{claims: []port.Claim{
				{Hostname: "keeper-0", IP: "10.0.1.20"},
				{Hostname: "writer-3", IP: ""},
			}}
			err := ConvergePeerHosts(context.Background(), &listerProvider{lister}, ConvergeHostsArgs{
				LeaseTable:   "claims",
				WriterPrefix: "writer-",
				ReaderPrefix: "reader-",
				DNSZoneName:  "zone.internal",
			})
			Expect(err).NotTo(HaveOccurred())

			data, _ := os.ReadFile(hostsPath)
			Expect(string(data)).NotTo(ContainSubstring("keeper-0"))
			Expect(string(data)).NotTo(ContainSubstring("writer-3"))
		})
	})

	Context("with zero live claims (the mid-churn boot state)", func() {
		It("writes an empty-but-clean hosts file, never fails", func() {
			lister := &fakeLeaseLister{claims: nil}
			err := ConvergePeerHosts(context.Background(), &listerProvider{lister}, ConvergeHostsArgs{
				LeaseTable:   "claims",
				WriterPrefix: "writer-",
				ReaderPrefix: "reader-",
				DNSZoneName:  "zone.internal",
			})
			// Zero claims is LEGITIMATE (mid-churn); the file simply
			// carries no peer lines — the next timer tick converges.
			Expect(err).NotTo(HaveOccurred())
			data, _ := os.ReadFile(hostsPath)
			Expect(string(data)).NotTo(ContainSubstring("writer-"))
		})
	})

	Context("degradation paths", func() {
		It("no-ops (no error) when the provider cannot list leases", func() {
			err := ConvergePeerHosts(context.Background(), &bareListerProvider{}, ConvergeHostsArgs{
				LeaseTable:   "claims",
				WriterPrefix: "writer-",
				ReaderPrefix: "reader-",
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("keeps the file and returns nil when the listing errors", func() {
			lister := &fakeLeaseLister{err: errBoom}
			err := ConvergePeerHosts(context.Background(), &listerProvider{lister}, ConvergeHostsArgs{
				LeaseTable:   "claims",
				WriterPrefix: "writer-",
				ReaderPrefix: "reader-",
			})
			Expect(err).NotTo(HaveOccurred())
			data, _ := os.ReadFile(hostsPath)
			Expect(string(data)).To(ContainSubstring("10.9.9.9 writer-1"))
		})

		It("no-ops when the identity carries no lease table", func() {
			lister := &fakeLeaseLister{claims: []port.Claim{{Hostname: "writer-0", IP: "10.0.1.69"}}}
			err := ConvergePeerHosts(context.Background(), &listerProvider{lister}, ConvergeHostsArgs{
				LeaseTable: "",
			})
			Expect(err).NotTo(HaveOccurred())
			data, _ := os.ReadFile(hostsPath)
			Expect(string(data)).NotTo(ContainSubstring("10.0.1.69"))
		})
	})

	Context("convergence (the re-run contract)", func() {
		It("a second run with changed IPs replaces the old lines", func() {
			lister := &fakeLeaseLister{claims: []port.Claim{{Hostname: "writer-0", IP: "10.0.1.69"}}}
			p := &listerProvider{lister}
			args := ConvergeHostsArgs{LeaseTable: "claims", DNSZoneName: "zone.internal"}
			Expect(ConvergePeerHosts(context.Background(), p, args)).To(Succeed())
			Expect(ConvergePeerHosts(context.Background(), p, args)).To(Succeed())

			// The writer changed IPs (replacement instance); the next
			// convergence must REPLACE, not append.
			lister.claims = []port.Claim{{Hostname: "writer-0", IP: "10.0.1.99"}}
			Expect(ConvergePeerHosts(context.Background(), p, args)).To(Succeed())

			data, _ := os.ReadFile(hostsPath)
			Expect(string(data)).To(ContainSubstring("10.0.1.99 writer-0"))
			Expect(string(data)).NotTo(ContainSubstring("10.0.1.69"))
			// Exactly ONE line for the claim (short + FQDN on the same
			// line — the sweep removed the prior boot's line).
			writerLines := 0
			for _, line := range splitLines(string(data)) {
				if len(line) > 0 && line[0] != '\n' && containsField(line, "writer-0") {
					writerLines++
				}
			}
			Expect(writerLines).To(Equal(1))
		})
	})
})

// bareListerProvider is a provider WITHOUT the LeaseLister capability
// (AsLeaseLister answers nil) — the no-lease-store cloud degradation.
type bareListerProvider struct{}

func (p *bareListerProvider) Name() string                            { return "azure" }
func (p *bareListerProvider) Metadata(context.Context) (port.InstanceMetadata, error) {
	return port.InstanceMetadata{}, nil
}
func (p *bareListerProvider) GetSecretValue(context.Context, port.SecretRequest) (string, error) {
	return "", nil
}
func (p *bareListerProvider) FetchBundle(context.Context, model.ConfigBundle, string) ([]string, error) {
	return nil, nil
}
