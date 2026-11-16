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
)

// This file pins ApplyBootstrapDDL — the shared DDL-convergence body of
// the boot's bootstrap_schema step and the `chinit schema-bootstrap`
// timer unit. The 2026-11-16 finding: a reader exhausted the boot's
// retries while the keeper rolled in the same instance refresh and
// stayed schemaless for HOURS because nothing re-ran the DDL after the
// boot gave up. These specs pin the contract the timer's self-heal
// depends on: the pass is idempotent, failures COUNT, and the Code 253
// stale-znode path drops this node's OWN registration before retrying.

var _ = Describe("ApplyBootstrapDDL", func() {
	var (
		dir           string
		queries       []string
		failOn        map[string]string
		restoreExec   func()
		restorePaths  func()
	)

	BeforeEach(func() {
		dir = filepath.Join(os.TempDir(), "chinit-apply-ddl-test")
		Expect(os.MkdirAll(dir, 0755)).To(Succeed())

		Expect(os.WriteFile(filepath.Join(dir, "bootstrap-ddl.sql"), []byte(
			`-- Bundle DDL
CREATE TABLE IF NOT EXISTS events (id String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/01/events', '{replica}');
CREATE TABLE IF NOT EXISTS staging_synth (id String) ENGINE = S3Queue('s3://x/staging/', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'CSV');
`), 0644)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(dir, "admin-password"), []byte("sekrit\n"), 0600)).To(Succeed())

		queries = nil
		failOn = map[string]string{}
		execClientFn = func(_ context.Context, _, query string) (string, error) {
			queries = append(queries, query)
			if msg, ok := failOn[query]; ok {
				return msg, &errString{msg}
			}
			return "", nil
		}
		restoreExec = func() { execClientFn = nil }
		DeferCleanup(restoreExec)

		ddl := bootstrapDDLPath
		pw := bootstrapAdminPasswordPath
		restorePaths = func() { bootstrapDDLPath, bootstrapAdminPasswordPath = ddl, pw }
		bootstrapDDLPath = func() string { return filepath.Join(dir, "bootstrap-ddl.sql") }
		bootstrapAdminPasswordPath = func() string { return filepath.Join(dir, "admin-password") }
		DeferCleanup(restorePaths)
	})

	It("applies every CREATE and reports zero failures on a clean pass", func() {
		result, err := ApplyBootstrapDDL(context.Background(), "writer-0")
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Created).To(Equal(2))
		Expect(result.Failed).To(Equal(0))
		// Both statements reached the client; the {replica} macro form
		// is passed through for server-side substitution.
		Expect(queries).To(HaveLen(2))
		Expect(queries[0]).To(ContainSubstring("/clickhouse/tables/01/events"))
		Expect(queries[0]).To(ContainSubstring("'{replica}'"))
	})

	It("counts failures so the caller surfaces a retryable error", func() {
		failOn["CREATE TABLE IF NOT EXISTS events (id String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/01/events', '{replica}')"] =
			"Code: 999. ZooKeeper connection refused"
		result, err := ApplyBootstrapDDL(context.Background(), "writer-0")
		Expect(err).NotTo(HaveOccurred(), "the pass itself errors only on missing credentials; statement failures COUNT")
		Expect(result.Failed).To(Equal(1))
		Expect(result.Created).To(Equal(1))
	})

	It("drops the node's OWN stale znode on Code 253 and retries the CREATE", func() {
		const stmt = "CREATE TABLE IF NOT EXISTS events (id String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/01/events', '{replica}')"
		failOnce := true
		execClientFn = func(_ context.Context, _, query string) (string, error) {
			queries = append(queries, query)
			if query == stmt && failOnce {
				failOnce = false
				return "Code: 253. Replica /clickhouse/tables/01/events already exists", &errString{"Code: 253"}
			}
			return "", nil
		}
		result, err := ApplyBootstrapDDL(context.Background(), "writer-0")
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Created).To(Equal(2))
		Expect(result.Failed).To(Equal(0))
		// The drop names THIS node's replica (hostname substitution of
		// the {replica} macro) and its OWN zk path.
		Expect(queries).To(ContainElement("SYSTEM DROP REPLICA 'writer-0' FROM ZKPATH '/clickhouse/tables/01/events'"))
		// The retried CREATE follows the drop.
		Expect(queries).To(ContainElement(stmt))
	})

	It("no-ops loudly when the bundle carries no DDL (pre-0.2.14 AMI)", func() {
		Expect(os.Remove(filepath.Join(dir, "bootstrap-ddl.sql"))).To(Succeed())
		result, err := ApplyBootstrapDDL(context.Background(), "writer-0")
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Created).To(Equal(0))
		Expect(result.Failed).To(Equal(0))
		Expect(queries).To(BeEmpty())
	})
})

// errString is the minimal error the fake exec returns — the pass only
// needs err != nil and the query text to classify.
type errString struct{ s string }

func (e *errString) Error() string { return e.s }
