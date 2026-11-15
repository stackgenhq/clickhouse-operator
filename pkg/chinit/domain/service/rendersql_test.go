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
)

// This file pins the bootstrap DDL's statement splitter — the seam the
// 2026-11-15 finding broke live: a naive strings.Split(";") shredded
// the bundle's line comments (the first statement reached the server
// still prefixed by "-- ... chinit's bootstrap_schema boot step
// executes it ..." — Code 62 at position 1) and its CODEC/interval
// parens (ZSTD(1) split mid-expression), leaving a fully-churned
// fleet WITHOUT its S3Queue drain surface.

var _ = Describe("splitSQLStatements", func() {
	It("splits on top-level semicolons only", func() {
		sql := "CREATE TABLE a (x String) ENGINE = MergeTree;\nCREATE TABLE b (y String) ENGINE = MergeTree;\n"
		statements := splitSQLStatements(sql)
		Expect(statements).To(HaveLen(2))
		Expect(statements[0]).To(ContainSubstring("CREATE TABLE a"))
		Expect(statements[1]).To(ContainSubstring("CREATE TABLE b"))
	})

	It("keeps parenthesized semicolons inside one statement", func() {
		// The CODEC/interval class: the parens' own structure never
		// terminates a statement, and the settings block's semicolon
		// (inside parens) stays part of it.
		sql := "CREATE TABLE t (ts DateTime CODEC(Delta(8), ZSTD(1))) TTL ts + toIntervalDay(7) SETTINGS index_granularity = 8192;"
		statements := splitSQLStatements(sql)
		Expect(statements).To(HaveLen(1))
		Expect(statements[0]).To(ContainSubstring("ZSTD(1)"))
		Expect(statements[0]).To(ContainSubstring("index_granularity = 8192"))
	})

	It("strips line comments before splitting", func() {
		// The live failure shape: a comment header above the first
		// statement — the naive split delivered the comment text AS
		// the statement's prefix (Code 62 at "chinit").
		sql := "-- Why this exists: the churn finding.\n-- Second comment line.\nCREATE TABLE c (z String) ENGINE = MergeTree;"
		statements := splitSQLStatements(sql)
		Expect(statements).To(HaveLen(1))
		Expect(statements[0]).NotTo(ContainSubstring("churn finding"))
		Expect(statements[0]).To(ContainSubstring("CREATE TABLE c"))
	})

	It("drops comment-only segments and trailing whitespace", func() {
		sql := "-- header comment\n\nCREATE TABLE d (w String);\n-- trailing comment\n"
		statements := splitSQLStatements(sql)
		onlyCreate := []string{}
		for _, s := range statements {
			if len(s) > 0 && containsCreate(s) {
				onlyCreate = append(onlyCreate, s)
			}
		}
		Expect(onlyCreate).To(HaveLen(1))
		Expect(onlyCreate[0]).To(ContainSubstring("CREATE TABLE d"))
	})

	It("emits an unterminated final statement", func() {
		sql := "CREATE TABLE e (v String) ENGINE = MergeTree"
		statements := splitSQLStatements(sql)
		Expect(statements).To(HaveLen(1))
		Expect(statements[0]).To(ContainSubstring("CREATE TABLE e"))
	})
})

// containsCreate mirrors bootstrapSchema's own filter (the caller's
// CREATE gate) without importing the step's body into the spec.
func containsCreate(stmt string) bool {
	for _, line := range splitLinesFor(stmt) {
		if len(line) >= 6 && line[:6] == "CREATE" {
			return true
		}
	}
	return false
}

// splitLinesFor trims each line the way the step's TrimSpace does.
func splitLinesFor(stmt string) []string {
	var out []string
	acc := ""
	for _, r := range stmt {
		if r == '\n' {
			out = append(out, acc)
			acc = ""
			continue
		}
		acc += string(r)
	}
	out = append(out, acc)
	return out
}


var _ = Describe("ddlReplicaIdentity", func() {
	It("extracts path and hostname from the macro form", func() {
		stmt := `CREATE TABLE IF NOT EXISTS default.events (timestamp DateTime64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/01/events', '{replica}') ORDER BY tuple()`
		path, replica := ddlReplicaIdentity(stmt, "writer-0")
		Expect(path).To(Equal("/clickhouse/tables/01/events"))
		Expect(replica).To(Equal("writer-0"))
	})

	It("extracts a literal replica name as-is", func() {
		stmt := `CREATE TABLE x.y ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/01/hourly', 'reader-2') ORDER BY tuple()`
		path, replica := ddlReplicaIdentity(stmt, "writer-0")
		Expect(path).To(Equal("/clickhouse/tables/01/hourly"))
		Expect(replica).To(Equal("reader-2"))
	})

	It("answers empty for non-replicated creates (queue, MVs)", func() {
		path, replica := ddlReplicaIdentity("CREATE TABLE s (x String) ENGINE = S3Queue('https://bucket/p', 'NDJSON', 'auto')", "writer-0")
		Expect(path).To(BeEmpty())
		Expect(replica).To(BeEmpty())
	})
})
