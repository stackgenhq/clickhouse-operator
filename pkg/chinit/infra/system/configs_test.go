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

package system

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// These specs pin ValidateConfigSemantics — the startup-fatal settings
// placement classes the 2026-11-16 convergence audit found in SHIPPED
// config templates (backup-0 crash-looped through three of them:
// Code 137, Code 115 inside merge_tree, and the floor>pool Code 36
// class). The server's own startup check is the last line of defense;
// this validator is the FIRST — a broken template fails the boot's
// install step ONCE with a pointed error instead of a crash loop.
var _ = Describe("ValidateConfigSemantics", func() {
	var dir string

	write := func(content string) string {
		path := filepath.Join(dir, "fragment.xml")
		Expect(os.WriteFile(path, []byte(content), 0644)).To(Succeed())
		return path
	}

	BeforeEach(func() {
		var err error
		dir, err = os.MkdirTemp("", "chinit-config-validate-*")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = os.RemoveAll(dir) })
	})

	It("accepts a legal server fragment", func() {
		path := write(`<clickhouse>
    <merge_tree>
        <always_fetch_merged_part>1</always_fetch_merged_part>
    </merge_tree>
    <background_pool_size>2</background_pool_size>
</clickhouse>
`)
		Expect(ValidateXML(path)).To(Succeed())
	})

	It("rejects a user-level setting at fragment top level (Code 137 class)", func() {
		// The backup-0 crash-loop round one: <max_threads> at the top
		// of backup-replica-settings.xml — restart counter 67.
		path := write(`<clickhouse>
    <merge_tree>
        <always_fetch_merged_part>1</always_fetch_merged_part>
    </merge_tree>
    <max_threads>4</max_threads>
</clickhouse>
`)
		err := ValidateXML(path)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("user-level setting"))
		Expect(err.Error()).To(ContainSubstring("max_threads"))
		Expect(err.Error()).To(ContainSubstring("Code 137"))
	})

	It("rejects a server-level pool setting inside merge_tree (Code 115 class)", func() {
		// The backup-0 crash-loop round two: background_pool_size
		// inside <merge_tree>.
		path := write(`<clickhouse>
    <merge_tree>
        <background_pool_size>2</background_pool_size>
    </merge_tree>
</clickhouse>
`)
		err := ValidateXML(path)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("SERVER-level setting"))
		Expect(err.Error()).To(ContainSubstring("background_pool_size"))
		Expect(err.Error()).To(ContainSubstring("Code 115"))
	})

	It("rejects a merge-tree-scoped setting at fragment top level (Code 115 mirror)", func() {
		// always_fetch_merged_part OUTSIDE <merge_tree>: 26.x rejects
		// it at startup with the same UNKNOWN_SETTING class.
		path := write(`<clickhouse>
    <always_fetch_merged_part>1</always_fetch_merged_part>
</clickhouse>
`)
		err := ValidateXML(path)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("inside <merge_tree>"))
		Expect(err.Error()).To(ContainSubstring("always_fetch_merged_part"))
	})

	It("accepts user-level settings inside a profiles tree (users.d shape)", func() {
		path := write(`<clickhouse>
    <profiles>
        <default>
            <max_threads>4</max_threads>
        </default>
    </profiles>
</clickhouse>
`)
		Expect(ValidateXML(path)).To(Succeed())
	})

	It("rejects the free-entry-threshold family at fragment top level", func() {
		// merge-tree-scoped settings belong in <merge_tree>; at the
		// top level they are Code 115 (the backup-0 round-three class
		// preceded the floor>pool fix).
		path := write(`<clickhouse>
    <number_of_free_entries_in_pool_to_execute_mutation>1</number_of_free_entries_in_pool_to_execute_mutation>
</clickhouse>
`)
		err := ValidateXML(path)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("inside <merge_tree>"))
	})
})
