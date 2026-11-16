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
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	chinitsvc "github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
)

// ValidateXML checks a ClickHouse config fragment: non-empty, a root
// element the config-merge parser accepts (<clickhouse> / <yandex>), and
// well-formed (the Go XML decoder replaces xmllint --noout; no external
// binary needed).
//
// Why strict: a malformed fragment in config.d makes clickhouse-server
// refuse to start — the bash script validated before install for exactly
// this reason, and a broken downloaded bundle must fail the install
// step loudly instead of poisoning the config directory.
func ValidateXML(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return chinitsvc.Fatal(fmt.Errorf("XML file is empty: %s", path))
	}

	// Well-formedness via a token walk (xml.Unmarshal would require a
	// concrete element; fragments have varying roots).
	dec := xml.NewDecoder(bytes.NewReader(data))
	for {
		if _, err := dec.Token(); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return chinitsvc.Fatal(fmt.Errorf("XML validation failed for %s: %w", path, err))
		}
	}
	if err := ValidateConfigSemantics(path, data); err != nil {
		return err
	}
	return nil
}

// userLevelSettingsAtTopLevel are the settings ClickHouse 26.x REJECTS
// at the top level of a server config fragment (Code 137
// UNKNOWN_ELEMENT_IN_CONFIG — "user-level setting that should be
// located in users.xml inside <profiles>"). The 2026-11-16 backup-0
// crash-loop round one: backup-replica-settings.xml carried
// <max_threads> at the top and the server crash-looped (restart 67)
// until the template was fixed. chinit now refuses to install the
// class — the bundle gate catches it at boot instead of the server
// dying in a restart loop.
var userLevelSettingsAtTopLevel = map[string]bool{
	"max_threads":            true,
	"max_concurrent_queries": true, // server-level COUNTERPART exists; the bare setting name is user-level at top level
	"max_memory_usage":       true,
	"max_execution_time":     true,
	"max_insert_block_size":  true,
	"readonly":               true,
	"profile":                true,
}

// serverLevelSettings are SERVER settings that merely LOOK like merge
// tuning (the background pool family). Inside <merge_tree> ClickHouse
// 26.x rejects them with Code 115 UNKNOWN_SETTING — the 2026-11-16
// backup-0 crash-loop round two shipped exactly this shape (pool
// sizes inside <merge_tree>) and the server restart-looped until the
// template was fixed. At the fragment TOP level they are legal; chinit
// rejects only the misplaced-inside-merge_tree form.
var serverLevelSettings = map[string]bool{
	"background_pool_size":                          true,
	"background_merges_mutations_concurrency_ratio": true,
	"background_move_pool_size":                     true,
	"background_fetches_pool_size":                  true,
	"background_schedule_pool_size":                 true,
}

// mergeTreeScopedSettings live in <merge_tree> ONLY. At the top level
// of a config.d fragment 26.x rejects them (Code 115 UNKNOWN_SETTING).
var mergeTreeScopedSettings = map[string]bool{
	"always_fetch_merged_part":                                            true,
	"replicated_can_become_leader":                                        true,
	"number_of_free_entries_in_pool_to_lower_max_size_of_merge":           true,
	"number_of_free_entries_in_pool_to_execute_mutation":                  true,
	"number_of_free_entries_in_pool_to_execute_optimize_entire_partition": true,
}

// ValidateConfigSemantics walks one config fragment's element tree and
// rejects the settings-placement patterns that crash ClickHouse at
// STARTUP — the three fatal classes the 2026-11-16 convergence audit
// found in shipped config templates (Code 137 user-level settings at
// top level; Code 115 merge_tree-scoped settings outside
// <merge_tree>). The server's own startup check is the last line of
// defense; this is the first — a broken template fails the boot's
// install step ONCE with a pointed error instead of a crash loop.
func ValidateConfigSemantics(path string, data []byte) error {
	var root xmlNode
	if err := xml.Unmarshal(data, &root); err != nil {
		// Token-valid but structurally unmarshalable (mixed content
		// etc.) — not this validator's class; ValidateXML's walk
		// already passed it.
		return nil
	}
	// Fragment roots vary (<clickhouse>, <yandex>) — validate the
	// element's CHILDREN whatever the root name is.
	if err := validateNodeChildren(path, &root); err != nil {
		return err
	}
	// Cross-element semantics: the Code 36 class — a merge_tree
	// free-entry threshold must stay UNDER the background pool size
	// or ClickHouse 26.x refuses to start ("The value of
	// 'number_of_free_entries_in_pool_to_execute_mutation' setting
	// (20) ... is greater than the background pool size"). The
	// 2026-11-16 backup-0 crash-loop round three shipped exactly
	// this: a reduced pool (2) against the default thresholds (8/20/
	// 25). The values can sit in the SAME fragment or the thresholds
	// alone can ride one (the pool then lives in another fragment or
	// config.xml — the check only fires when BOTH are visible and
	// conflict).
	return validatePoolThresholds(path, &root)
}

// poolSettings carry the cross-checked pair: the background pool size
// and the merge_tree free-entry thresholds that must stay under it.
type poolSettings struct {
	backgroundPoolSize int
	thresholds         map[string]int
}

// collectPoolSettings walks the fragment for the background pool size
// (fragment top level) and the free-entry thresholds (<merge_tree>).
// Absent values report -1 (unknown — the check needs both).
func collectPoolSettings(node *xmlNode, ps *poolSettings) {
	inMergeTree := node.XMLName.Local == "merge_tree"
	for _, child := range node.Children {
		name := child.XMLName.Local
		switch {
		case name == "background_pool_size" && !inMergeTree:
			if v, ok := childInt(&child); ok {
				ps.backgroundPoolSize = v
			}
		case strings.HasPrefix(name, "number_of_free_entries_in_pool_to_") && inMergeTree:
			if v, ok := childInt(&child); ok {
				ps.thresholds[name] = v
			}
		}
		collectPoolSettings(&child, ps)
	}
}

// childInt parses an element's text content as an integer.
func childInt(node *xmlNode) (int, bool) {
	v, err := strconv.Atoi(strings.TrimSpace(node.Text))
	if err != nil {
		return 0, false
	}
	return v, true
}

// validatePoolThresholds enforces floor <= pool for every visible
// free-entry threshold against a fragment-visible pool size.
func validatePoolThresholds(path string, root *xmlNode) error {
	ps := poolSettings{backgroundPoolSize: -1, thresholds: map[string]int{}}
	collectPoolSettings(root, &ps)
	if ps.backgroundPoolSize < 0 || len(ps.thresholds) == 0 {
		return nil // not both visible in this fragment — nothing to cross
	}
	for name, threshold := range ps.thresholds {
		if threshold >= ps.backgroundPoolSize {
			return chinitsvc.Fatal(fmt.Errorf(
				"%s: <%s> (%d) is at or above <background_pool_size> (%d) — ClickHouse 26.x refuses to start with Code 36 "+
					"\"is greater than the background pool size\" (the 2026-11-16 backup-0 crash-loop round three: "+
					"a reduced pool against the default thresholds 8/20/25; scale the thresholds under the pool)",
				path, name, threshold, ps.backgroundPoolSize))
		}
	}
	return nil
}

// xmlNode is a generic element: the fragment tree walk needs names,
// nesting, and (for the pool cross-check) leaf text.
type xmlNode struct {
	XMLName  xml.Name
	Text     string    `xml:",chardata"`
	Children []xmlNode `xml:",any"`
}

func validateNodeChildren(path string, node *xmlNode) error {
	inMergeTree := node.XMLName.Local == "merge_tree"
	for _, child := range node.Children {
		name := child.XMLName.Local
		// User-level settings belong in users.d (<profiles>), never
		// in a server config fragment (Code 137 at startup).
		if userLevelSettingsAtTopLevel[name] && !inProfileTree(node) {
			return chinitsvc.Fatal(fmt.Errorf(
				"%s: <%s> is a user-level setting in a server config fragment — "+
					"ClickHouse 26.x refuses to start with Code 137 UNKNOWN_ELEMENT_IN_CONFIG "+
					"(move it to the spec's profiles: section; it renders into users.d)",
				path, name))
		}
		// Server-level pool settings INSIDE <merge_tree>: the
		// backup-0 round-two shape — Code 115 at startup.
		if serverLevelSettings[name] && inMergeTree {
			return chinitsvc.Fatal(fmt.Errorf(
				"%s: <%s> is a SERVER-level setting — inside <merge_tree> ClickHouse 26.x "+
					"refuses to start with Code 115 UNKNOWN_SETTING (move it to the fragment top level)",
				path, name))
		}
		// Merge-tree-scoped settings at the fragment top level: the
		// mirror error — Code 115 the other way.
		if mergeTreeScopedSettings[name] && !inMergeTree && !inProfileTree(node) {
			return chinitsvc.Fatal(fmt.Errorf(
				"%s: <%s> belongs inside <merge_tree> — at the fragment top level "+
					"ClickHouse 26.x refuses to start with Code 115 UNKNOWN_SETTING",
				path, name))
		}
		if err := validateNodeChildren(path, &child); err != nil {
			return err
		}
	}
	return nil
}

// inProfileTree reports whether node is a profiles subtree (users.d
// renders carry these; a setting inside <profiles>/<default> is legal).
func inProfileTree(node *xmlNode) bool {
	return node.XMLName.Local == "profiles" || node.XMLName.Local == "default" || node.XMLName.Local == "readonly"
}

// StaticConfigInstaller moves the downloaded bundle's static XMLs into
// /etc/clickhouse-server/config.d with validation and ownership, and
// retires the pre-chinit hand-written overlays whose duplicate sections
// would conflict at config-merge time with the rendered files.
type StaticConfigInstaller struct {
	runner Runner
	// Role gates the backup-replica-specific overlay installed from the
	// bundle (the settings file is fetched and hash-covered for every
	// role; only the backup node installs it).
	Role model.Role
}

// NewStaticConfigInstaller builds the installer.
// defaultBackupOverlayXML is the built-in backup-replica overlay — the
// exact shape the fixed Terraform template ships, rendered by chinit
// when the bundle carries no copy (the port-to-chinit 2026-11-16 fix:
// the backup's role tuning no longer depends on a Terraform-rendered
// file existing). Every value is the one the 2026-11-16 crash-loop
// trilogy converged on: pools reduced (the backup neither merges nor
// serves queries), thresholds scaled UNDER the pool (Code 36), pools at
// the fragment top level (Code 115), and NO user-level settings
// (Code 137). ValidateConfigSemantics gates this shape itself — the
// built-in must pass the same rules it enforces on the bundle's copy.
// fileExists reports whether the path names an existing regular file.
func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func defaultBackupOverlayXML() string {
	return `<?xml version="1.0"?>
<clickhouse>
    <merge_tree>
        <always_fetch_merged_part>1</always_fetch_merged_part>
        <replicated_can_become_leader>0</replicated_can_become_leader>
        <number_of_free_entries_in_pool_to_lower_max_size_of_merge>1</number_of_free_entries_in_pool_to_lower_max_size_of_merge>
        <number_of_free_entries_in_pool_to_execute_mutation>1</number_of_free_entries_in_pool_to_execute_mutation>
        <number_of_free_entries_in_pool_to_execute_optimize_entire_partition>1</number_of_free_entries_in_pool_to_execute_optimize_entire_partition>
    </merge_tree>
    <background_pool_size>2</background_pool_size>
    <background_merges_mutations_concurrency_ratio>1</background_merges_mutations_concurrency_ratio>
    <background_move_pool_size>2</background_move_pool_size>
    <background_fetches_pool_size>4</background_fetches_pool_size>
    <background_schedule_pool_size>2</background_schedule_pool_size>
</clickhouse>
`
}

func NewStaticConfigInstaller(runner Runner) *StaticConfigInstaller {
	return &StaticConfigInstaller{runner: runner}
}

// WithRole pins the node's role for the role-conditional overlays.
func (i *StaticConfigInstaller) WithRole(role model.Role) *StaticConfigInstaller {
	i.Role = role
	return i
}

// retiredOverlays are files earlier stacks shipped by hand. A lingering
// copy is config drift: chinit renders these sections now, and the merge
// conflicts (duplicate <macros>, <remote_servers>, profiles) break the
// server config parse. Removed on every boot — idempotent by deletion.
var retiredOverlays = []string{
	"/etc/clickhouse-server/config.d/macros.xml",
	"/etc/clickhouse-server/config.d/cluster.xml",
	"/etc/clickhouse-server/users.d/z_users.xml",
	"/etc/clickhouse-server/config.d/settings.xml",
	"/etc/clickhouse-server/config.d/logger.xml",
	"/etc/clickhouse-server/config.d/query-log.xml",
	"/etc/clickhouse-server/config.d/prometheus.xml",
	"/etc/clickhouse-server/users.d/delete_default_user.xml",
}

// staticConfigs maps bundle filenames to their install destinations.
// The network/ssl/storage trio is the static remainder — everything
// else renders from the CHI spec via the render library.
var staticConfigs = map[string]string{
	"network.xml": "/etc/clickhouse-server/config.d/network.xml",
	"ssl.xml":     "/etc/clickhouse-server/config.d/ssl.xml",
	"storage.xml": "/etc/clickhouse-server/config.d/storage.xml",
}

// Install converges the static configs: validate each bundle XML, copy
// to its destination, fix ownership/perms. A missing or invalid bundle
// member is Fatal — the bundle SHA gate should have caught corruption,
// so reaching here with a bad file means the gate was skipped and the
// node must not boot half-configured.
func (i *StaticConfigInstaller) Install(ctx context.Context, bundleDir string) error {
	for _, gone := range retiredOverlays {
		if err := os.Remove(gone); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("retire %s: %w", gone, err)
		}
	}

	for name, dest := range staticConfigs {
		src := filepath.Join(bundleDir, name)
		if err := ValidateXML(src); err != nil {
			return err
		}
		data, err := os.ReadFile(src)
		if err != nil {
			return chinitsvc.Fatal(fmt.Errorf("read bundle config %s: %w", name, err))
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
			return chinitsvc.Fatal(fmt.Errorf("create %s: %w", filepath.Dir(dest), err))
		}
		if err := os.WriteFile(dest, data, 0644); err != nil {
			return chinitsvc.Fatal(fmt.Errorf("install %s: %w", name, err))
		}
		if _, err := i.runner.Run(ctx, "chown", "clickhouse:clickhouse", dest); err != nil {
			return fmt.Errorf("chown %s: %w", dest, err)
		}
	}

	// backup-replica-settings.xml: the backup's role overlay. The
	// bundle contract NO LONGER carries it (the 2026-11-16 port-to-
	// chinit): defaultBackupOverlayXML is the single source, rendered
	// here — the installer owns the role gate because it is the only
	// place the node's role and the config installation meet. A legacy
	// bundle that still ships a copy keeps its content (the last
	// bundles pre-0.2.32), validated against the same placement rules.
	if i.Role == model.RoleBackup {
		const brsName = "backup-replica-settings.xml"
		var data []byte
		if src := filepath.Join(bundleDir, brsName); fileExists(src) {
			b, err := os.ReadFile(src)
			if err != nil {
				return chinitsvc.Fatal(fmt.Errorf("read bundle config %s: %w", brsName, err))
			}
			if verr := ValidateXML(src); verr != nil {
				return verr
			}
			data = b
		} else {
			data = []byte(defaultBackupOverlayXML())
		}
		const dest = "/etc/clickhouse-server/config.d/backup-replica-settings.xml"
		if err := os.WriteFile(dest, data, 0644); err != nil {
			return chinitsvc.Fatal(fmt.Errorf("install %s: %w", brsName, err))
		}
		if _, err := i.runner.Run(ctx, "chown", "clickhouse:clickhouse", dest); err != nil {
			return fmt.Errorf("chown %s: %w", dest, err)
		}
	} else {
		// Non-backup roles MUST NOT carry the overlay — a lingering copy
		// on a writer would disable merges on a serving node. Removing
		// it here is the same converge-the-drift discipline as the
		// retiredOverlays block above.
		_ = os.Remove("/etc/clickhouse-server/config.d/backup-replica-settings.xml")
	}
	return nil
}
