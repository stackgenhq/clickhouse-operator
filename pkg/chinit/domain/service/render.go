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
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chinit/infra/keeper"
	"github.com/altinity/clickhouse-operator/pkg/chinit/infra/operator"
)

// renderNodeConfig renders the whole node through chctl's library —
// ONE call (identity is settled by now), installing host sections +
// inventory + users.d with ownership/perms/prune.
//
// Why the library and not the CLI: colocated binaries share the domain
// packages; the CLI boundary would re-parse flags, lose typed errors
// and fork a process per render. The NodeRenderer is the same code path
// the operator uses on Kubernetes.
func (b *Boot) renderNodeConfig(ctx context.Context) error {
	if b.Spec.Role == model.RoleKeeper {
		return nil // configure_keeper owns the keeper render
	}
	cr, err := b.LoadCHI(ctx)
	if err != nil {
		return Fatal(err)
	}

	// The settled identity's CRName is authoritative — NOT a fresh
	// CRName() call: settle_identity already mapped the reader ordinal
	// (writer_count + claimed), and re-applying the offset here
	// double-shifts the reader past the end of the replica list
	// (host "01-2" not found — the live chinit-cutover finding).
	crName := b.Identity.CRName
	if crName == "" {
		crName, _ = b.CRName()
	}

	renderer := operator.NewNodeRenderer(operator.NewConfigRenderer(), operator.NewUsersRenderer(providerSecrets{p: b.Provider}))
	request := port.RenderNodeRequest{
		NormalizedCR: cr,
		NodeType:     string(b.Spec.Role),
		HostName:     crName,
		OutputDir:    ConfigDir,
		UsersDir:     UsersDir,
		// Per-user IP allowlists from the spec: the users.d render
		// defaults to localhost-only when the map is empty, which
		// breaks every cross-node distributed query (Code 516
		// Authentication failed — the interserver peer arrives from
		// the VPC CIDR, not 127.0.0.1; the live chinit-cutover
		// finding). The bash era passed the same lists via -networks.
		Networks: map[string][]string{
			"admin":   withSubnet(b.Spec.Network.AdminAllowedNetworks, b.Spec.Network.SubnetCIDR),
			"writer":  withSubnet(b.Spec.Network.WriterAllowedNetworks, b.Spec.Network.SubnetCIDR),
			"default": withSubnet(b.Spec.Network.DefaultUserAllowedNetworks, b.Spec.Network.SubnetCIDR),
		},
	}
	if b.Spec.Role != model.RoleBackup {
		// The inventory host list renders remote_servers; writers and
		// readers are both CR members. The DNS-zone addressing is the
		// same convention the CHI spec's templates carry. The list is
		// built ONCE here (live-claims discovery is a lease-table scan;
		// the replica map and remote_servers must never disagree by
		// racing two scans).
		hosts, note := b.clusterHostsWithSource()
		if note != "" {
			slog.Warn(note)
			fmt.Fprintln(os.Stderr, note)
		}
		request.Hosts = hosts
		// Replica names (the {replica} macro value): without this map
		// the macros render the operator's POD pattern
		// (chi-<chi>-<cluster>-<shard>-<replica>) as the replica
		// identity — the keeper replica_path then registers under a
		// name no cloud tooling recognizes, and SYSTEM SYNC REPLICA
		// / DROP REPLICA operate on the wrong identity (the live
		// chinit-cutover finding). The bash era passed -replica
		// 01-$idx=$hostname.
		request.Replicas = map[string]string{}
		for _, h := range hosts {
			request.Replicas[h.Name] = strings.SplitN(h.Address, ".", 2)[0]
		}
		// Distributed-query credentials: without an injected
		// <user>/<password> in remote_servers, every cross-node query
		// authenticates as the passwordless default against nodes
		// whose default REQUIRES a password (Code 516 on every
		// clusterAllReplicas — the live chinit-cutover finding; the
		// bash era passed -remote-user/-remote-password-file).
		if pw, err := os.ReadFile(filepath.Join(BootstrapDir, "default-password")); err == nil {
			request.RemoteUser = "default"
			request.RemotePassword = strings.TrimSpace(string(pw))
		}
	}
	result, err := renderer.RenderNode(ctx, request)
	if err != nil {
		return Fatal(fmt.Errorf("render node config: %w", err))
	}
	_ = result
	return nil
}

// clusterHostsWithSource builds the CR inventory plus the provenance
// note for the boot log (live-claims vs static fallback + its
// warning) — split so the inventory tests assert the source without
// scraping stderr. The namer keys entries by the operator CR host name
// ({shard}-{index}, writers 01-0.. then readers
// 01-(writerCeiling+readerOrdinal).. — the same order CRName()
// derives), with the stack's private-zone DNS as the address. Keying
// by hostname instead of CR name renders remote_servers <host> entries
// as unresolvable chi-<chi>-<cluster>-<shard>-<replica> pod names (the
// live boot finding: server crash-loops on DNS_ERROR Code 198).
//
// The inventory's SOURCE OF TRUTH is the live lease table when the
// node holds a lease (identity_mode: lease): a clusterAllReplicas DDL
// read fans out to every host in this list, so a reserved-but-vacant
// slot (the ASG's max is a RESERVATION, not a member) would fail every
// fan-out with an unreachable host. The lease table owns liveness —
// only ACTIVE claim rows become hosts. The static spec lists stay as
// the fallback for clouds without the listing capability (Azure
// interim), reported loudly so a vacant-slot regression is visible in
// the boot log.
//
// Source selection:
//   - lease identity + listable lease table → ACTIVE claims (the
//     contract above). The table name is the SETTLED identity's
//     LeaseTable (the boot claimed from it; the spec field is the
//     pre-settle fallback for direct unit wiring).
//   - lease identity without the capability (Azure interim) or a
//     listing failure → static lists + a loud WARNING: a lease-mode
//     fleet's reserved lists contain vacant slots, and a silent
//     fallback would put unreachable hosts in clusterAllReplicas
//     reads with nothing in the log pointing at the cause.
//   - static identity → static lists, no warning: every static node
//     is a real machine, so the reserved list IS the member list.
func (b *Boot) clusterHostsWithSource() (hosts []domain.HostIdentity, note string) {
	if b.Spec.IdentityMode != model.IdentityLease {
		return staticSpecInventory(b), ""
	}
	table := b.Identity.LeaseTable
	if table == "" {
		table = b.Spec.ClaimerTableName
	}
	lister := port.AsLeaseLister(b.Provider)
	if lister == nil {
		return staticSpecInventory(b),
			fmt.Sprintf("WARNING: cloud %s has no lease-listing capability; inventory enumerated from static spec (may include vacant slots)", b.Provider.Name())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	claims, err := lister.ListActiveClaims(ctx, port.ListClaimsRequest{Table: table})
	if err != nil {
		// A lease-table outage must not wedge the boot in a half
		// inventory: fall back to the static lists and say so.
		return staticSpecInventory(b),
			fmt.Sprintf("WARNING: live-claims discovery on cloud %s failed (%s); inventory enumerated from static spec (may include vacant slots)", b.Provider.Name(), err)
	}
	return claimsInventory(b, claims), ""
}

// claimsInventory maps ACTIVE lease rows to CR slots: writers take
// 01-{writerOrdinal}; readers take 01-{writerCeiling+readerOrdinal}
// (writerCeiling = WriterMax, fallback WriterCount — the fixed-ceiling
// arithmetic CRName() lands, so reader slots never shift when the
// writer fleet scales). Ordinals at or beyond a role's ceiling are
// stale rows (a shrunk writer_max) — the CR no longer reserves their
// slot, so they are dropped. Order is CR slot (stable across runs: the
// lease-table Scan order is unordered). THIS node's claim is included
// deliberately: remote_servers and the -host inventories include self.
//
// Invariant: a lease-mode spec must carry writer_max (the Terraform
// render writes it into every role's spec — reader CR slots derive
// from it); with it unset the reader offset degenerates to WriterCount.
func claimsInventory(b *Boot, claims []port.Claim) []domain.HostIdentity {
	writerPrefix, readerPrefix := b.Spec.WriterPrefix(), b.Spec.ReaderPrefix()
	ceiling := b.Spec.WriterCeiling()
	readerBound := b.Spec.ReaderCeiling()
	var hosts []domain.HostIdentity
	for _, claim := range claims {
		ordinal := -1
		role := domain.Role("")
		switch {
		case strings.HasPrefix(claim.Hostname, writerPrefix):
			if n, err := strconv.Atoi(strings.TrimPrefix(claim.Hostname, writerPrefix)); err == nil && n >= 0 && (ceiling <= 0 || n < ceiling) {
				ordinal, role = n, domain.RoleWriter
			}
		case strings.HasPrefix(claim.Hostname, readerPrefix):
			if n, err := strconv.Atoi(strings.TrimPrefix(claim.Hostname, readerPrefix)); err == nil && n >= 0 && (readerBound <= 0 || n < readerBound) {
				ordinal, role = ceiling+n, domain.RoleReader
			}
		}
		if ordinal < 0 {
			continue // foreign row (backup-0, out-of-ceiling stale claim)
		}
		hosts = append(hosts, domain.HostIdentity{
			Cloud:   b.Provider.Name(),
			Region:  b.Meta.Region,
			Name:    fmt.Sprintf("01-%d", ordinal),
			Address: fmt.Sprintf("%s.%s", claim.Hostname, b.Spec.DNSZoneName),
			Role:    role,
		})
	}
	sort.SliceStable(hosts, func(i, j int) bool {
		// Claim keys are unique (hostname is the table key), so Name —
		// which encodes the slot — is a total order.
		return hosts[i].Name < hosts[j].Name
	})
	return hosts
}

// staticSpecInventory enumerates from the spec's reserved lists — the
// pre-lease inventory shape, kept as the fallback for clouds without
// the listing capability (Azure interim) and static-identity paths.
// Leased specs (writer_max set) enumerate the reserved writer ordinals
// by prefix — the same shape hostInventory (the app package's -host
// renderer) uses, so the boot's remote_servers and the schema-sync
// flags agree — and readers enumerate to ReaderMax when set (a
// scale-out past the baseline is a member the moment it claims)
// falling back to ReaderCount; the reader slot offset is
// WriterCeiling(), the same arithmetic the live path uses, so static
// and live inventories agree on CR slots.
func staticSpecInventory(b *Boot) []domain.HostIdentity {
	var hosts []domain.HostIdentity
	if b.Spec.WriterMax > 0 {
		prefix := b.Spec.WriterPrefix()
		for i := 0; i < b.Spec.WriterMax; i++ {
			name := fmt.Sprintf("%s%d", prefix, i)
			hosts = append(hosts, domain.HostIdentity{
				Cloud:   b.Provider.Name(),
				Region:  b.Meta.Region,
				Name:    fmt.Sprintf("01-%d", i),
				Address: fmt.Sprintf("%s.%s", name, b.Spec.DNSZoneName),
				Role:    domain.RoleWriter,
			})
		}
	} else {
		for i, name := range b.Spec.WriterNodes {
			hosts = append(hosts, domain.HostIdentity{
				Cloud:   b.Provider.Name(),
				Region:  b.Meta.Region,
				Name:    fmt.Sprintf("01-%d", i),
				Address: fmt.Sprintf("%s.%s", name, b.Spec.DNSZoneName),
				Role:    domain.RoleWriter,
			})
		}
	}
	ceiling := b.Spec.WriterCeiling()
	for i := 0; i < b.Spec.ReaderCeiling(); i++ {
		name := fmt.Sprintf("%s%d", b.Spec.ReaderPrefix(), i)
		hosts = append(hosts, domain.HostIdentity{
			Cloud:   b.Provider.Name(),
			Region:  b.Meta.Region,
			Name:    fmt.Sprintf("01-%d", ceiling+i),
			Address: fmt.Sprintf("%s.%s", name, b.Spec.DNSZoneName),
			Role:    domain.RoleReader,
		})
	}
	return hosts
}

// withSubnet merges the node subnet CIDR into a user's allowlist: the
// cluster's own peers, the NLB health checks and the config refresher
// all arrive from the subnet — without it every cross-node query is
// Code 516 Authentication failed.
func withSubnet(networks []string, subnetCIDR string) []string {
	if subnetCIDR == "" {
		return networks
	}
	for _, n := range networks {
		if n == subnetCIDR {
			return networks
		}
	}
	out := make([]string, 0, len(networks)+1)
	out = append(out, networks...)
	return append(out, subnetCIDR)
}

// configureKeeper renders the keeper single-file config, enables the
// keeper service, and clears stale coordination state (prevents
// split-brain on re-image). Keeper nodes skip the server start.
func (b *Boot) configureKeeper(ctx context.Context) error {
	if b.Spec.Role != model.RoleKeeper {
		return nil
	}
	// RENDER the complete keeper config from the CHK spec (the same
	// single-file chctl path the bash era ran): keeper launches with
	// an explicit --config so config.d auto-include is DISABLED —
	// without this render the node runs whatever stale config the
	// image or a previous era left behind (the live chinit-cutover
	// finding: keeper listened on 127.0.0.1 because the root-level
	// listen_host from the spec's files: section never rendered).
	cr, err := b.LoadCHK(ctx)
	if err != nil {
		return Fatal(err)
	}
	// Keeper CR hosts are 0-0..0-(N-1) in keeper_nodes order; THIS
	// node's ordinal is its position in that list.
	myOrdinal := -1
	for i, name := range b.Spec.KeeperNodes {
		if name == b.Identity.Hostname {
			myOrdinal = i
			break
		}
	}
	if myOrdinal < 0 {
		return Fatal(fmt.Errorf("keeper %s not in spec keeper_nodes %v", b.Identity.Hostname, b.Spec.KeeperNodes))
	}
	// server_id: the raft disk's myid file is authoritative across
	// re-images (ensemble identity lives on persistent state) — but
	// NEVER 0: keeper ids are 1-based, a stale 0 myid (from a broken
	// boot) would re-introduce the Code 568 mismatch; fall through to
	// the seeded 1-based value and rewrite the file. A fresh disk
	// seeds the ordinal+1.
	serverID := myOrdinal + 1
	myidPath := "/var/lib/clickhouse-keeper/log/myid"
	if data, err := os.ReadFile(myidPath); err == nil {
		if n, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && n > 0 {
			serverID = n
		}
	}
	if err := os.MkdirAll(filepath.Dir(myidPath), 0755); err == nil {
		_ = os.WriteFile(myidPath, []byte(fmt.Sprintf("%d", serverID)), 0644)
	}
	// Keeper addresses + raft ids: every ensemble member's cloud DNS,
	// with 1-BASED raft ids matching the server_id/myid convention —
	// the CR's zero-based replica indices render <id>0</id> and keeper
	// refuses to start (Code 568: our server id N not found in
	// raft_configuration — server_id is 1-based by the myid disk
	// convention; the live chinit-cutover finding).
	keeperAddrs := map[string]string{}
	raftIDs := map[string]int{}
	for i, name := range b.Spec.KeeperNodes {
		keeperAddrs[fmt.Sprintf("0-%d", i)] = fmt.Sprintf("%s.%s", name, b.Spec.DNSZoneName)
		raftIDs[fmt.Sprintf("0-%d", i)] = i + 1
	}
	renderer := operator.NewNodeRenderer(operator.NewConfigRenderer(), nil)
	_, err = renderer.RenderNode(ctx, port.RenderNodeRequest{
		NormalizedCR:    cr,
		NodeType:        string(b.Spec.Role),
		HostName:        fmt.Sprintf("0-%d", myOrdinal),
		Keeper:          true,
		SingleFilePath:  "/etc/clickhouse-keeper/keeper_config.xml",
		KeeperAddresses: keeperAddrs,
		RaftIDs:         raftIDs,
		ServerID:        serverID,
		OutputDir:       "/etc/clickhouse-keeper",
	})
	if err != nil {
		return Fatal(fmt.Errorf("keeper render: %w", err))
	}
	// Stale coordination state: a re-image with a live raft log wedges
	// the ensemble — the bash script wiped coordination/* deliberately.
	// BOTH path families exist: the package default renders
	// /var/lib/clickhouse/coordination/{logs,snapshots}; the stack's
	// keeper spec overrides to /var/lib/clickhouse-keeper/{log,snapshots}.
	// Clear and prepare BOTH so whichever the rendered config uses, the
	// keeper (running as user clickhouse) can write it — a root-owned
	// coordination dir is exit code 76 CANNOT_OPEN_FILE Permission
	// denied (the live chinit-cutover finding: the dir was created
	// AFTER the mount step's chown ran, as root, and never re-owned).
	//
	// Refresh mode MUST NOT wipe: the node is a LIVE ensemble member and
	// the raft log/snapshots are its vote. Wiping on a refresh would
	// amputate the member from quorum. (First boot on a fresh disk has
	// nothing to wipe anyway.)
	if !b.Refresh {
		for _, base := range []string{
			"/var/lib/clickhouse/coordination",
			"/var/lib/clickhouse-keeper",
		} {
			if err := os.RemoveAll(filepath.Join(base, "coordination")); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("clear keeper coordination state: %w", err)
			}
		}
	}
	for _, dir := range []string{
		"/var/lib/clickhouse/coordination/logs",
		"/var/lib/clickhouse/coordination/snapshots",
		"/var/lib/clickhouse-keeper/log",
		"/var/lib/clickhouse-keeper/snapshots",
	} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	out, err := runCommand(ctx, "chown", "-R", "clickhouse:clickhouse",
		"/var/lib/clickhouse/coordination", "/var/lib/clickhouse-keeper")
	if err != nil {
		return fmt.Errorf("chown keeper state dirs: %w: %s", err, out)
	}
	if b.Refresh {
		// A refresh must make the (possibly re-rendered) config take effect;
		// enable --now would leave the running keeper on the old config.
		// Raft state is on disk — a restart rejoins the ensemble.
		if out, rerr := runCommand(ctx, "systemctl", "restart", "clickhouse-keeper"); rerr != nil {
			return fmt.Errorf("restart clickhouse-keeper after config refresh: %w: %s", rerr, out)
		}
		return nil
	}
	out, err = runCommand(ctx, "systemctl", "enable", "--now", "clickhouse-keeper")
	if err != nil {
		return fmt.Errorf("enable clickhouse-keeper: %w: %s", err, out)
	}
	// clickhouse-server must NOT run on a keeper: keepers skip
	// render_node_config by design (configure_keeper owns the keeper
	// render), so no users.d ever renders and the server would serve
	// with the package's stock users. The deb's systemd preset enables
	// the unit anyway (the live 2026-11-16 finding: keeper-0 ran a
	// clickhouse-server answering AUTHENTICATION_FAILED for every
	// spec user — the "empty users.d" reports were the SERVER running
	// where it never should have). Disable + stop converges the drift
	// on both first boot and refresh.
	if out, derr := runCommand(ctx, "systemctl", "disable", "--now", "clickhouse-server"); derr != nil {
		return fmt.Errorf("disable clickhouse-server on keeper: %w: %s", derr, out)
	}
	return nil
}

// startClickhouse enables and starts the server, waits for /ping, and
// verifies the S3 disk is not broken. Keepers return immediately
// (clickhouse-server on a keeper node is wrong AND fails on
// keeper-only configs).
func (b *Boot) startClickhouse(ctx context.Context) error {
	if b.Spec.Role == model.RoleKeeper {
		return nil
	}

	// First-boot metadata cleanup (UNKNOWN_DISK avoidance): a blank
	// metadata disk with stale system tables from image-baking wedges
	// the first start. Skipped on refresh — wiping /var/lib/clickhouse
	// on a live node would delete the very data the node serves.
	marker := "/var/lib/clickhouse/first_boot_done"
	if b.Refresh {
		// fall through to the restart below
	} else if _, err := os.Stat(marker); os.IsNotExist(err) {
		_, _ = runCommand(ctx, "systemctl", "stop", "clickhouse-server")
		_ = os.RemoveAll("/var/lib/clickhouse/store")
		_ = os.RemoveAll("/var/lib/clickhouse/data/system")
		if err := os.MkdirAll("/var/lib/clickhouse/store", 0755); err != nil {
			return err
		}
		_, _ = runCommand(ctx, "chown", "-R", "clickhouse:clickhouse", "/var/lib/clickhouse/store")
		if err := os.WriteFile(marker, []byte("done"), 0644); err != nil {
			return err
		}
	}

	if out, err := runCommand(ctx, "systemctl", "enable", "clickhouse-server"); err != nil {
		return Fatal(fmt.Errorf("enable clickhouse-server: %w: %s", err, out))
	}
	if b.Refresh {
		// Refresh: make the re-rendered config take effect. restart
		// (not start) so a running server re-reads users.d/config.d.
		if out, err := runCommand(ctx, "systemctl", "restart", "clickhouse-server"); err != nil {
			return Fatal(fmt.Errorf("restart clickhouse-server: %w: %s", err, out))
		}
	} else if out, err := runCommand(ctx, "systemctl", "start", "clickhouse-server"); err != nil {
		return Fatal(fmt.Errorf("start clickhouse-server: %w: %s", err, out))
	}

	// /ping wait: the retry policy at the step level retries the whole
	// function; the ping loop is the bash wait_for_service shape.
	if err := waitForHTTP(ctx, "http://localhost:8123/ping", 30, 2); err != nil {
		return Fatal(fmt.Errorf("clickhouse-server did not become ready: %w", err))
	}

	// S3 disk health probe (a failed disk health check at boot marks
	// the S3 disk broken and excludes it — surfacing it in the boot log
	// is the diagnostic the bash script printed).
	out, _ := runCommand(ctx, "clickhouse-client", "--query",
		"SELECT name, is_broken FROM system.disks WHERE name = 's3'")
	if strings.Contains(out, "\t1") {
		fmt.Fprintf(os.Stderr, "WARNING: S3 disk is marked broken — check the instance role's s3:GetObject/PutObject on the data bucket\n")
	}
	return nil
}

// bootstrapSchema executes the bundle's bootstrap-ddl.sql idempotently
// on server nodes — the durable home for the fleet's schema (the
// 2026-11-15 full-churn finding: when every peer churns within one
// refresh, schema-sync has no peer to propagate from and the fleet
// stays schemaless until a manual DDL session recovers it from keeper
// metadata; the DDL now ships with the config bundle so every node
// re-materializes its own tables at boot).
//
// The DDL's {shard}/{replica} are CLICKHOUSE macros — the server
// substitutes them from chop-generated-macros.xml at statement time,
// so this function passes the file through UNCHANGED. Every statement
// is IF NOT EXISTS: a node that already holds the schema no-ops.
//
// BestEffort by design: a fleet with pre-existing znode drift (replica
// name conflicts from prior generations) fails a CREATE with
// REPLICA_ALREADY_EXISTS — that must not wedge the boot (the schema-sync
// unit and the health gate remain the heal paths); the failure is loud
// in the boot log for triage.
// bootstrapSchema is the boot step; the convergence body is
// ApplyBootstrapDDL (the `chinit schema-bootstrap` subcommand runs the
// same pass on its timer — the 2026-11-16 finding: reader-0 exhausted
// the boot's 5 apiRetry attempts while the keeper rolled in the same
// refresh and stayed schemaless for HOURS because nothing retried the
// bundle DDL after the boot gave up).
func (b *Boot) bootstrapSchema(ctx context.Context) error {
	if b.Spec.Role == model.RoleKeeper {
		return nil // keepers carry no tables
	}
	result, err := ApplyBootstrapDDL(ctx, b.Identity.Hostname)
	if err != nil {
		return err
	}
	fmt.Printf("schema bootstrap: %d statement(s) applied from %s (%d failed)\n", result.Created, result.DDLPath, result.Failed)
	// A retryable error surfaces when anything failed: the statement set
	// is idempotent (IF NOT EXISTS + the drop-own-znode conflict path),
	// so a re-run converges instead of wedging. BestEffort classification
	// still keeps a final-pass failure from killing the boot — the
	// schema-sync unit and health gate remain the heal paths.
	if result.Failed > 0 {
		return fmt.Errorf("schema bootstrap: %d statement(s) failed (retrying; transient keeper races converge on the next pass)", result.Failed)
	}
	return nil
}

// DDLApplyResult reports one convergence pass of the bundle DDL.
type DDLApplyResult struct {
	DDLPath string
	Created int
	Failed  int
}

// bootstrapDDLPath and bootstrapAdminPasswordPath are the path seams
// ApplyBootstrapDDL reads (tests redirect; production binds the
// bundle's constants — the peerHostsPath pattern).
var (
	bootstrapDDLPath          = func() string { return filepath.Join(BootstrapDir, "bootstrap-ddl.sql") }
	bootstrapAdminPasswordPath = func() string { return BootstrapDir + "/admin-password" }
)

// ApplyBootstrapDDL runs one idempotent pass of the bundle's
// bootstrap-ddl.sql on this node: IF NOT EXISTS creates, the
// Code 253 stale-znode drop-and-retry, and a failed count the caller
// turns into a retryable error. It is the shared body of the boot's
// bootstrap_schema step and the schema-bootstrap timer unit — the
// heal path for a boot that lost the keeper race.
func ApplyBootstrapDDL(ctx context.Context, hostname string) (*DDLApplyResult, error) {
	ddlPath := bootstrapDDLPath()
	data, err := os.ReadFile(ddlPath)
	if err != nil {
		// A bundle without the DDL is the pre-0.2.14 image shape —
		// loud no-op, never fatal (the schema-sync path remains).
		fmt.Fprintf(os.Stderr, "WARNING: bootstrap-ddl.sql not in the bundle (older AMI?) — schema bootstrap skipped: %v\n", err)
		return &DDLApplyResult{DDLPath: ddlPath}, nil
	}
	pw, err := os.ReadFile(bootstrapAdminPasswordPath())
	if err != nil {
		return nil, fmt.Errorf("read admin password file: %w", err)
	}
	adminPw := strings.TrimSpace(string(pw))
	result := &DDLApplyResult{DDLPath: ddlPath}

	// Statement-split: one statement per TOP-LEVEL terminator. The DDL
	// carries SQL comments (-- headers explaining each object) and
	// parenthesized defaults like CODEC(ZSTD(1)) / toIntervalDay(7) —
	// a naive strings.Split(";") shreds both (the live 2026-11-15
	// finding: the first statement arrived at the server still
	// prefixed by the comment text "chinit's bootstrap_schema boot
	// step executes it" — Code 62 at position 1 — and a CODEC's own
	// paren split mid-expression; five of eight statements failed,
	// leaving a churned fleet WITHOUT its S3Queue drain surface).
	// The splitter is comment-aware (line comments removed before
	// splitting; ClickHouse DDL in this bundle uses no block comments)
	// and delimiter-depth-aware (semicolons inside parentheses —
	// functions, codecs, tuples — never terminate a statement).
	statements := splitSQLStatements(string(data))
	for _, stmt := range statements {
		s := strings.TrimSpace(stmt)
		if s == "" || !strings.Contains(s, "CREATE") {
			continue
		}
		out, err := execClient(ctx, adminPw, s)
		if err != nil {
			// REPLICA_ALREADY_EXISTS (Code 253) is the NORMAL churn
			// case: this node's replica name matches a znode left by
			// its predecessor (the recycled-ordinal generation), and
			// CREATE refuses to attach over it — the statement logs
			// and CONTINUES, the table never materializes, and the
			// fleet converges on nothing (the live 2026-11-15
			// finding: 6 of 7 statements failed this way on every
			// churned writer while the boot printed "1 applied"). The
			// remedy is the drop-then-retry the drain and health-gate
			// paths already use: SYSTEM DROP REPLICA '<name>' FROM
			// ZKPATH '<path>' clears the node's OWN stale
			// registration (parts/queue znodes included — safe on a
			// fresh disk whose parts do not exist), and the retried
			// CREATE registers clean.
			if strings.Contains(out, "Code: 253") || strings.Contains(err.Error(), "Code: 253") {
				if tablePath, replicaName := ddlReplicaIdentity(s, hostname); tablePath != "" {
					dropSQL := fmt.Sprintf("SYSTEM DROP REPLICA '%s' FROM ZKPATH '%s'", replicaName, tablePath)
					if dOut, dErr := execClient(ctx, adminPw, dropSQL); dErr != nil {
						fmt.Fprintf(os.Stderr, "WARNING: bootstrap DDL stale-replica drop failed for %s: %v: %s\n", tablePath, dErr, strings.TrimSpace(dOut))
						result.Failed++
					} else if rOut, rErr := execClient(ctx, adminPw, s); rErr != nil {
						fmt.Fprintf(os.Stderr, "WARNING: bootstrap DDL statement failed after drop (continuing): %v: %s\n", rErr, strings.TrimSpace(rOut))
						result.Failed++
					} else {
						result.Created++
						fmt.Printf("schema bootstrap: %s re-created after dropping stale znode\n", tablePath)
					}
					continue
				}
			}
			// Other failures log and continue — but they COUNT: the
			// step returns an error when any statement failed so the
			// step-level apiRetry re-runs the whole idempotent pass
			// (the live 2026-11-15 keeper race: the CREATEs failed
			// Code 999 — ZooKeeper connection refused while the
			// keeper rolled in the same refresh — and the step
			// "succeeded" on the one statement that did not need
			// keeper, never retrying against the recovered keeper).
			result.Failed++
			fmt.Fprintf(os.Stderr, "WARNING: bootstrap DDL statement failed (continuing): %v: %s\n", err, strings.TrimSpace(out))
			continue
		}
		result.Created++
	}
	return result, nil
}

// ddlReplicaIdentity extracts the keeper path and replica name from one
// CREATE ... <ReplicatedEngine>('<zk-path>', '{replica}' | 'name') and
// the boot's settled hostname. Empty path answers "not a replicated
// CREATE" (the queue table, the MVs) — the caller's conflict-retry only
// applies to replicated tables. The '{replica}' macro form substitutes
// the hostname (the macros config renders per node); a literal replica
// name is taken as-is.
func ddlReplicaIdentity(stmt, hostname string) (path, replica string) {
	// The engine line: <Engine>('/clickhouse/tables/01/events', '{replica}')
	open := strings.Index(stmt, "('/clickhouse/tables/")
	if open < 0 {
		return "", ""
	}
	rest := stmt[open+1:]
	end := strings.Index(rest, "')")
	if end < 0 {
		return "", ""
	}
	parts := strings.SplitN(rest[:end], ",", 2)
	if len(parts) != 2 {
		return "", ""
	}
	path = strings.Trim(strings.TrimSpace(parts[0]), "'\"")
	replica = strings.Trim(strings.TrimSpace(parts[1]), "'\"")
	if replica == "{replica}" {
		replica = hostname
	}
	return path, replica
}

// splitSQLStatements splits a DDL file into statements on top-level
// semicolons: line comments (-- to end-of-line) are stripped first,
// and semicolons inside parentheses never terminate a statement.
//
// Why a dedicated function (not strings.Split): the bootstrap bundle's
// DDL carries explanatory comments and CODEC/interval defaults whose
// parentheses would otherwise shred statements into server-side
// syntax errors (Code 62) — the exact class the naive split produced
// live. The depth counter treats parentheses as the only nesting
// signal: single quotes are not tracked because the bundle carries no
// string literals containing semicolons or parens; a future bundle
// needing them should extend this with a quote state.
func splitSQLStatements(sql string) []string {
	var statements []string
	var current strings.Builder
	depth := 0
	for _, line := range strings.Split(sql, "\n") {
		// Strip line comments outside parens: a "--" inside a string
		// literal would corrupt, but the bundle's comments live at line
		// starts and after whitespace only (the file's own convention).
		if idx := strings.Index(line, "--"); idx >= 0 && depth == 0 {
			line = line[:idx]
		}
		for _, r := range line {
			switch r {
			case '(':
				depth++
			case ')':
				if depth > 0 {
					depth--
				}
			case ';':
				if depth == 0 {
					statements = append(statements, current.String())
					current.Reset()
					continue
				}
			}
			current.WriteRune(r)
		}
		current.WriteRune('\n')
	}
	if strings.TrimSpace(current.String()) != "" {
		statements = append(statements, current.String())
	}
	return statements
}

// execClient runs one SQL statement as admin over the local client (the
// same credential shape the drain/disk-autoscale probes use — env, never
// argv).
func execClient(ctx context.Context, adminPw, query string) (string, error) {
	if execClientFn != nil {
		return execClientFn(ctx, adminPw, query)
	}
	cmd := exec.CommandContext(ctx, "clickhouse-client", "--user", "admin", "--password", adminPw, "--query", query)
	cmd.Env = append(os.Environ(), "CLICKHOUSE_USER=admin", "CLICKHOUSE_PASSWORD="+adminPw)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("clickhouse-client: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// execClientFn is the exec seam (tests substitute; production runs the
// local clickhouse-client).
var execClientFn func(ctx context.Context, adminPw, query string) (string, error)

// healthGate runs the cluster health check as a library call — the
// operator's health service over the chctl probe (the same code path
// `chctl health` exercised; the binary is gone, the library remains).// Non-Fatal BY DESIGN (the step's Criticality is BestEffort): a reader
// booting while a peer is down must still join; the WARNING + report
// make a degraded boot loud in the logs.
//
// Self-healing: a degraded verdict now triggers one schema-sync retry
// pass with a backoff (bounded attempts) before the final report.
// The 2026-11-14 zombie-reader: a reader scaled out during a keeper
// restart, every peer query timed out, the boot recorded "degraded"
// and the node served NLB reads with an EMPTY database for hours —
// degraded is a transient condition at boot (peers restarting), not
// a terminal one; the retry converts it back to healthy when the
// transient clears, and the final report still tells the truth when
// it doesn't.
func (b *Boot) healthGate(ctx context.Context) error {
	if b.Spec.Role == model.RoleKeeper {
		return nil
	}
	report, err := b.runHealthCheck(ctx)
	if report != nil {
		fmt.Fprintf(os.Stderr, "cluster health verdict:\n%s\n", report.FormatHuman())
	}
	// Refresh restarts the server under a live cluster: a transient
	// degradation is EXPECTED while the node rejoins, and the boot-path
	// retry loop costs up to ~5 minutes of wall clock inside a 300s SSM
	// command. One probe, one report, no gate — the hourly schema-sync
	// and the node's own replication are the recovery path, and the
	// refresher's roll ordering already paces the fleet.
	if b.Refresh {
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: post-refresh cluster health check degraded (node still serves; investigate via RUNBOOK_CLUSTER_HEALTH.md): %v\n", err)
		}
		return nil
	}
	if err != nil && b.Spec.Role != model.RoleBackup {
		return b.retryGate(ctx, err)
	}
	// Degraded reports come back with err == nil (Check's contract:
	// the report IS the finding, never an error) — the retry must key
	// on the report's status or a zombie reader (empty schema, peers
	// unreachable at first probe) sails through the gate on its FIRST
	// degraded verdict without ever running the schema-sync heal (the
	// live 0.2.4 finding: one verdict line, zero retries).
	if report != nil && !report.Healthy() && b.Spec.Role != model.RoleBackup && !b.Refresh {
		return b.retryGate(ctx, fmt.Errorf("health report degraded: %s", report.Status))
	}
	if err != nil {
		return fmt.Errorf("post-boot cluster health check reported degradation (boot continues; investigate via RUNBOOK_CLUSTER_HEALTH.md): %w", err)
	}
	return nil
}

// retryGate runs the health gate's bounded schema-sync + re-probe loop
// (shared by the error path and the degraded-report path).
func (b *Boot) retryGate(ctx context.Context, firstErr error) error {
	err := firstErr
	for attempt := 1; attempt <= healthRetryAttempts && ctx.Err() == nil; attempt++ {
		fmt.Fprintf(os.Stderr, "health gate degraded (attempt %d/%d): retrying schema-sync before the final verdict\n",
			attempt, healthRetryAttempts)
		if serr := b.gateSchemaSync(ctx); serr != nil {
			fmt.Fprintf(os.Stderr, "WARNING: health-gate schema-sync retry failed: %v\n", serr)
		}
		select {
		case <-time.After(healthRetryBackoff):
		case <-ctx.Done():
		}
		report, rerr := b.runHealthCheck(ctx)
		if report != nil {
			fmt.Fprintf(os.Stderr, "cluster health verdict (retry %d):\n%s\n", attempt, report.FormatHuman())
		}
		// Success is a HEALTHY report, not a nil error: the degraded
		// path must keep healing until the verdict clears or the
		// attempts run out.
		if rerr == nil && report != nil && report.Healthy() {
			return nil
		}
		if rerr != nil {
			err = rerr
		}
	}
	return fmt.Errorf("post-boot cluster health check still degraded after %d schema-sync retries (boot continues; investigate via RUNBOOK_CLUSTER_HEALTH.md): %w",
		healthRetryAttempts, err)
}

// healthRetryAttempts/backoff bound the health-gate self-healing loop:
// enough to ride out a fleet roll or keeper restart (~4 minutes of
// retries), short enough to never wedge an ASG boot.
const (
	healthRetryAttempts = 3
	healthRetryBackoff  = 90 * time.Second
)

// runHealthCheck is the health gate's single probe pass.
func (b *Boot) runHealthCheck(ctx context.Context) (*HealthReport, error) {
	// The admin password file is the credentials source (the boot
	// resolved it in retrieve_passwords); the typed request carries
	// the value, never an argv.
	pw, err := os.ReadFile(BootstrapDir + "/admin-password")
	if err != nil {
		return nil, fmt.Errorf("read admin password file: %w", err)
	}
	// The health check requires the inventory (the same CR-name →
	// address map the render uses) and refuses to run without it.
	hosts, note := b.clusterHostsWithSource()
	if len(hosts) == 0 {
		return nil, fmt.Errorf("post-boot cluster health check skipped: host inventory is empty")
	}
	if note != "" {
		slog.Warn(note)
		fmt.Fprintln(os.Stderr, note)
	}
	// The keeper check takes ONE mntr address (the ensemble's quorum
	// math lives in the keeper health cron, not here). Pinning the
	// ensemble's entrypoint exercises the reachability+mntr path;
	// per-member liveness is the deadline for the dedicated gate.
	keeperAddress := ""
	if len(b.Spec.KeeperNodes) > 0 {
		keeperAddress = fmt.Sprintf("%s.%s:9181", b.Spec.KeeperNodes[0], b.Spec.DNSZoneName)
	}
	cr, err := b.LoadCHI(ctx)
	if err != nil {
		return nil, fmt.Errorf("post-boot health check could not load the spec: %w", err)
	}
	tables := operator.ExpectedTablesFromCR(cr)
	svc := NewHealthService(operator.NewHealthProbe(), keeper.NewProbe(), func() []domain.TableRef { return tables })
	return svc.Check(ctx, HealthRequest{
		ClusterName:   b.Spec.ClusterName,
		Hosts:         hosts,
		Credentials:   domain.Credentials{Username: "admin", Password: strings.TrimSpace(string(pw))},
		Timeout:       DefaultHealthTimeout,
		KeeperAddress: keeperAddress,
		CheckTables:   true,
		SelfName:      b.Identity.Hostname,
	})
}

// gateSchemaSync re-runs the schema propagation in-process (the same
// service the schema-sync unit/timer drives — never a shell-out, so
// the gate's retry inherits the library's own retry semantics).
func (b *Boot) gateSchemaSync(ctx context.Context) error {
	if b.Spec.Role == model.RoleKeeper || b.Spec.Role == model.RoleBackup {
		return nil
	}
	pw, err := os.ReadFile(BootstrapDir + "/admin-password")
	if err != nil {
		return fmt.Errorf("read admin password file: %w", err)
	}
	hosts, _ := b.clusterHostsWithSource()
	cr, err := b.LoadCHI(ctx)
	if err != nil {
		return fmt.Errorf("schema-sync retry could not load the spec: %w", err)
	}
	svc := NewSchemaService(operator.NewSchemaClientFactory())
	// The retry is the boot's own heal: a reader that booted mid-churn
	// holds no replica registrations worth pruning, but a REPLACEMENT
	// must drop its predecessor's stale row before the create or the
	// sync re-attaches the dead registration (Code 253).
	return svc.SyncSchema(ctx, SchemaRequest{
		ClusterName:  b.Spec.ClusterName,
		Credentials:  domain.Credentials{Username: "admin", Password: strings.TrimSpace(string(pw))},
		NormalizedCR: cr,
		Hosts:        hosts,
	}, domain.SchemaSyncRequest{
		HostName:       b.Identity.Hostname,
		Retries:        DefaultSchemaSyncRetries,
		PruneStaleSelf: true,
	})
}

// prometheusReload reloads the config so the metrics endpoint comes up
// without a restart, and verifies the endpoint answers.
func (b *Boot) prometheusReload(ctx context.Context) error {
	if b.Spec.Role == model.RoleKeeper {
		return nil
	}
	if _, err := os.Stat(ConfigDir + "/server-prometheus.xml"); err != nil {
		return fmt.Errorf("server-prometheus.xml not found — config should have been rendered by chctl (spec files)")
	}
	if _, err := runCommand(ctx, "clickhouse-client", "--query", "SYSTEM RELOAD CONFIG"); err != nil {
		// The reload is a convenience; a restart covers it.
		_, _ = runCommand(ctx, "systemctl", "restart", "clickhouse-server")
	}
	return waitForHTTP(ctx, "http://localhost:9363/metrics", 10, 2)
}

// runCommand is the local exec helper for service management (the
// injectable Runner lives in the system package for host ops; these
// step-level calls go through the same recording-friendly surface in
// tests via the Boot's runner injection point).
func runCommand(ctx context.Context, name string, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// waitForHTTP polls a URL until it answers or attempts run out (the
// wait_for_service shape, bounded).
func waitForHTTP(ctx context.Context, url string, attempts int, delaySec int) error {
	for i := 0; i < attempts; i++ {
		out, err := runCommand(ctx, "curl", "-s", "-o", "/dev/null", url)
		_ = out
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(delaySec) * time.Second):
		}
	}
	return fmt.Errorf("%s did not become ready after %d attempts", url, attempts)
}

// providerSecrets adapts the chinit provider (a port.SecretSource,
// GetSecretValue) to chctl's users-renderer SecretsResolver
// (ResolveSecret). Without this adapter the renderer receives a nil
// resolver and PANICS the first time the spec declares a passwordFrom
// user (nil dereference in adaptUser) — the renderer resolves
// password references at render time and cannot skip them.
type providerSecrets struct {
	p port.Provider
}

// ResolveSecret fetches one secret value by name via the cloud
// provider's secret source (instance role on AWS, Key Vault on Azure).
// chinit's Provider interface embeds chctl's port.SecretSource, so the
// request shape is shared.
func (a providerSecrets) ResolveSecret(ctx context.Context, request port.SecretRequest) (string, error) {
	return a.p.GetSecretValue(ctx, request)
}
