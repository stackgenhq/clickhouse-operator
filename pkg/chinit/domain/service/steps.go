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
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chinit/infra/system"
)

// Retry policies: named once, referenced by step (the bash script's 16
// ad-hoc loops become four deliberate shapes).
var (
	// apiRetry: transient API failures (throttling, DNS, IMDS lag).
	apiRetry = Policy{Attempts: 5, BaseDelay: 2 * time.Second, MaxDelay: 10 * time.Second}
	// bootWait: waiting on the system (services, disks) — patient.
	bootWait = Policy{Attempts: 30, BaseDelay: 2 * time.Second, MaxDelay: 10 * time.Second}
	// onceFatal: config-class operations where a retry masks a bug
	// (SHA mismatch, malformed XML): one attempt, no retry.
	onceFatal = Policy{Attempts: 1}
	// claimSweep: bounded by one lease window (16 min > 15-min TTL).
	claimSweep = Policy{Attempts: 3, BaseDelay: 5 * time.Second, MaxDelay: 60 * time.Second, Deadline: 16 * time.Minute}
)

// Steps builds the ordered boot pipeline. The order is load-bearing and
// documented inline where a finding pinned it; the engine adds resume,
// retry classification and the fatal/best-effort split.
func (b *Boot) Steps(ctx context.Context) ([]Step, error) {
	runner := system.ExecRunner{}
	disks := system.NewDiskManager(runner).WithPlatform(b.Provider.Name())
	tuner := system.NewOSTuner(runner)
	configs := system.NewStaticConfigInstaller(runner).WithRole(b.Spec.Role)

	steps := []Step{
		// PHASE 1 (parallel): the boot's wall-clock poles are IO-bound
		// and INDEPENDENT — the claim sweep can legally wait out a
		// predecessor's lease for many minutes (claimSweep's 16-min
		// deadline) while the bundle downloads, EBS attaches and the
		// kernel settings apply. Running them together pays the MAX
		// of the poles instead of the SUM; each child keeps its own
		// retry policy and criticality (a Critical failure cancels
		// the siblings — see ParallelGroup).
		ParallelGroup{Children: []Step{
			// 1. Fetch the config bundle.
			// SHA mismatch inside is Fatal — no retry.
			StepFunc("fetch_config_bundle", Critical, apiRetry, func(ctx context.Context) error {
				_, err := b.Provider.FetchBundle(ctx, b.Spec.ConfigBundle, BootstrapDir)
				return err
			}),

			// 2-3. OS tuning + binary presence. THP/limits are convergent;
			// a missing AMI binary is Fatal (the ASG must replace the node).
			StepFunc("os_optimizations", Critical, onceFatal, func(ctx context.Context) error {
				return tuner.Tune(ctx)
			}),
			StepFunc("require_binaries", Critical, onceFatal, func(ctx context.Context) error {
				// clickhouse binaries: every cloud. The render and the
				// recurring-job subcommands are in-process library calls —
				// no chctl binary. The `aws` CLI is AWS-only — an Azure
				// node never has it, and requiring it there would fail every
				// boot fatally.
				bins := []string{"clickhouse-server", "clickhouse-client"}
				if b.Provider.Name() == "aws" {
					bins = append(bins, "aws")
				}
				return tuner.RequireBinaries(ctx, bins)
			}),

			// 4. Mount data disks (metadata Critical; cache/keeper degrade
			// inside). EBS volumes attach asynchronously — bootWait covers
			// the attach window.
			StepFunc("mount_data_disks", Critical, bootWait, func(ctx context.Context) error {
				_, err := disks.MountAll(ctx, *b.Spec, nil)
				return err
			}),

			// 6. Identity settles BEFORE any render (the ip-* macro bug:
			// ASG members must claim their ordinal before macros render). The
			// claimer is an optional capability — static identity (keepers,
			// backup — and the interim static-writer path) needs no claim.
			// Concurrent-safety: the claim touches only the provider (the
			// lease table + DNS), sharing no files with the phase siblings.
			StepFunc("settle_identity", Critical, claimSweep, b.settleIdentity),

			// 7. Passwords land BEFORE the render (passwordFrom refs and
			// the distributed-query password files the render consumes).
			// Writes 0600 files under BootstrapDir — distinct names from
			// the bundle's files, so the concurrent fetch races nothing.
			StepFunc("retrieve_passwords", Critical, apiRetry, b.retrievePasswords),

			// 7b. TLS material: server cert/key/CA from the cloud secret
			// store, or self-signed interserver-only when no TLS secrets
			// are configured (acm mode: the NLB terminates client TLS, the
			// interserver port still requires the files to EXIST). Writes
			// only /etc/clickhouse-server/ssl — no phase sibling touches it.
			StepFunc("configure_tls", Critical, apiRetry, b.configureTLS),
		}},

		// PHASE 2 (serial): everything below reads what phase 1 wrote —
		// the render needs the identity, the passwords and the bundle;
		// the server start needs the render; the units need the server.

		// 5. Static configs: validate + install + retire overlays
		// (reads the fetched bundle).
		StepFunc("install_static_configs", Critical, onceFatal, func(ctx context.Context) error {
			return configs.Install(ctx, BootstrapDir)
		}),

		// 5b. Peer /etc/hosts entries: short names must resolve
		// locally (systemd-resolved's stub breaks short-name DNS and
		// the replication queue addresses the leader by its SHORT
		// name — Cannot resolve host writer-0 stalls every part
		// fetch). Needs the settled identity (the lease table scan).
		StepFunc("configure_peer_hosts", Critical, onceFatal, b.configurePeerHosts),

		// 7c. Server environment: /etc/default/clickhouse-server + the
		// systemd EnvironmentFile drop-in. storage.xml resolves the S3
		// endpoint via from_env — the variable must be set before the
		// server starts.
		StepFunc("configure_server_env", Critical, onceFatal, b.configureServerEnv),

		// 8. Render the node config — chctl's library, not its CLI.
		StepFunc("render_node_config", Critical, onceFatal, b.renderNodeConfig),

		// 9. Keeper config (keeper nodes only) + state cleanup.
		// CRITICAL for keepers (the step no-ops for every other role):
		// keeper launches with an explicit --config, so without this
		// render it runs whatever stale config the image left behind —
		// a misconfigured RAFT member can disrupt the ensemble quorum
		// itself. A BestEffort classification made a keeper-node
		// boot "succeed" while the cluster lost its coordination.
		StepFunc("configure_keeper", Critical, onceFatal, b.configureKeeper),

		// 10. The identity contract file for legacy unit consumers.
		StepFunc("write_node_env", BestEffort, onceFatal, func(ctx context.Context) error {
			if b.Spec.Role == model.RoleKeeper {
				return nil // keepers wrote theirs in configure_keeper
			}
			return b.WriteNodeEnv(NodeEnvPath)
		}),

		// 11. Start clickhouse-server (CRITICAL; keepers skip inside).
		StepFunc("start_clickhouse", Critical, onceFatal, b.startClickhouse),

		// 12. Health gate: NON-FATAL by design — a reader booting while
		// a peer is down (fleet rolling, spot churn) must still join; a
		// fatal gate wedges the ASG in a boot loop.
		StepFunc("health_gate", BestEffort, onceFatal, b.healthGate),

		// 13. Prometheus endpoint reload + verification.
		StepFunc("prometheus_reload", BestEffort, bootWait, b.prometheusReload),

		// 14. Recurring-job units (chinit subcommands as ExecStart).
		// CRITICAL for prod: the units carry the node's identity
		// lifecycle — lease-renew is what keeps the ordinal claim
		// alive (a node that boots WITHOUT it loses its lease 15
		// minutes later and another instance claims the same
		// ordinal: two nodes serving one identity is the worst
		// failure this pipeline can produce), schema-sync is what
		// materializes this node's tables, disk-autoscale is the
		// merge-stall early-warning. A boot that "succeeds" without
		// them is a liability, not a node — the ASG replacement a
		// Critical failure triggers is the remedy.
		StepFunc("install_units", Critical, onceFatal, b.installUnits),

		// 15. Backup (clickhouse-backup config + timer). CRITICAL when
		// the operator EXPLICITLY pinned a backup bucket: a silent
		// no-backup node is a data-loss risk discovered at restore
		// time. The step no-ops cleanly when backups are not
		// configured (bucket empty) or the binary is absent (a
		// deliberate, warned, non-cluster choice).
		StepFunc("configure_backup", Critical, onceFatal, b.configureBackup),

		// 16. Monitoring agents (OTEL/Fluent Bit remote shipping;
		// CloudWatch agent on AWS — chinit owns it now, the image only
		// installs the deb).
		StepFunc("configure_monitoring", BestEffort, onceFatal, b.configureMonitoring),
		StepFunc("configure_cloudwatch", BestEffort, onceFatal, b.configureCloudWatch),

		// 17. Spot watcher service (readers). BestEffort stays: the
		// spot notice is an optimization over the shutdown drain +
		// the lease TTL (a missed notice costs a 15-minute ghost
		// window, not identity theft), and spot-capability is a
		// cloud property the boot must not die on.
		StepFunc("install_spot_watch", BestEffort, onceFatal, b.installSpotWatch),

		// 18. In-node shutdown drain unit (writers + readers): the
		// systemd stop-path drain — closes the scale-in/health-check/
		// spot-deadline gaps the hook's SSM dispatch cannot reach.
		// CRITICAL for prod: a node without it terminates cold and
		// leaves exactly the stale-DNS/lease residue the 2026-11-15
		// soak found wedging the fleet's cluster fan-outs.
		StepFunc("install_shutdown_drain", Critical, onceFatal, b.installShutdownDrain),

		// 19. boot-finished marker (health verification greps it).
		StepFunc("boot_finished", Critical, onceFatal, func(ctx context.Context) error {
			if err := os.MkdirAll(filepath.Dir(BootFinishedMarker), 0755); err != nil {
				return err
			}
			stamp := time.Now().UTC().Format("2006-01-02T15:04:05Z")
			return os.WriteFile(BootFinishedMarker, []byte(stamp), 0644)
		}),
	}
	return steps, nil
}

// crNameFromHostname derives the CR slot ("01-<n>") from a settled hostname:
// writers occupy slots 0..WriterMax-1, readers occupy WriterMax..WriterMax+ReaderMax-1.
// Returns ok=false for hostnames that carry no CR slot (keepers, backup).
func crNameFromHostname(spec *model.BootSpec, hostname string) (string, int, bool) {
	writerPrefix := spec.WriterHostPrefix
	if writerPrefix == "" {
		writerPrefix = "writer-"
	}
	readerPrefix := spec.ReaderHostPrefix
	if readerPrefix == "" {
		readerPrefix = "reader-"
	}
	switch {
	case strings.HasPrefix(hostname, writerPrefix):
		if n, err := strconv.Atoi(strings.TrimPrefix(hostname, writerPrefix)); err == nil && n >= 0 {
			return fmt.Sprintf("01-%d", n), n, true
		}
	case strings.HasPrefix(hostname, readerPrefix):
		offset := spec.WriterMax
		if offset == 0 {
			offset = spec.WriterCount
		}
		if n, err := strconv.Atoi(strings.TrimPrefix(hostname, readerPrefix)); err == nil && n >= 0 {
			return fmt.Sprintf("01-%d", offset+n), offset + n, true
		}
	}
	return "", 0, false
}

// restoreIdentity is the refresh path's identity settle: take the identity
// the boot persisted in the state file and reuse it. The identity is the
// cluster's name for this node — CR slot in remote_servers/macros, the DNS
// A record, the ReplicatedMergeTree replica path — so a refresh must
// reproduce it exactly or fail loudly. Re-claiming is NOT an option (the
// old flow silently renumbered the live reader from reader-0/01-2 to
// reader-1/01-3; the CR slot, DNS and replicated metadata disagree on who
// the node is).
//
// Validation by identity mode:
//   - lease: the lease row for the persisted hostname must still be held
//     by THIS instance (renew proves it without moving the ordinal). If it
//     lapsed and a different instance took it, this node has lost its
//     claim to the identity and must not serve under it — Fatal (the ASG
//     replaces the node).
//   - static: re-derive from the private IP exactly as a first boot would
//     and compare; a mismatch means the machine was re-provisioned with a
//     different offset and must not keep serving the persisted name.
//
// On success the lease is renewed (best-effort TTL top-up), the hostname is
// re-asserted, and the state file identity is refreshed in place (the DNS
// zone id may have been learned since the last settle).
func (b *Boot) restoreIdentity(ctx context.Context) error {
	statePath := ChinitStatePath
	if b.StatePath != "" {
		statePath = b.StatePath
	}
	stored, err := loadBootIdentity(statePath)
	if err != nil {
		return Fatal(err)
	}
	if model.Role(stored.Role) != b.Spec.Role {
		return Fatal(fmt.Errorf("state role %q != spec role %q — refusing to refresh a node whose role changed; re-image instead", stored.Role, b.Spec.Role))
	}
	if stored.InstanceID != b.Meta.InstanceID {
		return Fatal(fmt.Errorf("state instance %s != this instance %s — refusing to refresh under a foreign identity (re-image the node instead)", stored.InstanceID, b.Meta.InstanceID))
	}

	id := model.NodeIdentity{
		Role:        model.Role(stored.Role),
		Hostname:    stored.Hostname,
		InstanceID:  stored.InstanceID,
		PrivateIP:   b.Meta.PrivateIP, // authoritative from metadata, not the (possibly stale) snapshot
		Region:      b.Meta.Region,
		ClusterName: b.Spec.ClusterName,
		DNSZone:     b.Spec.DNSZoneName,
		CRName:      stored.CRName,
		CROrdinal:   stored.CROrdinal,
		ZoneID:      stored.ZoneID,
		LeaseTable:  stored.LeaseTable,
	}

	switch b.Spec.IdentityMode {
	case model.IdentityStatic:
		hostname, ordinal := StaticHostname(b.Meta.PrivateIP)
		if hostname == "" {
			return Fatal(fmt.Errorf("private IP %s does not map to a static role offset (writers +10..19, keepers +20..29, backup +30)", b.Meta.PrivateIP))
		}
		if hostname != stored.Hostname {
			return Fatal(fmt.Errorf("state hostname %q != static derivation %q from IP %s — the node's placement changed; re-image instead of refreshing", stored.Hostname, hostname, b.Meta.PrivateIP))
		}
		id.CROrdinal = ordinal
	case model.IdentityLease:
		if id.LeaseTable == "" {
			return Fatal(fmt.Errorf("state carries no lease table for lease-mode node — refusing to guess; re-image instead"))
		}
		claimer := port.AsOrdinalClaimer(b.Provider)
		if claimer == nil {
			return Fatal(fmt.Errorf("cloud %s has no ordinal-lease capability but spec demands identity_mode: lease", b.Provider.Name()))
		}
		// Renew proves ownership without moving the ordinal: the renew
		// condition pins instance_id, so a renewal succeeds iff WE hold the
		// lease. The STOLEN-LEASE failure (the port's ErrLeaseStolen —
		// the provider's conditional-check rejection) is Fatal: do NOT
		// keep serving under a recycled name. A TRANSIENT failure
		// (throttling, network, DynamoDB blip) is a plain error the
		// claimSweep policy retries — only an exhausted sweep fails the
		// refresh. Classifying the transient class Fatal aborted healthy
		// in-place refreshes on one API blip (the same misclassification
		// the disk-attach race carried).
		if err := claimer.RenewLease(ctx, port.RenewRequest{
			Table:        id.LeaseTable,
			Hostname:     id.Hostname,
			InstanceID:   b.Meta.InstanceID,
			LeaseSeconds: 900,
		}); err != nil {
			if errors.Is(err, port.ErrLeaseStolen) {
				return Fatal(fmt.Errorf("lease renew for %s failed (%v) — another instance holds it now; do NOT keep serving under this name; re-image the node", id.Hostname, err))
			}
			return fmt.Errorf("lease renew for %s failed: %w", id.Hostname, err)
		}
		slog.Info("identity: persisted lease renewed", "hostname", id.Hostname, "lease_table", id.LeaseTable)
	default:
		return Fatal(fmt.Errorf("unknown identity_mode %q in spec", b.Spec.IdentityMode))
	}

	// CR name: derive it from the HOSTNAME's ordinal rather than from the
	// persisted CROrdinal, whose meaning differs by role (writers store
	// the bare ordinal; readers store the writer-offset slot — Boot.CRName
	// expects the bare ordinal in Identity.CROrdinal). The hostname is the
	// canonical claim; everything else derives from it.
	if crName, ord, ok := crNameFromHostname(b.Spec, id.Hostname); ok {
		if stored.CRName != "" && crName != stored.CRName {
			return Fatal(fmt.Errorf("CR name derived from hostname %q is %q but state says %q — spec topology changed under this node; re-image instead of refreshing", id.Hostname, crName, stored.CRName))
		}
		id.CRName = crName
		id.CROrdinal = ord
	} else if stored.CRName != "" {
		// Keepers/backup: no CR slot; carry the persisted values through.
		id.CRName = stored.CRName
		id.CROrdinal = stored.CROrdinal
	}

	b.Identity = id
	if b.PersistIdentity != nil {
		b.PersistIdentity(id)
	}
	if err := b.setHostname(ctx, id.Hostname); err != nil {
		slog.Warn("WARNING: could not set system hostname", "hostname", id.Hostname, "err", err)
	}

	// Re-assert DNS (same record the first boot wrote): best-effort, same
	// contract as the boot path.
	if dns := port.AsDNSRegistrar(b.Provider); dns != nil {
		zoneID := id.ZoneID
		if zoneID == "" {
			zoneID, _ = dns.ResolveZoneID(ctx, b.Spec.DNSZoneName)
		}
		if zoneID != "" {
			id.ZoneID = zoneID
			b.Identity = id
			if err := dns.UpsertARecord(ctx, port.ARecordRequest{
				ZoneID:     zoneID,
				ZoneName:   b.Spec.DNSZoneName,
				RecordName: id.Hostname,
				IP:         id.PrivateIP,
				TTL:        60,
			}); err != nil {
				slog.Warn("WARNING: DNS re-registration failed; boot continues", "hostname", id.Hostname, "err", err)
			} else if b.PersistIdentity != nil {
				b.PersistIdentity(id)
			}
		}
	}
	return nil
}

// settleIdentity derives or claims the node identity and registers DNS.
// Static nodes (keepers/backup — and writers while the static path still
// exists) map the IP octet; lease nodes (BOTH ASG roles — readers and,
// since the writer-ASG rollout, writers) claim via the provider's
// OrdinalClaimer. The capability is probed once, and its absence under
// lease identity is Fatal (an ASG member without a claimer would boot
// with the ephemeral ip-* name and break replication identity — the
// exact production finding).
//
// The claim is role-aware: the claim sweeps ordinals within the ROLE'S
// ASG max (Prefix/Count come from the spec's role fields — writer-
// /WriterMax for writers, reader-/ReaderMax for readers), so a
// scaled-out node claims the first free slot in [0, max) and never
// exceeds the CR slot ceiling the CHI spec reserved.
func (b *Boot) settleIdentity(ctx context.Context) error {
	// Refresh mode: an in-place refresh of a converged node must keep the
	// identity the cluster already knows — re-claiming here would renumber
	// the node out from under ReplicatedMergeTree's replica paths and the
	// DNS A records. The persisted identity is re-validated against the
	// lease table when there is one (a lease that lapsed AND was taken by
	// another instance means we must NOT keep serving under the old name —
	// fail loudly; the ASG replaces this node), then reused verbatim.
	if b.Refresh {
		return b.restoreIdentity(ctx)
	}

	id := model.NodeIdentity{
		Role:        b.Spec.Role,
		InstanceID:  b.Meta.InstanceID,
		PrivateIP:   b.Meta.PrivateIP,
		Region:      b.Meta.Region,
		ClusterName: b.Spec.ClusterName,
		DNSZone:     b.Spec.DNSZoneName,
		CROrdinal:   -1,
	}

	switch b.Spec.IdentityMode {
	case model.IdentityStatic:
		hostname, ordinal := StaticHostname(b.Meta.PrivateIP)
		if hostname == "" {
			return Fatal(fmt.Errorf("private IP %s does not map to a static role offset (writers +10..19, keepers +20..29, backup +30)", b.Meta.PrivateIP))
		}
		id.Hostname = hostname
		id.CROrdinal = ordinal
		// Keepers' CROrdinal is their own ordinal; server nodes get the
		// CR name from CRName() at render time.
	case model.IdentityLease:
		claimer := port.AsOrdinalClaimer(b.Provider)
		if claimer == nil {
			return Fatal(fmt.Errorf("cloud %s has no ordinal-lease capability but spec demands identity_mode: lease", b.Provider.Name()))
		}
		// Role-aware claim parameters: prefix defaults keep pre-writer-ASG
		// spec templates working (the model documents the defaults), and
		// the ceiling is the role's ASG max — the validation contract
		// already requires it for lease identity.
		var prefix string
		var ceiling int
		switch b.Spec.Role {
		case model.RoleWriter:
			prefix = b.Spec.WriterHostPrefix
			if prefix == "" {
				prefix = "writer-"
			}
			ceiling = b.Spec.WriterMax
		case model.RoleReader:
			prefix = b.Spec.ReaderHostPrefix
			if prefix == "" {
				prefix = "reader-"
			}
			ceiling = b.Spec.ReaderMax
		default:
			// Lease identity is an ASG-member property: keepers and the
			// backup replica are static EC2s (raft safety — they are not
			// CR members and must never claim a CR slot).
			return Fatal(fmt.Errorf("identity_mode: lease is only valid for writer/reader roles (got %s)", b.Spec.Role))
		}
		leaseTable := b.Spec.ClaimerTableName
		if leaseTable == "" {
			leaseTable = os.Getenv("CHINIT_LEASE_TABLE")
		}
		// The empty-table blind spot: a lease identity with no table
		// name claims against "" — DynamoDB answers a confusing
		// validation error from inside the ASG's boot loop and the
		// operator has nothing pointing at the rendered spec being
		// incomplete. Fatal, named, immediately.
		if leaseTable == "" {
			return Fatal(fmt.Errorf("lease identity requires claimer_table_name in the spec (or CHINIT_LEASE_TABLE env) — the rendered chinit spec is missing the field"))
		}
		slog.Info("identity: claiming ordinal lease",
			"hostname_prefix", prefix,
			"claim_ceiling", ceiling,
			"lease_table", leaseTable)
		ordinal, err := claimer.ClaimOrdinal(ctx, port.ClaimRequest{
			Table:           leaseTable,
			Prefix:          prefix,
			Count:           ceiling,
			InstanceID:      b.Meta.InstanceID,
			PrivateIP:       b.Meta.PrivateIP,
			LeaseSeconds:    900,
			DeadlineSeconds: 960,
		})
		if err != nil {
			return err
		}
		id.Hostname = fmt.Sprintf("%s%d", prefix, ordinal)
		id.CROrdinal = ordinal
		id.LeaseTable = leaseTable
		slog.Info("identity: lease claimed",
			"hostname", id.Hostname,
			"ordinal", ordinal,
			"lease_table", leaseTable)
	}

	// The CR host name derives from the settled role + ordinal (writers
	// 01-<ordinal>, readers 01-<writer ceiling + ordinal>; backup and
	// keepers carry none).
	b.Identity = id
	if crName, ord := b.CRName(); crName != "" {
		id.CRName = crName
		id.CROrdinal = ord
	}

	// Set the hostname (static and transient) so macros/replication see
	// the settled name. Best-effort per the bash script (hostnamectl
	// can fail inside cloud-init contexts) but a missing identity is
	// Fatal — this is the cluster's view of who we are.
	b.Identity = id
	if b.PersistIdentity != nil {
		// Persist the settled identity IMMEDIATELY (before renders and
		// the server start): a crash later must not lose it, and the
		// job subcommands read it from the state file.
		b.PersistIdentity(id)
	}
	if err := b.setHostname(ctx, id.Hostname); err != nil {
		// Best-effort (hostnamectl can fail inside cloud-init); the
		// identity itself is settled and persisted.
		slog.Warn("WARNING: could not set system hostname",
			"hostname", id.Hostname,
			"err", err)
	}

	// DNS registration: optional capability, best-effort with a loud
	// warning (the bash script's `|| echo WARNING`).
	if dns := port.AsDNSRegistrar(b.Provider); dns != nil {
		zoneID, err := dns.ResolveZoneID(ctx, b.Spec.DNSZoneName)
		if err == nil && zoneID != "" {
			id.ZoneID = zoneID
			b.Identity = id
			if err := dns.UpsertARecord(ctx, port.ARecordRequest{
				ZoneID:     zoneID,
				ZoneName:   b.Spec.DNSZoneName,
				RecordName: id.Hostname,
				IP:         id.PrivateIP,
				TTL:        60,
			}); err != nil {
				// Best-effort: registration failure must not fail the
				// boot; the peer-hosts entries carry discovery.
				slog.Warn("WARNING: DNS registration failed; boot continues",
					"hostname", id.Hostname,
					"err", err)
			} else {
				slog.Info("identity: DNS A record registered",
					"hostname", id.Hostname,
					"zone_id", zoneID)
			}
		} else if err != nil {
			slog.Warn("WARNING: zone resolution failed; DNS registration skipped",
				"dns_zone", b.Spec.DNSZoneName,
				"err", err)
		} else {
			slog.Warn("WARNING: zone resolution returned an empty zone id; DNS registration skipped",
				"dns_zone", b.Spec.DNSZoneName)
		}
	}
	// Persist identity a second time when the DNS block enriched it: the
	// drain path releases the A record through the state's ZoneID, and
	// the first persist (pre-DNS) carried an empty zone id.
	if id.ZoneID != "" && b.PersistIdentity != nil {
		b.PersistIdentity(id)
	}
	return nil
}

// retrievePasswords resolves the boot's secrets into 0600 files the
// render and the post-boot consumers read. Values never enter the
// state file or logs (the file write is the only landing zone).
func (b *Boot) retrievePasswords(ctx context.Context) error {
	secretFiles := map[string]string{
		"default": b.Spec.Secrets.DefaultPassword,
		"admin":   b.Spec.Secrets.AdminPassword,
		"writer":  b.Spec.Secrets.WriterPassword,
	}
	if err := os.MkdirAll(BootstrapDir, 0700); err != nil {
		return Fatal(fmt.Errorf("create %s: %w", BootstrapDir, err))
	}
	for role, secretName := range secretFiles {
		if secretName == "" {
			continue
		}
		value, err := b.Provider.GetSecretValue(ctx, port.SecretRequest{Name: secretName})
		if err != nil {
			return err
		}
		dest := filepath.Join(BootstrapDir, role+"-password")
		if err := os.WriteFile(dest, []byte(value), 0600); err != nil {
			return Fatal(fmt.Errorf("write %s: %w", dest, err))
		}
	}
	return nil
}

// configureTLS writes the interserver TLS material the server's
// interserver port (9010) requires: server cert/key + CA. With TLS
// secrets configured, the values come from the cloud secret store;
// without them (acm mode — the NLB terminates client TLS, the
// interserver port is internal) a self-signed pair is generated — the
// bash-era generate_interserver_selfsigned_cert shape. The server
// FAILS TO START when the ssl.xml paths point at files that do not
// exist (Code 210 File not found — the live chinit-cutover finding:
// the bash script's configure_tls step had no chinit equivalent).
func (b *Boot) configureTLS(ctx context.Context) error {
	sslDir := "/etc/clickhouse-server/ssl"
	if err := os.MkdirAll(sslDir, 0755); err != nil {
		return Fatal(fmt.Errorf("create %s: %w", sslDir, err))
	}

	if b.Spec.Secrets.TLSCertificate == "" {
		// No TLS secrets configured (acm mode): interserver-only
		// self-signed, preserved across reboots (idempotent).
		crt := filepath.Join(sslDir, "server.crt")
		if _, err := os.Stat(crt); err == nil {
			return nil
		}
		out, err := runCommand(ctx, "openssl", "req", "-x509", "-newkey", "rsa:2048",
			"-nodes", "-days", "3650",
			"-subj", "/CN="+b.Identity.Hostname,
			"-keyout", filepath.Join(sslDir, "server.key"),
			"-out", crt)
		if err != nil {
			return Fatal(fmt.Errorf("generate self-signed interserver cert: %w: %s", err, out))
		}
		if err := os.WriteFile(filepath.Join(sslDir, "ca.crt"), mustRead(crt), 0644); err != nil {
			return Fatal(fmt.Errorf("write ca.crt: %w", err))
		}
		if _, err := runCommand(ctx, "chown", "-R", "clickhouse:clickhouse", sslDir); err != nil {
			return Fatal(err)
		}
		fmt.Println("TLS: generated self-signed cert for interserver TLS (no TLS secrets configured)")
		return nil
	}

	material := []struct {
		secret string
		dest   string
		mode   os.FileMode
	}{
		{b.Spec.Secrets.TLSCertificate, filepath.Join(sslDir, "server.crt"), 0644},
		{b.Spec.Secrets.TLSPrivateKey, filepath.Join(sslDir, "server.key"), 0600},
		{b.Spec.Secrets.TLSCACertificate, filepath.Join(sslDir, "ca.crt"), 0644},
	}
	for _, m := range material {
		if m.secret == "" {
			continue
		}
		value, err := b.Provider.GetSecretValue(ctx, port.SecretRequest{Name: m.secret})
		if err != nil {
			return err
		}
		if err := os.WriteFile(m.dest, []byte(value), m.mode); err != nil {
			return Fatal(fmt.Errorf("write %s: %w", m.dest, err))
		}
	}
	if _, err := runCommand(ctx, "chown", "-R", "clickhouse:clickhouse", sslDir); err != nil {
		return Fatal(err)
	}
	fmt.Println("TLS: material installed from the cloud secret store")
	return nil
}

// mustRead reads a file, panicking on failure (used only on files this
// boot just wrote).
func mustRead(path string) []byte {
	data, err := os.ReadFile(path)
	if err != nil {
		panic("mustRead " + path + ": " + err.Error())
	}
	return data
}

// configureServerEnv writes the server's EnvironmentFile (the
// /etc/default/clickhouse-server shape the bash era wrote) and the
// systemd drop-in that loads it. storage.xml's <endpoint
// from_env="CH_S3_ENDPOINT"> is a START requirement: unset, the server
// dies with Code 36 Empty protocol in the URL (the live chinit-cutover
// finding — the bash configure_environment_variables step had no
// chinit equivalent). Credentials NEVER go here: the SDK chain reads
// the instance role.
func (b *Boot) configureServerEnv(ctx context.Context) error {
	if b.Spec.S3DataBucket == "" {
		return Fatal(fmt.Errorf("spec s3_data_bucket is empty — storage.xml's endpoint env cannot be set (Code 36 at server start)"))
	}
	region := b.Spec.Region
	if region == "" {
		region = b.Meta.Region
	}
	envFile := "/etc/default/clickhouse-server"
	var sb strings.Builder
	if b.Spec.Timezone != "" {
		sb.WriteString("CLICKHOUSE_TZ=" + b.Spec.Timezone + "\n")
	}
	sb.WriteString("CH_S3_ENDPOINT=\"https://" + b.Spec.S3DataBucket + ".s3." + region + ".amazonaws.com\"\n")
	if err := os.WriteFile(envFile, []byte(sb.String()), 0644); err != nil {
		return Fatal(fmt.Errorf("write %s: %w", envFile, err))
	}
	if _, err := runCommand(ctx, "chown", "clickhouse:clickhouse", envFile); err != nil {
		return Fatal(err)
	}

	dropIn := "/etc/systemd/system/clickhouse-server.service.d/env.conf"
	if err := os.MkdirAll(filepath.Dir(dropIn), 0755); err != nil {
		return Fatal(err)
	}
	if err := os.WriteFile(dropIn, []byte("[Service]\nEnvironmentFile=-/etc/default/clickhouse-server\n"), 0644); err != nil {
		return Fatal(fmt.Errorf("write %s: %w", dropIn, err))
	}
	if _, err := runCommand(ctx, "systemctl", "daemon-reload"); err != nil {
		return Fatal(err)
	}
	return nil
}

// setHostname sets the node's hostname through the Boot's injectable
// seam: the injected recorder (tests) or the hostnamectl/hostname
// sequence. Kept separate from setSystemHostname so specs can assert
// the settled name never escapes the pipeline.
func (b *Boot) setHostname(ctx context.Context, hostname string) error {
	if b.SetHostname != nil {
		return b.SetHostname(ctx, hostname)
	}
	return setSystemHostname(ctx, hostname)
}

// setSystemHostname sets the node's static + transient hostname so
// ClickHouse macros and Keeper replica paths see the settled cluster
// identity instead of the ephemeral cloud-assigned ip-* name (the
// "keeper exits 56" and "ip-10-0-1-101 never joined" findings). Both
// modes: --transient alone does not survive a reboot, --static alone
// can fail inside early cloud-init contexts — try both, warn, never
// fail the boot.
func setSystemHostname(ctx context.Context, hostname string) error {
	if _, err := runCommand(ctx, "hostnamectl", "set-hostname", hostname, "--static"); err != nil {
		if _, fallbackErr := runCommand(ctx, "hostname", hostname); fallbackErr != nil {
			return err
		}
	}
	_, _ = runCommand(ctx, "hostnamectl", "set-hostname", hostname, "--transient")
	return nil
}

// configurePeerHosts writes /etc/hosts entries for the cluster's
// SHORT names: the replication queue addresses replica sources by
// their short registered name (the {replica} macro — identity), and
// systemd-resolved's stub resolver does not answer short names —
// "Cannot resolve host writer-0" stalls every part fetch (the bash
// era's configure_peer_hosts step, restored for the chinit cutover
// finding; the live reader finding extended it from writers-only to
// every server node: the reader's fetch queue addressed the writer
// by short name and died on DNS_ERROR while interserver-by-IP
// worked).
//
// Leased-fleet era: peer addresses come from the LIVE claims (the
// same inventory the render uses) — the old code fabricated the
// static EC2 offsets (10.0.1.10+i), which on the writers ASG pinned
// DEAD IPs: the replacement writer inherited the /etc/hosts line of
// its ordinal and every self/peer connection timed out against a
// vacant address (the live writers-ASG cutover finding — the hosts
// entry beats DNS). Stale entries from prior boots are removed first
// (the old contains-check kept a wrong entry forever). When a slot
// has no live claim it gets no hosts line — DNS (self-registered at
// claim) is the source of truth, and an absent peer is unreachable
// either way.
//
// The boot step is the FIRST pass, not the only one: the periodic
// convergence (the lease-renew timer's service.ConvergePeerHosts)
// owns the ongoing correctness, because a node booting mid-churn
// sees zero live writer claims and legitimately writes an empty
// hosts file here (the 2026-11-15 finding: 0s step, hours of
// DNS_ERROR until a human patched hosts by hand).
func (b *Boot) configurePeerHosts(ctx context.Context) error {
	// Keepers register themselves in Keeper's own host records; the
	// short-name problem is a server-node concern.
	if b.Spec.Role == model.RoleKeeper {
		return nil
	}
	return ConvergePeerHosts(ctx, b.Provider, ConvergeHostsArgs{
		LeaseTable:   b.Identity.LeaseTable,
		WriterPrefix: b.Spec.WriterPrefix(),
		ReaderPrefix: b.Spec.ReaderPrefix(),
		DNSZoneName:  b.Spec.DNSZoneName,
	})
}
