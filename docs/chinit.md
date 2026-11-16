# chinit — cloud-adaptive node bootstrapper

> Contributor-facing internals — layers, pipeline, identity contract,
> the conformance suite — live in
> [chinit-architecture.md](chinit-architecture.md). This page is the
> operator's manual.

`chinit` is a single Go binary that bootstraps a ClickHouse node on **any**
cloud: it detects the platform it runs on (AWS, Azure; more via the provider
factory), fetches its configuration bundle, converges the node to a
declarative spec and installs its own recurring maintenance jobs as
subcommands — the entire lifecycle, first boot to termination, is one
binary plus one small YAML spec.

## Why it exists

Bootstrapping a stateful, replicated database fleet on auto-scaling cloud
infrastructure is a problem full of races and failure modes. A node is not
a pet: instances launch, scale in, get replaced by health checks and spot
reclaims — and every one of those events must leave the cluster's identity
state consistent. The problems chinit is built to solve:

| The problem | How chinit solves it |
|---|---|
| Node configuration has to differ per instance (identity, IPs, role) while staying identical in logic — templating config into per-node variants drifts and is untestable | `chinit.yaml` — a typed, validated spec fetched (SHA256-gated) from object storage; the binary is identical across roles and clouds, only the spec is per-role |
| Cloud APIs are flaky and every failure needs a deliberate answer (retry? abort? degrade loudly?) — ad-hoc retries either mask bugs or wedge boots | A retry engine: Retryable/Fatal error classification, exponential backoff with jitter, per-step deadlines, Critical/BestEffort criticality |
| A boot that crashes halfway and re-runs from scratch is slow and unsafe (re-claiming identity mid-flight can renumber a live cluster member) | Convergence: every step probes real system state first; `state.json` records done steps and resumes; a new bundle hash invalidates and re-renders |
| Post-boot maintenance (schema bootstrap, schema sync, replica sync, lease renewal, keeper health, GCs, backups, spot handling, graceful shutdown) needs to run on schedule, on the right node, with the right identity | Subcommands: `chinit schema-bootstrap`, `schema-sync`, `replica-sync`, `lease-renew`, `keeper-health`, `dead-replica-gc`, `orphaned-gc`, `backup-daily`, `spot-watch`, `drain-shutdown` — units are one ExecStart line, all logic testable Go |
| A terminating instance must release its identity (ordinal lease, DNS record, replica registration) — but an ASG scale-in's SSM dispatch races the agent's own teardown, and an EC2 health-check termination never fires the hook at all | `chinit-shutdown-drain.service` (v0.2.15): ExecStop runs `chinit drain-shutdown` inside systemd's shutdown transaction, ordered `After=clickhouse-server` — the ONE path every termination mode goes through, with a reboot guard (`reboot.target` → skip: identity survives a reboot) |
| A dead node's residue outlives it: the DNS record keeps answering a dead IP until the lease TTL (15 min), and the rendered `remote_servers` keeps naming the dead peer until the next re-render — every cluster fan-out resolves or retries the dead name and stalls | The `lease-renew` tick converges all three layers from the live claims: `/etc/hosts` (v0.2.7), the private zone (`ConvergePeerDNS`, v0.2.14: self-upsert + expired-slot sweep), and the rendered `remote_servers` (`ConvergeRemoteServers`, v0.2.27/0.2.28: drift-only install + admin-authenticated hot reload) — residue heals within one 5-minute tick |
| Cloud CLI calls with hand-built JSON are fragile and per-cloud ports don't compose | Provider adapters over the SDKs: Secrets Manager/Key Vault, DynamoDB/Table Storage ordinal leases, Route53/Private DNS, S3/Blob bundle fetch, IMDSv2 |
| Nothing testable — a boot change means a live-fleet experiment | Unit tests per package + a **provider conformance suite**: one contract, run per cloud against fake IMDS/canned secrets/canned bundle |
| A new cloud means a rewrite | One `infra/azure` package implementing the same ports; GCP/Civo are one package + one registry line each |

## How it adapts to the cloud

Detection runs in precedence order: `-cloud` flag → `CHINIT_CLOUD` env →
DMI `sys_vendor` signature (`Amazon`/`Microsoft`) → IMDS probe. The first
hit builds the provider from the registry:

```go
// pkg/chinit/domain/port/provider.go
type Provider interface {
    Name() string
    MetadataProvider          // instance id, region, private IP — mandatory
    port.SecretSource          // the shared secrets port
    ConfigSource               // SHA-gated bundle fetch — mandatory
}
// Optional capabilities, probed at boot; nil = disabled with a WARNING:
type OrdinalClaimer interface { ClaimOrdinal(...); RenewLease(...); ReleaseLease(...) }
type DNSRegistrar interface  { ResolveZoneID(...); UpsertARecord(...); DeleteARecord(...) }
type SpotSignals interface    { Watch(ctx) <-chan InterruptionNotice }
```

A cloud without a capability (Civo has no IMDS; a provider without managed
DNS) still boots — the pipeline degrades loudly instead of failing.

## The boot

`chinit boot` runs 23 convergent steps in a load-bearing order (identity
settles **before** renders — the "ip-10-0-1-101 never joined the cluster"
production finding; passwords land **before** the render; keepers skip the
server start; the health gate is non-fatal so a reader booting while a peer
is down still joins):

```
PHASE 1 (parallel — errgroup):
  fetch_config_bundle ∥ os_optimizations ∥ require_binaries
  ∥ mount_data_disks ∥ settle_identity ∥ retrieve_passwords ∥ configure_tls

PHASE 2 (serial — each reads what phase 1 wrote):
  install_static_configs → configure_peer_hosts → configure_server_env
  → render_node_config (in-process library — no process boundary)
  → configure_keeper → write_node_env → start_clickhouse
  → bootstrap_schema → health_gate → prometheus_reload → install_units
  → configure_backup → configure_monitoring → configure_cloudwatch
  → install_spot_watch → install_shutdown_drain → boot_finished
```

The settled identity (hostname, CR name, lease, DNS zone) is persisted to
`/var/lib/chinit/state.json` at claim time — the job subcommands reload
it, never re-derive (the step-13 `node-env` shell contract remains
for unit scripts not yet on typed state).

### The termination side (v0.2.15)

Boot installs the units; teardown runs them. Every writer and reader
carries `chinit-shutdown-drain.service` — a no-op-at-start unit whose
**ExecStop** runs `chinit drain-shutdown` when systemd stops it at OS
shutdown (ordered `After=clickhouse-server`, so the drain runs against a
still-live server):

```
stop replication queues → stop distributed sends → peer DROP REPLICA
→ DNS A-record DELETE → ordinal lease release → parting zone sweep
(other expired-slot records)
```

- **Reboots are exempt**: `reboot.target` in `systemctl list-jobs` makes
  the drain decline — a reboot reloads the same node, and its identity
  must survive.
- **The server stop is skipped** (`SkipServerShutdown`): the drain runs
  INSIDE the shutdown transaction; issuing stop commands would fight the
  very teardown that invoked it.
- **Why in-node**: the OS shutdown path is the ONE signal every
  termination mode goes through — ASG scale-in, EC2 health-check
  replacement (which never fires the ASG termination hook), spot reclaim
  at the deadline, operator stop. The hook's SSM dispatch races the
  agent's own teardown and cannot cover all of these; the systemd stop
  path has no delivery race at all.

The settled identity (hostname, CR name, lease, DNS zone) is persisted to
`/var/lib/chinit/state.json` at claim time — the job subcommands reload
it, never re-derive (the step-13 `node-env` shell contract remains
for unit scripts not yet on typed state).

### `chinit boot --refresh`

Re-applies the CURRENT spec/bundle to a **live** node without re-imaging:

```
chinit boot --refresh          # same pipeline, no resume-skip, idempotent
```

What it does differently from a plain boot:

- **Resume is disabled** — every step re-executes (the point is to
  converge the node onto the current bundle even when nothing looks
  different).
- **Identity is restored, never re-claimed.** The persisted identity is
  re-validated (lease holder must still be this instance for lease-mode
  nodes; static mode re-derives from the IP and compares) and re-renewed,
  but never re-claimed from the table — a re-claim could renumber a live
  member of the cluster.
- **Keeper raft state is never wiped** — the `coordination/` wipe that a
  first boot needs is skipped.
- **First-boot-only side effects are skipped** (e.g. the metadata-disk
  wipe) and the service is **restarted** (not `enable --now`) so the
  re-rendered config actually takes effect.

The refresh is what the stack's config refresher invokes over SSM when a
node's `chinit-<role>.yaml` (or the bundle it pins) changes — the
spec-only edit path that previously required a re-image.

## The spec

`chinit.yaml` (rendered by Terraform per role, uploaded to the bundle
prefix, fetched by chinit itself):

```yaml
role: reader
identity_mode: lease        # static (IP-octet map) or lease (ordinal claim)
cluster_name: logexplorer
config_bundle:
  bucket: data-bucket
  prefix: bootstrap/clickhouse
  sha256: <config_files_hash>   # the gate; a stale hash is Fatal
disks:
  metadata_label: ch-metadata   # mounted by LABEL, never device path
secrets:                        # NAMES only; values resolve via the
  default_password: logexplorer-default   # instance role at boot
region: us-east-1
```

## Get the binary

```bash
CHINIT_VERSION=v0.2.15
curl -fsSL -o chinit \
  "https://github.com/sks/clickhouse-operator/releases/download/${CHINIT_VERSION}/chinit-linux-arm64"
chmod +x chinit
# supply-chain gate:
curl -fsSL "https://github.com/sks/clickhouse-operator/releases/download/${CHINIT_VERSION}/chinit-SHA256SUMS" | grep arm64 | sha256sum -c
```

Releases build from the `chinit-v*` tag workflow (vet+test gate, then
GoReleaser — `.goreleaser.chinit.yaml` — producing stripped static binaries for
linux/darwin arm64+amd64 plus `chinit-SHA256SUMS`; the setup-go module cache is
enabled so repeat tags skip cold module downloads). The golden AMI bakes a
pinned version; the user-data wrapper upgrades from the staged
`bootstrap/artifacts/chinit/<version>/` prefix with the same sha256 gate
the cluster-ops port uses.

## Wiring into node boot (the langchain-examples stacks)

EC2/VM user_data shrinks to a ~1.5KB wrapper:

1. ensure the pinned chinit binary (upgrade-or-keep, checksum-gated)
2. fetch the role's `chinit-<role>.yaml` from the bundle prefix
3. `exec chinit boot -spec /var/lib/clickhouse-bootstrap/chinit.yaml -cloud aws`

The whole node lifecycle — first boot, recurring maintenance, and
termination cleanup — is chinit now.

## Layout

```
cmd/chinit/            entry point
pkg/chinit/app/        flag parsing, subcommands, provider wiring
pkg/chinit/domain/
  model/               BootSpec, NodeIdentity, state-file shapes
  port/                provider capability interfaces + registry
  service/             retry engine, boot pipeline, state store
  fail/                Retryable/Fatal classification
pkg/chinit/infra/
  detect/              cloud detection (flag/env/DMI/IMDS)
  system/              disks, OS tuning, units, XML validation
  aws/                 IMDS, Secrets Manager, S3, DynamoDB leases, Route53, spot
  azure/               IMDS, Key Vault, Blob, Table leases, Private DNS, scheduled events
  fakes/               canned IMDS/secrets/bundle for the conformance suite
```
