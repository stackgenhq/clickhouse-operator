# chinit architecture

`chinit` bootstraps a ClickHouse node on any cloud: it detects the
platform, fetches a SHA-gated config bundle, converges the node to a
declarative spec, and installs its own recurring maintenance jobs as
subcommands. [chinit.md](chinit.md) is the user-facing doc — why it
exists, the problems it solves, how to wire it into node boot. This
doc is for contributors: how the binary is put together, why each
layer exists, and where the load-bearing decisions live.

## The shape in one paragraph

One process, four layers, no cloud SDK imports above the `infra/` line:

```
cmd/chinit ──► pkg/chinit/app ──► pkg/chinit/domain ──► pkg/chinit/infra
   main()        flags,            model, port,           aws/, azure/,
                 subcommands,      service (engine,       detect/, system/,
                 provider wiring   steps, units), fail    fakes/
```

Dependencies point inward only. `domain/` compiles without AWS, Azure,
or a Linux box; `infra/` adapts real clouds and real system calls to
the ports the domain declares. The same discipline the operator's
Kubernetes control plane uses, pointed at a different edge: instead of
reconciling CustomResources against a pod-shaped reality, chinit
reconciles a `BootSpec` against a bare machine.

## Layers and what each owns

| Layer | Owns | Never does |
|---|---|---|
| `cmd/chinit` | `main()` — one call into `app.Main` | logic |
| `app/` | flag parsing, subcommand dispatch, provider registry wiring, the `boot` orchestration (`pkg/chinit/app/boot.go`) | cloud calls; it talks to the provider through `port.Provider` |
| `domain/model` | the data shapes: `BootSpec`, `NodeIdentity`, role/mode enums — plain structs with yaml tags | behavior; validation only |
| `domain/port` | capability interfaces + the capability probes (`AsOrdinalClaimer`, `AsDNSRegistrar`, `AsSpotSignals` in `provider.go`) | implementations |
| `domain/service` | the boot pipeline (`steps.go`), the retry engine (`engine.go`), step implementations (`render.go`, `units.go`, `identity in steps.go`), the state store | direct cloud/system access — everything goes through `infra/` or the injected provider |
| `domain/fail` | `Retryable`/`Fatal` classification (`fail/`) | — |
| `infra/detect` | cloud detection: `-cloud` flag → `CHINIT_CLOUD` env → DMI `sys_vendor` → IMDS probe | — |
| `infra/aws`, `infra/azure` | the provider adapters: metadata, secrets, bundle fetch, ordinal leases, DNS, spot signals — one package per cloud, one registry entry each | boot logic |
| `infra/system` | machine facts and mutations: disks, OS tuning, config install, unit install (`system/disks.go`, `ostune.go`, `configs.go`, `units.go`) | cloud API calls |
| `infra/fakes` | canned IMDS/secrets/bundle for the conformance suite | — |

The rule of thumb for placement: if a change would make sense on a
different cloud, it belongs in `domain/`; if it names a cloud API, it
belongs in `infra/<cloud>/`; if it mutates the machine, it belongs in
`infra/system/`.

## The provider factory: mandatory core, optional capabilities

`port.Provider` is deliberately small. Three things are mandatory —
every cloud can name itself, describe its instance, fetch the config
bundle, and resolve secrets:

```go
// pkg/chinit/domain/port/provider.go
type Provider interface {
    Name() string
    MetadataProvider          // instance id, region, private IP
    port.SecretSource         // the same secrets port the cluster ops use
    ConfigSource              // SHA-gated bundle fetch
}
```

Everything else is an **optional capability**, probed once at boot:

```go
func AsOrdinalClaimer(p Provider) OrdinalClaimer   // readers' identity leases
func AsDNSRegistrar(p Provider) DNSRegistrar       // private-zone A records
func AsSpotSignals(p Provider) SpotSignals         // interruption notices
```

A provider without a capability does not fail the boot — the pipeline
degrades loudly (a `WARNING` naming the missing capability) and the
step either skips or refuses spec shapes that demand it
(`identity_mode: lease` with no claimer is `Fatal`, because a leased
reader without a claimer boots as an ephemeral `ip-*` name and breaks
replication identity — the exact production finding this rule comes
from).

This is the seam that makes new clouds cheap: GCP or Civo is one
`infra/<cloud>` package implementing at minimum the three mandatory
ports, plus a registry entry. The conformance suite (below) is the
proof the contract holds.

## The boot pipeline

`app.Main("boot", …)` → detect cloud → build provider → parse spec →
`boot.Steps(ctx)` → `engine.Run(ctx, steps)`. The step list is the
architecture's spine (`pkg/chinit/domain/service/steps.go`):

**Phase 1 runs concurrently** (a `ParallelGroup` — errgroup under the
hood, see "The retry engine" below): the boot's wall-clock poles are
IO-bound and INDEPENDENT — the claim sweep can legally wait out a
predecessor's lease for many minutes while the bundle downloads, EBS
attaches and the kernel settings apply. The phase pays the MAX of the
poles, not the sum. Each child keeps its own retry policy and
criticality; the FIRST Critical child failure cancels the siblings
(their contexts abort) and aborts the boot; BestEffort children
record-and-continue exactly as serial ones do; resume is per-CHILD
(a group re-run skips only the children the state file records done).
Children write disjoint files by construction (the claim touches only
the lease table and DNS; the passwords write 0600 files under the
bundle dir; TLS writes only the ssl dir; the tuning writes sysctls) —
adding a child requires proving that disjointness, not assuming it.

| # | Step | Crit | Policy | What it does / why here |
|---|------|------|--------|-------------------------|
| 1 | `fetch_config_bundle` | Critical | apiRetry | **[phase 1]** S3/Blob fetch of the XMLs + chi.yaml + keeper.yaml; the SHA256 gate is Fatal (a stale hash means the spec does not vouch for these bytes) |
| 2 | `os_optimizations` | Critical | onceFatal | **[phase 1]** THP off, limits, sysctls — convergent probes first |
| 3 | `require_binaries` | Critical | onceFatal | **[phase 1]** clickhouse binaries present; the `aws` CLI check is cloud-gated (an Azure node never has it) |
| 4 | `mount_data_disks` | Critical | bootWait | **[phase 1]** mount by LABEL (NVMe renames device paths); ownership; the keeper state dirs prepared and chowned |
| 6 | `settle_identity` | Critical | claimSweep | **[phase 1]** the identity contract (below); persists immediately; concurrently-safe (touches only the provider) |
| 8 | `retrieve_passwords` | Critical | apiRetry | **[phase 1]** secrets → 0600 files; values never enter state or logs; distinct filenames from the bundle |
| 9 | `configure_tls` | Critical | apiRetry | **[phase 1]** TLS material from the cloud store, or self-signed interserver-only when no secrets are configured; writes only the ssl dir |
| 5 | `install_static_configs` | Critical | onceFatal | **[phase 2]** the bundle's XMLs → `/etc/clickhouse-server/config.d` (reads what phase 1 fetched) |
| 7 | `configure_peer_hosts` | Critical | onceFatal | **[phase 2]** `/etc/hosts` entries for writer short names — the replication queue addresses the leader by short name and systemd-resolved does not answer short names; needs the settled identity |
| 10 | `configure_server_env` | Critical | onceFatal | **[phase 2]** `/etc/default/clickhouse-server` + the systemd drop-in; `storage.xml`'s `from_env` endpoint is a start requirement (Code 36), not a warning |
| 11 | `render_node_config` | Critical | onceFatal | the whole node config through the in-process render library (below) |
| 12 | `configure_keeper` | Critical | onceFatal | keeper nodes: single-file render + myid/raft-id discipline + service enable; no-ops for others — a keeper joining with a stale --config disrupts the raft ensemble itself (the role gate keeps every non-keeper boot unaffected) |
| 13 | `write_node_env` | BestEffort | onceFatal | the shell-contract file for anything not yet on typed state |
| 14 | `start_clickhouse` | Critical | onceFatal | enable, start, wait for `/ping`, S3-disk health probe; keepers return immediately |
| 15 | `bootstrap_schema` | BestEffort | apiRetry | **[phase 2]** the bundle's DDL applied per-node (every CREATE is IF NOT EXISTS with {shard}/{replica} macros): a node that already has its tables is a no-op; a FULLY-CHURNED fleet re-materializes its own schema at boot (the finding: when every peer churns within one refresh, no node holds the schema for schema-sync to propagate from, and the fleet stays schemaless until a manual DDL session). Retried by the schema-bootstrap timer until a pass is clean |
| 16 | `health_gate` | BestEffort | onceFatal | the in-process health fan-out with the CR inventory; non-fatal BY DESIGN so a reader booting while a peer is down still joins |
| 17 | `prometheus_reload` | BestEffort | bootWait | reload + wait for the metrics endpoint; restart fallback |
| 18 | `install_units` | Critical | onceFatal | the recurring-job units (below): lease-renew keeps the ordinal claim alive (an expired claim is identity theft — another instance claims the same ordinal), schema-sync materializes the node's tables, disk-autoscale is the merge-stall early warning |
| 19 | `configure_backup` | Critical | onceFatal | clickhouse-backup config + daily timer — no-ops cleanly when backups are not configured; when the operator EXPLICITLY pinned a bucket, a silent no-backup node is a restore-time data-loss discovery |
| 20 | `configure_monitoring` | BestEffort | onceFatal | OTEL/Fluent Bit shipping; silently disabled when the endpoint is empty |
| 21 | `install_spot_watch` | BestEffort | onceFatal | readers on spot-capable clouds: the drain daemon |
| 22 | `install_shutdown_drain` | Critical | onceFatal | writers + readers: the in-node shutdown drain unit (below) — a node without it terminates cold and leaks the stale-DNS/lease residue |
| 23 | `boot_finished` | Critical | onceFatal | the final timing summary |

**The criticality question, stated once**: Critical means "a node
that boots WITHOUT this step is a liability as a cluster member" — it
holds an identity it cannot renew, a keeper seat with a stale config,
a termination path that leaks residue, or an operator-promised backup
that does not exist. BestEffort means "observable-by-absence or
environment-shaped": the health gate (a reader joining mid-churn must
still join — the gate wedge was a real boot-loop incident), metrics
reload, log shipping, the spot watcher. A boot that "succeeds" while
skipping a Critical step is worse than a boot that fails: the ASG
replacement the failure triggers IS the remedy. The matrix is pinned
by specs (`criticality_test.go`) — a future edit answers for its
failure mode instead of defaulting to "continue".

Three deliberate orderings, all paid for in production findings:

- **Identity before renders.** A pre-claim hostname bakes into Keeper
  paths and macros and the node joins as the wrong replica.
- **Passwords before renders.** The users render resolves
  `passwordFrom` references at render time through the provider's
  secret source.
- **Health gate non-fatal.** A degraded boot must be visible (the
  verdict prints) but not wedged in a boot loop — the runbooks, not
  the boot, decide whether degraded is acceptable.

### The retry engine and failure taxonomy

Every step runs through `engine.Do(ctx, policy, op)`
(`domain/service/engine.go`): bounded attempts, exponential backoff
with jitter, per-policy deadline. Classification is by error type, not
by step: `fail.Fatal` short-circuits immediately (IAM AccessDenied,
SHA mismatch, a missing image binary — retrying masks a config bug and
burns the boot window), everything else retries under the step's
policy:

```go
// steps.go — the named policies
apiRetry   = Policy{Attempts: 5,  BaseDelay: 2s, MaxDelay: 10s} // transient API ills
bootWait   = Policy{Attempts: 30, BaseDelay: 2s, MaxDelay: 10s} // the system being slow
onceFatal  = Policy{Attempts: 1}                                // config-class: one try, no masking
claimSweep = Policy{Attempts: 3, BaseDelay: 5s, MaxDelay: 5s}    // ordinal claim races
```

Criticality decides what a *failure* means: a Critical step failing
aborts the boot with a non-zero exit (an ASG member then gets
replaced — the node cannot self-heal a broken disk or wrong image);
a BestEffort step logs and continues. The classification is a design
decision per step, stated up front — one table row per step, not a
runtime guess.

### Convergence and resume: the state contract

`engine.Run` loads `/var/lib/chinit/state.json` once and treats it as
the run's working state:

```go
// domain/service/engine.go
type modelState struct {
    Identity *bootIdentity `json:"identity,omitempty"` // the settled snapshot
    SpecHash string        `json:"spec_hash,omitempty"` // the bundle SHA this run targets
    Steps    []stepRecord  `json:"steps"`                // per-step done/failed records
}
```

- **Resume** — a step recorded `done` is skipped; a crash mid-boot
  re-runs only what is missing.
- **Spec-hash invalidation** — a state recorded under a different
  bundle hash is stale by definition: the configs it installed are not
  what the current release vouches for, so the records wipe and the
  pipeline re-renders.
- **Identity is shared state, not a side file** — `settle_identity`
  persists through the same in-memory object the engine saves per
  step. Two Save paths racing was a real bug (the persister's
  Load+Save clobbered the run's pre-settle copy; the subcommands then
  read "no settled identity" from a file whose node-env proved the
  settle ran). The engine owns one state object for exactly this
  reason.

The subcommands (`schema-sync`, `replica-sync`, `dead-replica-gc`, …)
reload `state.json` and never re-derive identity — identity is typed
state, settled once; the step-13 `node-env` shell contract remains
only for unit scripts not yet migrated to typed state.

## Identity: two modes, one naming contract

`settle_identity` (`steps.go`) is where the cluster's view of "who is
this node" is decided, and it is the step most of the pipeline's
invariants hang off.

**Static mode** (writers, keepers, backup): the private IP's last octet
maps to a role-ordinal (`10.0.1.1x` → `writer-x`, `10.0.1.2x` →
`keeper-x`, `10.0.1.30` → `backup-0`) — `StaticHostname()`. Fixed
machines keep fixed addresses; identity derives from the address.

**Lease mode** (readers, and writers on ASG-only stacks): an ordinal
claim against the provider's lease table (DynamoDB on AWS, Table
Storage on Azure) — a conditional write that succeeds only if the row
is free or its lease expired, renewed by timer, **released by the
shutdown drain** (below). The claim ceiling is the role's ASG **max**
(`reader_max` / `writer_max`), not today's count: a scale-out past the
baseline must be able to claim, or it boots forever identity-less.

Both modes then derive the **CR name** — the identity the Altinity
operator's machinery uses (`CRName()` in `boot.go`). The CR layout
names its single shard `01` (never an anonymous zero index — that
re-keys every table's Keeper path), so CR hosts are `01-0..01-(N-1)`
over the full server fleet: writers take `01-0..`, readers continue at
`01-(writer_count + ordinal)..`, and the **backup replica is a CR
member** (v0.2.26) at the FIRST slot after the readers' ceiling —
`01-(writer_max + reader_max)`, the +1 the Terraform render folds
into `replicasCount`. The backup materializes the same replicated
tables (it is a part-fetch peer despite holding no lease), so its
render needs the host-group sections like any server node: an empty
CRName meant the host-group render never ran on it (no macros file —
Code 139 on `{replica}` paths; no zookeeper section — Code 225;
remote_servers listing VACANT slots — Code 198). Keepers remain
non-CR — the empty CR name is the signal every consumer keys on.

The settled identity persists **immediately** at claim time
(`PersistIdentity`), before renders and the server start: a crash
later must not lose it, and the job subcommands read it from state.

## The render path: the library, not a CLI

`render_node_config` and `configure_keeper` do not shell out to
the library packages in `pkg/chinit/domain/...` —
`SpecLoader`, `NodeRenderer`, `ConfigRenderer` from
`pkg/chinit/infra/operator` — as a library:

```
BootSpec ──► LoadCHI/LoadCHK ──► NodeRenderer.RenderNode(...)
                                    │  Hosts:       CR-name → DNS-address map
                                    │  Replicas:     CR-name → short-name map ({replica} macro)
                                    │  Networks:     per-user allowlists + subnet CIDR
                                    │  RemoteUser/Password: distributed-query credentials
                                    │  Keeper:      single-file mode, RaftIDs, ServerID
                                    ▼
                       config.d/users.d XML — staged, validated, chowned,
                       0640/0644 by credential-bearing-ness, manifest-pruned
```

Why each map exists (every one is a live finding):

- `Hosts` keyed by CR name — key by hostname and `remote_servers`
  renders the operator's unresolvable pod-DNS pattern. The inventory
  includes the backup replica (v0.2.26: it is a CR member and a
  part-fetch peer despite holding no lease).
- `{shard}` macro substitution in the spec's DDL fragments — the
  bundle's CREATE statements carry `{shard}`/`{replica}` macros that
  the per-node bootstrap substitutes BEFORE execution (v0.2.26:
  an unsubstituted `{shard}` in a Keeper path re-keyed every table).
- Config-fragment discipline (v0.2.30): user-level settings at the
  TOP level of a server config fragment are REFUSED at load
  (ClickHouse 26.x Code 137 UNKNOWN_ELEMENT_IN_CONFIG) — the loader
  rejects the spec with the offending key named, instead of letting
  the server crash-loop on a fragment that parses but cannot apply.
- `Replicas` — without it the `{replica}` macro renders the pod
  pattern; the table registers under a Keeper path no cloud tooling
  recognizes and `SYSTEM SYNC/DROP REPLICA` operate on the wrong
  identity.
- `Networks` with the **node subnet CIDR merged in** — the render
  defaults to localhost-only; the cluster's own peers arrive from the
  subnet and every cross-node query dies Code 516 otherwise.
- `RemoteUser/RemotePassword` — spliced into `remote_servers`
  `<replica>` entries; without them nodes authenticate as passwordless
  `default` against nodes whose `default` requires a password.
- Keeper mode renders **one merged file** (`-single-file` semantics):
  `clickhouse-keeper` runs with an explicit `--config`, so config.d
  auto-include is disabled and the node needs a single complete
  document — with 1-based raft IDs (the myid-disk convention; the CR's
  zero-based indices render `<id>0</id>` and keeper exits Code 568:
  "our server id 1 not found in raft_configuration") and the
  root-level `listen_host` from the spec's `files:` section
  (`keeper_server/listen_host` is silently ignored — the config is
  rendered, the lint warns, the section never applies).

Keeper identity is **cloud state, not render state**: `server_id`
comes from the raft disk's `myid` file when it exists (re-image
safety — the ensemble's identity outlives the instance), is seeded
1-based when it does not, and a stale `0` is never honored.

## The recurring jobs: subcommands as the unit interface

Every recurring maintenance job after boot is a `chinit` subcommand.
Units are one `ExecStart` line — logic lives in testable Go:

| Unit (service/timer) | ExecStart | Roles | Cadence |
|---|---|---|---|
| `schema-bootstrap` | `chinit schema-bootstrap` | writer, reader, backup | every 15 min — idempotent re-apply of the bundle DDL until a pass is clean (the boot's bootstrap can exhaust its retries against a mid-churn peer; the timer is the after-the-boot heal) |
| `schema-sync` | `chinit schema-sync -retries 3 -prune-stale-self` | writer, reader | hourly |
| `replica-sync` | `chinit replica-sync` | writer, reader | post-boot |
| `keeper-health` | `chinit keeper-health` | keeper | timer |
| `lease-renew` | `chinit lease-renew` | leased nodes (writers + readers), and lease-less CR members (the backup — v0.2.29: DNS-resolved hosts lines) | every 5 min — also converges `/etc/hosts`, the private zone (the DNS self-heal, v0.2.14) and the rendered `remote_servers` (v0.2.27/0.2.28) |
| `dead-replica-gc` | `chinit dead-replica-gc` | writers (runs on the ordinal-0 holder) | hourly (v0.2.29 — weekly left a dead registration wedging fan-outs for days) |
| `orphaned-data-gc` | `chinit orphaned-gc` | writers (runs on the ordinal-0 holder) | weekly |
| `clickhouse-backup-daily` | `chinit backup-daily` | backup replica | daily 02:00 |
| `chinit-spot-watch` | `chinit spot-watch` | spot readers | daemon |
| `chinit-shutdown-drain` | `chinit drain-shutdown` (at **ExecStop**, not ExecStart) | writers + readers | once per OS shutdown |

Each subcommand is a thin orchestration over the **in-process clusterops port** for
cluster operations (schema sync, replica sync, replica gc, replica drop, keeper mntr) —
no subprocess per job, typed errors end to end — and over the provider for
cloud operations (lease renewal, DNS cleanup). They reload identity from
`state.json`; none re-derive it.

**Maintenance gating and the GC safety contract.** With ASG writers,
`writer-0` is a lease ordinal, not a machine — the maintenance timers
(`dead-replica-gc`, `orphaned-data-gc`) install on every writer and the
subcommands no-op exit 0 on any node that does not hold the settled
identity `writer-{prefix}0` (readers and non-0 writers take the same
skip path, so an SSM manual run cannot fire the sweep from the wrong
node). The dead-replica GC itself is two passes with an asymmetric
safety posture: the **reader pass always runs** (`reader-*` — readers
are disposable, their drain already dropped them authoritatively); the
**writer pass runs only with liveness data** — it lists the lease
table's active claims first and passes every live writer as an exclude
entry on the gc request, so a stale-looking registration whose lease
is minutes fresh (a mid-boot re-join) is reported but never dropped.
No lease-listing capability or a scan failure skips the writer pass
with a warning; the reader pass still ran. The GC request carries the
exclude set for exactly this guard.

The cadence is **hourly** (v0.2.29 — weekly left a dead registration
wedging cluster fan-outs for days after the drain missed it; hourly
bounds the exposure to the GC's own window while the liveness guard
keeps a mid-boot re-join safe).

## The termination story: three triggers, one drain sequence

A node's identity — the ordinal lease, the DNS A record, the replica
registration — outlives the instance by default. Nothing in the
infrastructure releases it on death; the chinit design runs the SAME
drain sequence (`chinit drain`) from three triggers, each covering a
termination mode the others cannot reach:

| Trigger | Termination modes | Why it exists |
|---|---|---|
| **In-node shutdown drain** (`chinit-shutdown-drain.service`, v0.2.15) | ALL: ASG scale-in, EC2 health-check replacement, spot reclaim at the deadline, operator stop | The OS shutdown path is the ONE signal every mode goes through. systemd runs the drain from inside the node — no external dispatch, no delivery race. |
| **ASG termination lifecycle hook** (the stack's writer_drain module → SSM `chinit drain -reason termination-hook`) | ASG scale-ins and instance refreshes | Belt-and-suspenders: the hook holds the terminating phase open (300 s heartbeat) while the drain runs, catching what the OS drain misses. |
| **Spot watcher** (`chinit spot-watch`, readers) | Spot reclaims, 2 minutes before the deadline | The notice's deadline lets the drain finish BEFORE the reclaim — the replacement can re-claim the ordinal immediately. |

### The in-node shutdown drain (the primary layer)

`install_shutdown_drain` (boot step 21) writes
`chinit-shutdown-drain.service` on every writer and reader:

- **A no-op unit whose ExecStop is the point.** `ExecStart=/bin/true`
  with `RemainAfterExit=yes` leaves the unit "active" so systemd has
  something to stop; `ExecStop=/usr/local/bin/chinit drain-shutdown`
  runs at teardown.
- **Ordering is the design.** `After=network-online.target
  clickhouse-server.service` means systemd's shutdown stops units in
  REVERSE dependency order — the drain runs while ClickHouse is still
  up to serve the peer drop and the `SYSTEM STOP` queries, then the
  server stops.
- **`SkipServerShutdown`** — the shutdown drain never issues
  `SYSTEM SHUTDOWN` nor `systemctl stop clickhouse-server`: it runs
  INSIDE the shutdown transaction that invoked it, and a stop job
  waiting on a stop job is a deadlock.
- **The reboot guard.** A reboot reloads the same node with the same
  identity — its lease, DNS record and replica registration must all
  survive. `drain-shutdown` reads `systemctl list-jobs` and declines
  (exit 0, loud log) when `reboot.target` is the pending job. The
  check is best-effort: a systemd IPC failure assumes TERMINATION
  (a skipped drain on a real termination is the costlier failure; the
  lease TTL and the weekly GC are the safety nets).
- **Budget discipline.** The drain's own deadline is 150 s; the
  unit's `TimeoutStopSec` is 170 s — both under EC2's 180 s
  API-driven shutdown window, so the drain is never the reason a
  termination is killed mid-drain.
- **The parting zone sweep.** After releasing its own record, the
  dying node sweeps the zone's OTHER expired-slot records
  (`SweepExpiredSlotRecords` — the sweep half of the DNS convergence
  below, WITHOUT the self-heal upsert, which would re-create the
  record the drain just deleted). A terminating node next to an
  undrained predecessor heals that predecessor's stale record
  immediately instead of waiting for the next renewal tick elsewhere.
- **Keepers and backup are excluded** (the role gate): keepers hold
  no lease and run no clickhouse-server; the backup replica's
  identity must survive shutdowns.

**Failure is CONTINUE, by design** — the same contract as the hook:
the node is stateless (parts durable in object storage, quorum spans
the shard), a failed drain logs loudly, and the safety nets (lease
TTL 15 min, hourly dead-replica GC, the periodic convergences)
clean the residue.

### The periodic convergences (the self-healing net)

Every `lease-renew` tick (5 min) converges three layers of the same
live-claims inventory, best-effort — the RESOLUTION of peer names and
the MEMBERSHIP list the fan-outs use:

- **`/etc/hosts`** (`ConvergePeerHosts`, v0.2.7): sweep + rewrite the
  short-name lines from the live claims — replication queues fetch
  parts by bare hostname and systemd-resolved will not answer short
  names. Lease-less CR members (the backup) earn their hosts line via
  DNS resolution (v0.2.29 — the backup claims no ordinal, so the
  claims list alone never listed it).
- **The private DNS zone** (`ConvergePeerDNS`, v0.2.14): upsert SELF
  (the settled identity is authoritative for its own record even
  after an ASG re-IP), then delete every ordinal name whose lease
  expired — the A record a missed drain leaves points at a dead IP
  and wedges every cluster fan-out that resolves it.
- **The rendered `remote_servers`** (`ConvergeRemoteServers`,
  v0.2.27/0.2.28): re-render the cluster inventory section from the
  CURRENT live claims (the same render path the boot uses — the
  vacant-slot filter, credential injection), install it ONLY when
  the content drifted (byte-compare against the installed file), and
  hot-reload — `SYSTEM RELOAD CONFIG` applies `remote_servers` live,
  authenticated as admin via env credentials (v0.2.28; the bare
  client falls back to the passwordless `default` whose readonly
  profile refuses the statement). A scale-in peer's entry leaves the
  rendered inventory within one tick instead of waiting for the next
  boot/refresh re-render.

The DNS sweep is race-safe by construction: a replacement claims the
ordinal BEFORE it boots (the claim row is live by then), so a record
whose lease is live is never a delete candidate; an expired lease
has no holder by definition. The `remote_servers` convergence can
never fight the render contract — a converged file is byte-identical
to what the next boot --refresh would write.

## The conformance suite: one contract, every cloud

`domain/service/conformance` is the provider contract as executable
scenarios: metadata probes, secret resolution, bundle fetch with the
SHA gate, ordinal claim/renew/release, DNS upsert, spot notices —
run per registered provider against `infra/fakes` (canned IMDS,
canned secrets, a canned bundle). Optional-capability scenarios
**skip** for providers that lack the capability; the mandatory core
must pass everywhere.

Adding a cloud = one `infra/<cloud>` package + one suite entry. The
scenarios never change — that is the point: the factory's contract is
pinned by a test, not by convention.

## Invariants worth knowing before you change something

1. **Identity precedes rendering.** Any step that touches config must
   run after `settle_identity`; anything that derives a name must read
   the settled identity, never re-derive it.
2. **The shard's name is load-bearing.** `01` expands into every
   ReplicatedMergeTree Keeper path; an anonymous layout re-keys the
   cluster's data.
3. **Keeper IDs are 1-based and disk-owned.** myid file wins; a fresh
   disk seeds ordinal+1; zero is never valid.
4. **Exclusive resources order destroy-then-create** — the static IPs,
   the raft volume, the quorum seat. The IaC that drives node
   replacement must not let create-before-destroy flags propagate to
   these instances (the InvalidIPAddress.InUse class of failure).
5. **Fatal is a design decision, not an escape hatch.** If a retry
   would mask a config bug, classify Fatal; if a failure should stop
   the boot, mark the step Critical. The matrix is the product.
6. **The spec is data; the binary is logic.** Behavioral changes ship
   as a new chinit release (the golden image bakes it, checksummed);
   the spec changes what the same binary targets. Never smuggle logic
   into the spec template.
7. **The drain runs inside the transaction it must not fight.** The
   shutdown drain is an ExecStop: it releases identity (DNS, lease,
   replica registration) and STOPS — it never issues stop commands
   against services systemd is already stopping under it (a stop job
   waiting on a stop job is a deadlock), and it never runs on a reboot
   (identity survives a reboot; `drain-shutdown` declines on
   `reboot.target`).
8. **Cleanup has three layers on purpose.** The shutdown drain (every
   termination mode, in-node), the ASG hook (scale-ins, held open with
   heartbeats), and the periodic DNS convergence (the 5-minute
   self-heal). Removing one layer is fine only if the failure mode it
   uniquely covers is acceptable — the health-check termination covers
   ONLY the shutdown drain, and the "SSM could not deliver" case
   covers only the other two.

## Where to look in the tree

```
cmd/chinit/main.go               entry point
pkg/chinit/app/
  app.go                          subcommand dispatch + version
  boot.go                         boot orchestration, provider wiring, state store
  subcommands.go                  the recurring jobs; the cluster ops run in-process via the clusterops port
pkg/chinit/domain/
  model/spec.go                   BootSpec + validation (the data contract)
  port/provider.go                Provider, capabilities, registry, probes
  service/steps.go                the pipeline table, identity, TLS, env
  service/engine.go               retry Do(), state store, shared-state discipline
  service/render.go               in-process library renders (node + keeper)
  service/units.go                unit composition per role (incl. the shutdown-drain unit)
  service/hosts.go                the /etc/hosts convergence (lease-renew tick)
  service/peerdns.go              the private-zone convergence (lease-renew tick) + the parting sweep
  service/peerrservers.go         the remote_servers convergence (lease-renew tick, v0.2.27+)
  service/conformance/            the provider contract suite
  fail/                           Retryable/Fatal classification
pkg/chinit/infra/
  detect/                         flag/env/DMI/IMDS detection
  system/                         disks, ostune, configs, units — the machine
  aws/, azure/                    one package per cloud
  fakes/                          the conformance doubles
```
