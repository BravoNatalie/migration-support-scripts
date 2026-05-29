# Backup Helper Agent Guide

This directory implements a small backup pipeline around a read-only
`space-inventory.db` input and a derived per-output `tracking.db`.

The most important architectural rule is:

- `space-inventory.db` is input only
- `tracking.db` is the mutable source of truth for backup-helper state
- `<dir>/shards/*.car` plus `.aria2` sidecars are the transfer-level resume state

## Command Overview

### `create`

Entry point:

- `commands/create.mjs`

Purpose:

- stream the input inventory
- populate `tracking.db`
- preserve root-to-shard relationships
- write `manifest.aria2` as a transitional/debug artifact

Important behavior:

- `create` is idempotent for the same input
- `tracking.db.shards` is deduplicated by `shard_cid`
- `root_shards(root_cid, shard_cid)` preserves the many-to-many mapping that is
  lost by shard deduplication
- `manifest.aria2` is not the source of truth for download anymore; `tracking.db`
  is
- can be run multiple times with different `space-inventory.db` inputs targeting the same output dir and `tracking.db`

Technical choices:

- ingestion happens in one SQLite transaction for speed
- `piece_cid` uses `COALESCE` on conflict so a previously discovered value is not
  clobbered
- `root_shards` uses `INSERT OR IGNORE` because the mapping is many-to-many and
  naturally deduplicated by `(root_cid, shard_cid)`

What not to do:

- do not add logic here that depends on download state
- do not treat `manifest.aria2` as authoritative queue state

### `download`

Entry point:

- `commands/download.mjs`

Purpose:

- start one private `aria2c` daemon per command invocation
- schedule downloads from `tracking.db`
- persist failures and resumable state back into `tracking.db`

Important behavior:

- `download` is DB-driven, not manifest-driven
- one aria2 daemon is started per run
- the daemon uses a local RPC port; if `--port` is omitted, the app chooses a
  free localhost port
- restart behavior depends on:
  - `tracking.db.shards.download_status`
  - local partial files
  - local `.aria2` sidecar files
- the aria2 session file is secondary; `tracking.db` is the real queue source

Technical choices:

- startup readiness uses a raw HTTP JSON-RPC probe against
  `http://127.0.0.1:<port>/jsonrpc`, then opens the WebSocket transport
- the app keeps using aria2.js for the normal RPC/WebSocket path after startup
- the queue is bounded (`TARGET_RPC_QUEUE_SIZE`) and submitted in batches
  (`RPC_SUBMISSION_BATCH_SIZE`)
- completion detection is local-file aware:
  - output file must exist
  - `.aria2` sidecar must be absent
  - file size must not be smaller than expected
- per-file transfer policy is set in `addUri` options, not in the aria2 process bootstrap
- large files can use more `split` / `max-connection-per-server` than small ones

Failure and retry model:

- aria2 handles short-lived transport retries internally
- backup-helper handles persistent job state in `tracking.db`
- failure rows live in `failures` with:
  - `stage`
  - `shard_cid`
  - `url`
  - `status_code`
  - `error`
  - `retryable`
  - `attempts`
- `status_code` is the structured field used for download retry decisions
- current scheduler-side reconciliation only special-cases `403` failures on the
  original roundabout URL:
  - find failed rows with `status_code = 403`
  - resolve unsigned fallback URL
  - persist `effective_url`
  - requeue shard as `pending`
- fallback resolution is intentionally not done in the `onDownloadError` handler;
  it is scheduler-driven to avoid shutdown races

What not to do:

- do not move queue truth into aria2 session files
- do not reintroduce manifest-driven download behavior
- do not eagerly rewrite source URLs before a real `403` fallback case
- do not make `shards.download_status='error'` rows directly enqueueable again
  unless there is a deliberate retry policy change

### `prepare`

Expected role:

- consume `tracking.db` after download
- validate local `<dir>/shards/<shardCid>.car` file presence
- use `piece_cid`
- rename completed local CARs to `<pieceCid>.car`

Current behavior:

- prepare starts from `download_status = 'complete'`
- local file existence is validated in command code
- prepare accepts either local CAR name on rerun:
  - `<shardCid>.car` before rename
  - `<pieceCid>.car` after rename
- prepare computes pieceCID v2 only when `piece_cid IS NULL`, using
  `calculateFromIterable` from `@filoz/synapse-core/piece`
- prepare renames each eligible CAR to `<pieceCid>.car` and carries any
  `.aria2` sidecar file along with it when present
- prepare failures are recorded in `failures` with `stage='prepare'`

Design implication:

- preserve generic failure staging (`failures.stage`)
- keep `shards` as the canonical deduplicated entity table
- keep future stage-specific behavior out of `create` and `download`
- do not reintroduce network dependency into piece computation; prepare works
  from local `.car` files only

### `commit`

Entry point:

- `commands/commit.mjs`

Purpose:

- park prepared pieces so the target provider can use them
- commit each `(piece_cid, root_cid)` pair on-chain with piece metadata
- persist migration-level state in `tracking.db`

Current behavior:

- commit requires:
  - `--dir`
  - `--target`
  - `--provider-id`
  - `--session-key`
  - `--customer-wallet`
- parking shells out to:
  - `curio toolbox import-pieces --source <dir> --target <target> --batch-size N`
- the command consumes JSON output shaped like:
  - `{ count, pieces }`
- parking marks matching `root_shards` rows from `pending` to `parked`
- commit claims flat root-level batches ordered by `(piece_cid, root_cid)`
- the first successful commit creates the dataset and persists
  `migration_metadata.data_set_id`
- later commit batches can run concurrently
- `--retry` revives `failed` and `committing` rows back to `parked`

Important design choices:

- `root_shards` is the commit work table
- `shards.piece_cid` stays canonical at shard level
- `root_shards.piece_cid` is a propagated copy for commit-phase querying
- commit metadata per row is:
  - `{ ipfsRootCID: rootCid }`
- commit state is stored on `root_shards`, not in `failures`
- migration-level identity and dataset state live in `migration_metadata`

## Persistence Model

### `tracking.db`

Main tables:

- `shards`
  - canonical deduplicated shard inventory
  - one row per `shard_cid`
  - stores shared shard metadata plus current download state

- `root_shards`
  - preserves many-to-many `root_cid <-> shard_cid`
  - required because one shard can belong to multiple roots
  - also carries propagated `piece_cid` plus commit workflow state

- `failures`
  - shared across stages via `stage`
  - one current failure row per `(stage, shard_cid)`

- `migration_metadata`
  - one logical row per backup dir
  - stores:
    - `client_wallet`
    - `provider_id`
    - `data_set_id`
    - migration `state`

Important invariants:

- `shards` is the canonical deduplicated shard table
- `root_shards` is the canonical root membership table
- `root_shards` is also the canonical commit work table
- `failures` stores current retry/error state, not append-only history
- `download_status` belongs to the download stage only, but is intentionally kept
  on `shards` as a pragmatic scheduler optimization

### Input DB

`inventory-db.mjs` opens `space-inventory.db` with `{ readOnly: true }`.

Important nuance:

- if the input DB was created in WAL mode, SQLite may still create `-shm` / `-wal`
  side files when opening it
- that is SQLite/WAL behavior, not a sign that backup-helper is mutating rows in
  the input database

## aria2 Integration

### aria2 Worker

Purpose:

- start one aria2 daemon directly from `download.mjs` with daemon-level settings only

Settings that belong in the aria2 worker process:

- RPC enable/listen flags
- session persistence
- daemon lifecycle (`--stop-with-process`)
- global concurrency (`--max-concurrent-downloads`)

Settings that belong in `addUri`:

- `dir`
- `out`
- `continue`
- `split`
- `max-connection-per-server`

Rationale:

- per-file behavior may depend on shard size
- daemon config should stay global and stable

### Retry Boundary

Split responsibilities carefully:

- aria2:
  - transport-level retries
  - byte-range resume
  - partial-file continuation from `.aria2`

- backup-helper:
  - queue state
  - cross-run persistence
  - fallback URL reconciliation
  - stage-specific failure policy

If behavior looks wrong during restart, inspect:

- `tracking.db.shards.download_status`
- local `.car` and `.car.aria2` files
- `failures`
- aria2 process logs

## Concurrency and Lifecycle

- do not run two backup-helper commands against the same `--dir`
- running different `--dir` values concurrently is supported
- each `download` run owns its own aria2 daemon
- shutdown is scoped to the daemon launched for that run

## Development Notes

When changing code here:

- keep the pipeline boundaries clear:
  - `create` ingests and materializes derived state
  - `download` executes transfers from `tracking.db`
  - `prepare` should consume finished shard state later
- prefer structured DB fields over parsing free-form text in SQL
- keep URL policy in JS code unless the predicate is entirely based on structured
  columns
- avoid speculative abstractions; thin wrappers are preferred
- if you change retry behavior, reason about:
  - shutdown timing
  - restart timing
  - what happens after partial success
  - whether aria2 or backup-helper owns the retry

## Good Places to Start Reading

- `index.mjs` — CLI contract and argument validation
- `commands/create.mjs` — ingest + manifest generation
- `commands/download.mjs` — scheduler, aria2 RPC flow, retry behavior
- `lib/tracking-db.mjs` — schema and persistence model
- `lib/inventory-db.mjs` — read-only source iterator
- `README.md` — operator-facing behavior
