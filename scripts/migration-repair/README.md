# Migration Repair

Tool for repairing storacha → FOC migration state files so that uploads currently
routed to the slow Store flow can instead use the fast Pull flow.

## What it does

Storacha's migration state file partitions shards into two arrays per space:

- `shards` — has `pieceCID`, served via the **Pull flow** (storage provider fetches
  directly from `sourceURL`).
- `shardsToStore` — missing `pieceCID`, served via the **Store flow** (client
  downloads the shard locally, then uploads it to the storage provider). Slow.

For every `shardsToStore` entry, the shard bytes exist at a public carpark URL.
We can download them, compute the `pieceCID` ourselves, write the result back
into the state file, and **move the entry from `shardsToStore` to `shards`**.

`storacha space migrate --resume` trusts the state file, so the same upload now
flows through Pull instead of Store.

Truly unresolvable uploads (`skippedUploads`) carry no `sourceURL` and remain
manual.

## Subcommands

```
node migration-repair.mjs scan      --input <state.json> [--out missing.json]
node migration-repair.mjs repiece   --input <state.json> --db <checkpoint.sqlite> [--concurrency 8] [--limit N]
node migration-repair.mjs patch     --input <state.json> --db <checkpoint.sqlite> --out <patched.json>
node migration-repair.mjs validate  --input <state.json>
node migration-repair.mjs manual    --input <state.json> [--threshold-bytes 1073741824] [--out manual.json]
```

SQLite migration state files are also supported:

```sh
node migration-repair.mjs scan      --input state.db
node migration-repair.mjs repiece   --input state.db --db repiece.sqlite --concurrency 8
node migration-repair.mjs patch     --input state.db --db repiece.sqlite
node migration-repair.mjs validate  --input state.db
node migration-repair.mjs manual    --input state.db --out manual.json
```

- `scan` — count entries missing `pieceCID`, optionally write the work list.
- `repiece` — download each missing shard from its carpark `sourceURL`,
  compute the `pieceCID`, store it in a SQLite checkpoint. Resumable; rerun to
  pick up where it left off.
- `patch` — for every entry the checkpoint covers, fill `pieceCID` and move the
  entry from `shardsToStore` into `shards`. JSON input writes atomically to
  `--out`; SQLite input patches the migration DB in place.
- `validate` — assert the state file has zero `shardsToStore`, every shard has
  a `pieceCID`, every shard has a `sourceURL`, and every `sizeBytes` is `> 0`.
  Exit code 1 if not migratable.
- `manual` — emit a JSON report listing (a) all `skippedUploads` with gateway
  URLs for manual download and (b) every root whose total size is at least the
  `--threshold-bytes` (default 1 GiB) plus its per-shard download URLs.

## End-to-end workflow

```sh
# 1. Inventory (fast, no network)
node migration-repair.mjs scan --input state.json

# 2. Compute missing pieceCIDs (long, ~150 GiB of downloads for nfts2me-scale
#    spaces, resumable, hours)
node migration-repair.mjs repiece \
  --input state.json \
  --db repiece.sqlite \
  --concurrency 8

# 3. Apply the checkpoint to the state file
node migration-repair.mjs patch \
  --input state.json \
  --db repiece.sqlite \
  --out state.patched.json

# 4. Verify
node migration-repair.mjs validate --input state.patched.json

# 5. Feed back to storacha CLI
storacha space migrate --resume --state-file state.patched.json [...]

# Optional: list the unrecoverable ones for manual handling
node migration-repair.mjs manual --input state.patched.json --out manual.json

# SQLite input follows the same flow, except patch runs in place
node migration-repair.mjs patch \
  --input state.db \
  --db repiece.sqlite
```

## Requirements

- Node.js **24+** (uses built-in `node:sqlite`; no native `better-sqlite3`
  install required).
- `@filoz/synapse-core` for `calculateFromIterable` (pieceCID v2 streaming
  hash). Pin to the version that matches the SDK used by storacha's migration
  lib.
- Read access to carpark URLs (public R2; no auth).
- ~150 GiB free disk **not** required — the script never persists shards to
  disk, it streams each one through the pieceCID hash and discards the bytes.

## Caveats

- **`storacha space migrate --resume` must trust the state file.** Confirmed
  with the migration lib author. If a future version re-validates against the
  indexing service on resume and overrides edits, this approach breaks.
- **`skippedUploads` remain manual.** They have no `sourceURL`; we cannot
  reach the bytes.
- **The repair checkpoint DB is separate from the migration state DB.**
  `--db repiece.sqlite` stores only computed `pieceCID` results and failures
  for this repair tool. When the input is a migration SQLite state file, the
  `patch` step updates that migration DB in place.

## How `pieceCID` gets computed

For each missing shard we stream the carpark blob through
`calculateFromIterable` from `@filoz/synapse-core/piece`. This computes the
Filecoin piece commitment (piece CID v2) deterministically. The storage
provider performs the same computation when it pulls the shard, so the value
we write into the state file matches what the SP will commit on-chain.
