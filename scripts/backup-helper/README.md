# Backup Helper

CLI to backup CAR data from Storacha using the `spaceInventory.db`, SQLite file produced by the migration flow.
This tool turns the inventory into:

1. A deduplicated `aria2` manifest listing every unique shard CAR to download.
2. Downloaded CAR files under `<dir>/shards/`.
3. Per-shard sidecar JSONs keyed by pieceCID v2

The input DB is treated as strictly read-only, and all derived state lives in `tracking.db` under the output directory, so `rm -rf <dir>` is always a safe reset.

## Subcommands

```sh
node scripts/backup-helper/index.mjs create   --db <space-inventory.db> --dir <output-dir>
node scripts/backup-helper/index.mjs download --dir <output-dir> [--port N] [--concurrency N]
node scripts/backup-helper/index.mjs prepare  --dir <output-dir> [--concurrency N]
```

- `create` — read the input DB, deduplicate by `shard_cid` across every space,
  populate `<dir>/tracking.db`, and stream `<dir>/manifest.aria2`. Idempotent
  on re-run: produces byte-identical output for unchanged input and never
  clobbers a previously-computed `piece_cid` in tracking.db.
- `download` — starts a local aria2 RPC worker directly from the command,
  then schedules shards from `tracking.db`.
  Writes each CAR to `<dir>/shards/<shardCID>.car`. Resumable via shard status
  in `tracking.db`, plus aria2's session and per-file `.aria2` control files.
  `--port N` is optional; if omitted, `download` picks a free localhost port
  for that run automatically. The `manifest.aria2` and `aria2.session` files are
  just generated artifacts that can be useful for debugging if needed.
- `prepare` — process completed local shard CARs from `tracking.db`, compute
  pieceCID v2 when `piece_cid IS NULL`, and write `<dir>/shards/<pieceCID>.json`
  sidecars for every eligible shard. It also renames each prepared CAR from
  `<shardCID>.car` to `<pieceCID>.car`. Failures land in `tracking.db`'s
  `failures` table under `stage='prepare'`; rows are deleted on a later
  successful attempt.

## End-to-end workflow

```sh
# 1. Inventory + manifest (fast: minutes for 2M-row input DBs).
node scripts/backup-helper/index.mjs create \
  --db /path/to/space-inventory.db \
  --dir /path/to/backup-dir

# 2. Download CARs (long: hours for terabyte-scale inventories; resumable).
node scripts/backup-helper/index.mjs download \
  --dir /path/to/backup-dir

# Optional: pin aria2 RPC to a specific localhost port for debugging.
node scripts/backup-helper/index.mjs download \
  --dir /path/to/backup-dir \
  --port 6801

# 3. Compute pieceCIDs + write sidecars.
node scripts/backup-helper/index.mjs prepare \
  --dir /path/to/backup-dir
```

## Final Output layout

```text
<dir>/
  manifest.aria2          # one entry per unique shard_cid across all spaces
  tracking.db             # SQLite: shards + download status/failures + prepare state
  aria2.session           # written by aria2 during downloads
  shards/
    <pieceCID>.car        # one prepared CAR per unique pieceCID
    <pieceCID>.json       # one sidecar per unique pieceCID (written by prepare)
```

### `shards/<pieceCID>.json`

One sidecar per unique pieceCID, written by `prepare`. The JSON body carries
both the pieceCID and the shardCID so consumers can join either direction
between the sidecar and the matching `<pieceCID>.car` file.

```json
{
  "shardCid": "bag…",
  "pieceCid": "baga…",
  "sizeBytes": 134217728,
  "rootCids": ["bafy…", "bafy…2"]
}
```

- `shardCid` / `pieceCid` — the two identities for the shard's bytes; either
  can be used to look up the other.
- `sizeBytes` — uncompressed byte size of the CAR file on disk.
- `rootCids` — every distinct root CID that references this shard, across
  all spaces / uploads in the input inventory. Array because a single shard
  can belong to multiple uploads; most arrays will have one entry.

There is no `spaceDid` field in the sidecar. If the SP needs the space-level
mapping (e.g., for billing or quota reporting) they query the input
inventory by `shard_cid` — that's the cheapest source of truth.

## Requirements

- Node.js **24** (uses the built-in `node:sqlite`; no native `better-sqlite3`
  install required).
- `aria2c` on PATH for the `download` subcommand.
  - macOS: `brew install aria2`
  - Debian/Ubuntu: `sudo apt install aria2`
  - Windows: download from <https://aria2.github.io/> and add `aria2c.exe` to PATH.
- `@filoz/synapse-core` for pieceCID v2 streaming hash

## Caveats

- **The input `space-inventory.db` is read-only.** The tool opens it with the
  `{ readOnly: true }` option and never writes. All derived state lives in
  `<dir>/tracking.db`, separate from the client's deliverable.
- **One `<dir>` per client.** The output directory is the unit of "a client's
  backup". Different clients should run in different directories.
- **Don't run two backup-helper commands against the same `<dir>` at the same
  time.** .
- **`download` uses one local aria2 RPC daemon per run.** By default it picks a
  free localhost port automatically; you can override it with `--port N` when
  you need a predictable port for debugging.
- **`download` performance is tuned for Cloudflare R2-hosted shards.** The
  aria2 worker disables intra-file multi-threading (`--split=1`,
  `--max-connection-per-server=1`) and saturates bandwidth horizontally via
  `--max-concurrent-downloads`. Tune via `--concurrency N`; default is 16.
