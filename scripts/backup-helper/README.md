# Backup Helper

CLI to backup CAR data from Storacha using the `spaceInventory.db`, SQLite file produced by the migration flow.
This tool turns the inventory into:

1. A deduplicated `aria2` manifest listing every unique shard CAR to download.
2. Downloaded CAR files under `<dir>/shards/`.
3. (Coming next) per-shard sidecar JSONs keyed by pieceCID v2

The input DB is treated as strictly read-only, and all derived state lives in `tracking.db` under the output directory, so `rm -rf <dir>` is always a safe reset.

## Subcommands

```
node scripts/backup-helper/index.mjs create   --db <space-inventory.db> --dir <output-dir>
node scripts/backup-helper/index.mjs download --manifest <path> [--concurrency N]
node scripts/backup-helper/index.mjs prepare  --dir <output-dir> [--concurrency N]
```

- `create` — read the input DB, deduplicate by `shard_cid` across every space,
  populate `<dir>/tracking.db`, and stream `<dir>/manifest.aria2`. Idempotent
  on re-run: produces byte-identical output for unchanged input and never
  clobbers a previously-computed `piece_cid` in tracking.db.
- `download` — thin wrapper around the bundled `run-backup-download.sh` aria2
  launcher. Reads the manifest, writes each CAR to `<dir>/shards/<shardCID>.car`.
  Resumable via aria2's session + per-file `.aria2` control files; a second
  invocation against the same manifest picks up where the first left off.
- `prepare` *(WIP)* — compute pieceCID v2 for every shard in
  `tracking.db` where `piece_cid IS NULL`, then write `<dir>/shards/<pieceCID>.json`
  sidecars for every shard with a known pieceCID. Failures land in
  `tracking.db`'s `failures` table with an attempt counter; rows are deleted
  on a later successful attempt.

## End-to-end workflow

```sh
# 1. Inventory + manifest (fast: minutes for 2M-row input DBs).
node scripts/backup-helper/index.mjs create \
  --db /path/to/space-inventory.db \
  --dir /path/to/backup-dir

# 2. Download CARs (long: hours for terabyte-scale inventories; resumable).
node scripts/backup-helper/index.mjs download \
  --manifest /path/to/backup-dir/manifest.aria2

# 3. Compute pieceCIDs + write sidecars (coming soon).
node scripts/backup-helper/index.mjs prepare \
  --dir /path/to/backup-dir
```

## Output layout

```text
<dir>/
  metadata.json           # dataset metadata
  manifest.aria2          # one entry per unique shard_cid across all spaces
  tracking.db             # SQLite: shards (deduped inventory) + failures (added by prepare)
  aria2.session           # written by aria2 during downloads
  shards/
    <shardCID>.car        # one copy per unique shard
    <pieceCID>.json       # one sidecar per unique pieceCID (written by prepare)
```

### `metadata.json`

A short, fixed-schema identity document describing the backup itself. Written
by `create`. Contains no per-space data; if the SP needs per-space details
later they can query the input `space-inventory.db` directly.

```json
{
  "source": "filecoin-pin",
  "withIPFSIndexing": ""
}
```

- `source` — identifies the data lineage (the migration flow that produced
  the input inventory). Constant for this version of the tool.
- `withIPFSIndexing` — reserved for future use; currently always an empty string.

### `shards/<pieceCID>.json`

One sidecar per unique pieceCID, written by `prepare`. The JSON body carries
both the pieceCID and the shardCID so consumers can join either direction
between the sidecar and the matching `<shardCID>.car` file.

```json
{
  "shardCid": "bag…",
  "pieceCid": "baga…",
  "sizeBytes": 134217728,
  "sourceUrl": "https://…r2.w3s.link/…",
  "rootCids": ["bafy…", "bafy…2"]
}
```

- `shardCid` / `pieceCid` — the two identities for the shard's bytes; either
  can be used to look up the other.
- `sizeBytes` — uncompressed byte size of the CAR file on disk.
- `sourceUrl` — the URL the CAR was downloaded from (`MIN(source_url)` from
  the input inventory when multiple rows reference the same shard).
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
- **`download` performance is tuned for Cloudflare R2-hosted shards.** The
  launcher disables intra-file multi-threading (`--split=1`,
  `--max-connection-per-server=1`) and saturates bandwidth horizontally via
  `--max-concurrent-downloads`. Tune via `--concurrency N`; default is 16.
