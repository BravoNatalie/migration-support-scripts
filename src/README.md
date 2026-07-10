# Backup Helper

CLI to backup CAR data from Storacha using the `spaceInventory.db`, SQLite file produced by the migration flow.
This tool turns the inventory into:

1. A deduplicated `aria2` manifest listing every unique shard CAR to download.
2. Downloaded CAR files under `<dir>/shards/`.

The input DB is treated as strictly read-only, and all derived state lives in `tracking.db` under the output directory, so `rm -rf <dir>` is always a safe reset.

## Commands

* **`create`** — Reads the input DB, deduplicates shards by `shard_cid` across all spaces, and generates:

  * `<dir>/tracking.db`
  * `<dir>/manifest.aria2`

  Safe to re-run. Produces identical output for unchanged input and never overwrites an existing `piece_cid`.

* **`download`** — Starts a local aria2 RPC worker and downloads shards from `tracking.db` into:

  ```
  <dir>/shards/<shardCID>.car
  ```

  Resumable via `tracking.db`, `aria2.session`, and `.aria2` control files. If `--port` is not provided, a free localhost port is selected automatically.

* **`prepare`** — Processes downloaded CARs, computes missing Piece CID v2 values, and renames files from:

  ```
  <shardCID>.car → <pieceCID>.car
  ```

  Failures are recorded in `tracking.db` (`stage='prepare'`) and cleared automatically after a successful retry.

* **`commit`** — Parks prepared pieces in Curio and performs on-chain commits concurrently using synapse-core. Piece parking is done through:

  ```bash
  curio toolbox import-pieces \
    --source <dir> \
    --target <target> \
    --batch-size N
  ```

  The command continuously imports pieces into Curio while concurrently submitting eligible commits on-chain. Progress is persisted in `tracking.db` (`root_shards` and `migration_metadata`), allowing the operation to be safely resumed after interruptions.

* **`verify`** — Final verification pass for a completed migration. Run this after `commit` has processed all pieces and there are no pending, parked, committing, or failed commit rows left.

The command checks that the input inventory is covered by `tracking.db`, the PDP data set is live, every locally committed piece is active on-chain, and every committed root resolves from the provider service URL. It writes a detailed report to:

```
<dir>/verify-report.json
```

If `--foc-api-url` is provided, missing roots are enriched with indexed on-chain metadata from foc-observer. This helps distinguish roots that were committed on-chain but are not served by the provider from roots whose commit metadata is missing or points at a different IPFS root CID.

## End-to-end workflow

```sh
# 1. Inventory + manifest (fast: minutes for 2M-row input DBs).
node src/index.mjs create \
  --db <space-inventory.db> \
  --dir <output-dir>

# 2. Download CARs (long: hours for terabyte-scale inventories; resumable).
# Optional: pin aria2 RPC to a specific localhost port
node src/index.mjs download \
  --dir <output-dir> \
  [--port N] \
  [--concurrency N]

# 3. Compute pieceCIDs + rename CARs to pieceCID filenames.
node src/index.mjs prepare \
  --dir <output-dir> \
  [--concurrency N]

# 4. Park prepared pieces and commit them on-chain.
node src/index.mjs commit \
  --dir <output-dir> \
  --target <curio-piece-dir> \
  --service-url https://... \
  --provider-address 0x... \
  --session-key 0x... \
  --customer-wallet 0x... \
  [--network mainnet|calibration] \
  [--concurrency N] \
  [--retry]

# 5. Verify after the migration is finalized and all pieces are committed.
node src/index.mjs verify \
  --db <space-inventory.db> \
  --dir <output-dir> \
  [--network mainnet|calibration] \
  [--concurrency N] \
  [--foc-api-url https://...]
```

## Final Output layout

```text
<dir>/
  manifest.aria2          # one entry per unique shard_cid across all spaces
  tracking.db             # SQLite: shards + root_shards + download/prepare/commit state
  verify-report.json      # written by verify with inventory, piece, and root results
  aria2.session           # written by aria2 during downloads
  shards/
    <pieceCID>.car        # one prepared CAR per unique pieceCID
```

## Requirements

* Node.js **24** (uses the built-in `node:sqlite`; no native `better-sqlite3`
  install required).
* `aria2c` on PATH for the `download` subcommand.
  * macOS: `brew install aria2`
  * Debian/Ubuntu: `sudo apt install aria2`
  * Windows: download from <https://aria2.github.io/> and add `aria2c.exe` to PATH.
* `@filoz/synapse-core` for pieceCID v2 streaming hash

## Caveats

* **The input `space-inventory.db` is read-only.** The tool opens it with the `{ readOnly: true }` option and never writes. All derived state lives in `<dir>/tracking.db`, separate from the client's deliverable.
* **One `<dir>` per client.** The output directory is the unit of "a client's backup". Different clients should run in different directories.
* **Don't run two backup-helper commands against the same `<dir>` at the same time.** .
* **`download` uses one local aria2 RPC daemon per run.** By default it picks a free localhost port automatically; you can override it with `--port N` when you need a predictable port for debugging.
* **`download` performance is tuned for Cloudflare R2-hosted shards.** The aria2 worker saturates bandwidth horizontally via `--max-concurrent-downloads` and uses size-based per-file `split` / `max-connection-per-server` settings. Tune via `--concurrency N`; current default is 50.
* **Run `verify` only after commit is finalized.** The command is a final correctness check, not a progress monitor. If commit rows are still pending, parked, committing, or failed, the verification state will be `incomplete`.
* **`verify --foc-api-url` is diagnostic.** The base verification does not require foc-observer. Use the option when missing roots need on-chain context in `verify-report.json`.
