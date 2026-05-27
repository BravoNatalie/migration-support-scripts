# Storacha to FOC Migration: Support Scripts

Helper scripts that sit alongside the storacha `space migrate` workflow.

## Scripts

- [`scripts/backup-helper/`](scripts/backup-helper/) — utilities for
  running the migration internally, from a FOC Node.
- [`scripts/prepare-cars-download/`](scripts/prepare-cars-download/) — read a
  migration `state.json` and prepare a bulk CAR download manifest for `aria2`.
- [`scripts/migration-repair/`](scripts/migration-repair/) — convert
  `shardsToStore` entries (slow Store flow) into `shards` entries (fast Pull
  flow) by computing missing `pieceCID`s locally, then patching the state
  file. Also lists `skippedUploads` and large roots for manual handling.

## Requirements

Node.js 24+ for all scripts.
