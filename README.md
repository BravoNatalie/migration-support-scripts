# Storacha Backup Helper

CLI tooling for running Storacha backup migration work from a FOC Node.

The CLI owns the backup workflow: creating local tracking state, downloading
shard CARs, preparing piece CARs, committing pieces, and verifying on-chain
state. It also has an aggregate planner that stores aggregate roots and ordered
sub-piece members in `tracking.db` after the source data set is fully committed.

## Current CLI

```sh
pnpm backup-helper --help
```

Direct invocation also works:

```sh
node src/index.mjs --help
```

See [`src/README.md`](src/README.md) for the command flow and operational
notes.

## Requirements

- Node.js 24+
- `pnpm`
- `aria2c` on `PATH` for the `download` command

## Verification

```sh
pnpm test
pnpm run typecheck
pnpm run biome:check
```

## Archived Tools

This repository used to contain several migration support tools. The active
maintenance target is now this CLI only.

- `scripts/prepare-cars-download/` — read a migration `state.json` and prepare
  a bulk CAR download manifest for `aria2`.
- `scripts/migration-repair/` — repair older migration state by converting
  Store-flow shard entries into Pull-flow shard entries where possible.

Recommended archive tag:

```sh
git tag -a archive/pre-backup-helper 9f9818b167395da0e91ea97033d1d347ee4229bf
git push origin archive/pre-backup-helper
```
