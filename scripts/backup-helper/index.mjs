#!/usr/bin/env node
/**
 * backup-helper — CLI for backup workflows
 *
 * Subcommands:
 *   create    --db <space-inventory.db> --dir <output-dir>
 *             Produce manifest.aria2 (deduped by shard_cid across all spaces).
 *   download  --manifest <path> [--concurrency N]
 *             Run aria2 against the manifest; CARs land in <dir>/shards/.
 *   prepare   --dir <output-dir> [--concurrency N]      [not yet implemented]
 */

import path from 'node:path'
import { parseArgs } from 'node:util'

import { runCreate } from './commands/create.mjs'
import { runDownload } from './commands/download.mjs'

function usage() {
  console.error(`Usage:
  backup-helper create   --db <space-inventory.db> --dir <output-dir>
  backup-helper download --manifest <path> [--concurrency N]
  backup-helper prepare  --dir <output-dir> [--concurrency N]`)
}

async function create(argv) {
  let values
  try {
    ;({ values } = parseArgs({
      args: argv,
      options: {
        db: { type: 'string' },
        dir: { type: 'string' },
      },
      allowPositionals: false,
      strict: true,
    }))
  } catch (err) {
    console.error(`Error: ${err instanceof Error ? err.message : String(err)}`)
    process.exit(1)
  }

  if (!values.db) {
    console.error('Error: create: missing required --db <path> argument')
    process.exit(1)
  }

  if (!values.dir) {
    console.error('Error: create: missing required --dir <path> argument')
    process.exit(1)
  }

  if (!['.db', '.sqlite', '.sqlite3'].some((suffix) => values.db.endsWith(suffix))) {
    console.error('Error: create: --db must end with .db, .sqlite, or .sqlite3')
    process.exit(1)
  }

  await runCreate({
    db: path.resolve(values.db),
    dir: path.resolve(values.dir),
  })
}

/**
 * @param {string[]} argv
 */
async function download(argv) {
  let values
  try {
    ;({ values } = parseArgs({
      args: argv,
      options: {
        manifest: { type: 'string' },
        concurrency: { type: 'string' },
      },
      allowPositionals: false,
      strict: true,
    }))
  } catch (err) {
    console.error(`Error: ${err instanceof Error ? err.message : String(err)}`)
    process.exit(1)
  }

  if (!values.manifest) {
    console.error('Error: download: missing required --manifest <path> argument')
    process.exit(1)
  }

  let concurrency
  if (values.concurrency != null) {
    const n = Number(values.concurrency)
    if (!Number.isInteger(n) || n < 1) {
      console.error(`Error: download: --concurrency must be a positive integer (got ${values.concurrency})`)
      process.exit(1)
    }
    concurrency = n
  }

  await runDownload({
    manifest: path.resolve(values.manifest),
    concurrency,
  })
}

const [, , sub, ...rest] = process.argv

try {
  switch (sub) {
    case 'create':
      await create(rest)
      break
    case 'download':
      await download(rest)
      break
    case 'prepare':
      console.error('prepare: not yet implemented')
      break
    default:
      usage()
      process.exit(1)
  }
} catch (err) {
  console.error(`Error: ${err?.message || err}`)
  process.exit(1)
}
