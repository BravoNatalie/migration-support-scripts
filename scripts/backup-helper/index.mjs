#!/usr/bin/env node
/**
 * backup-helper — CLI for backup workflows
 *
 * Subcommands:
 *   create    --db <space-inventory.db> --dir <output-dir>
 *             Produce manifest.aria2 (deduped by shard_cid across all spaces).
 *   download  --dir <output-dir> [--concurrency N] [--port N]
 *             Run aria2 via RPC; CARs land in <dir>/shards/. If omitted, --port
 *             defaults to a free localhost port chosen at runtime.
 *   prepare   --dir <output-dir> [--concurrency N]
 */

import fs from 'node:fs'
import path from 'node:path'
import { parseArgs } from 'node:util'

import { runCreate } from './commands/create.mjs'
import { runDownload } from './commands/download.mjs'
import { runPrepare } from './commands/prepare.mjs'

function usage() {
  console.error(`Usage:
  backup-helper create   --db <space-inventory.db> --dir <output-dir>
  backup-helper download --dir <output-dir> [--concurrency N] [--port N]
  backup-helper prepare  --dir <output-dir> [--concurrency N]`)
}

/**
 * @param {string | undefined} dir
 * @param {string} command
 */
function requireDir(dir, command) {
  if (!dir) {
    console.error(`Error: ${command}: missing required --dir <path> argument`)
    process.exit(1)
  }

  return path.resolve(dir)
}

/**
 * @param {string | undefined} db
 * @param {string} command
 */
function requireDb(db, command) {
  if (!db) {
    console.error(`Error: ${command}: missing required --db <path> argument`)
    process.exit(1)
  }

  const ext = path.extname(db)
  if (!['.db', '.sqlite', '.sqlite3'].includes(ext)) {
    console.error(`Error: ${command}: --db must end with .db, .sqlite, or .sqlite3`)
    process.exit(1)
  }

  const resolved = path.resolve(db)
  if (!fs.existsSync(resolved)) {
    console.error(`Error: ${command}: --db file does not exist: ${resolved}`)
    process.exit(1)
  }

  return resolved
}

/**
 * @param {string | undefined} value
 * @param {string} command
 */
function parseConcurrency(value, command) {
  if (value == null) return

  const n = Number(value)
  if (!Number.isInteger(n) || n < 1) {
    console.error(`Error: ${command}: --concurrency must be a positive integer (got ${value})`)
    process.exit(1)
  }

  return n
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

  await runCreate({
    db: requireDb(values.db, 'create'),
    dir: requireDir(values.dir, 'create'),
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
        dir: { type: 'string' },
        concurrency: { type: 'string' },
        port: { type: 'string' },
      },
      allowPositionals: false,
      strict: true,
    }))
  } catch (err) {
    console.error(`Error: ${err instanceof Error ? err.message : String(err)}`)
    process.exit(1)
  }

  const concurrency = parseConcurrency(values.concurrency, 'download')

  let port
  if (values.port != null) {
    const n = Number(values.port)
    if (!Number.isInteger(n) || n < 1 || n > 65_535) {
      console.error(`Error: download: --port must be an integer between 1 and 65535 (got ${values.port})`)
      process.exit(1)
    }
    port = n
  }

  await runDownload({
    dir: requireDir(values.dir, 'download'),
    concurrency,
    port,
  })
}

/**
 * @param {string[]} argv
 */
async function prepare(argv) {
  let values
  try {
    ;({ values } = parseArgs({
      args: argv,
      options: {
        dir: { type: 'string' },
        concurrency: { type: 'string' },
      },
      allowPositionals: false,
      strict: true,
    }))
  } catch (err) {
    console.error(`Error: ${err instanceof Error ? err.message : String(err)}`)
    process.exit(1)
  }

  const concurrency = parseConcurrency(values.concurrency, 'prepare')

  await runPrepare({
    dir: requireDir(values.dir, 'prepare'),
    concurrency,
  })
}

const [, , sub, ...rest] = process.argv

async function main() {
  switch (sub) {
    case 'create':
      await create(rest)
      break
    case 'download':
      await download(rest)
      break
    case 'prepare':
      await prepare(rest)
      break
    default:
      usage()
      process.exit(1)
  }
}

main().catch((err) => {
  console.error(`Error: ${err?.message || err}`)
  process.exit(1)
})
