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
 *   commit    --dir <output-dir> --target <curio-piece-dir> --provider-id N --session-key 0x... --customer-wallet 0x...
 *             [--network mainnet|calibration] [--concurrency N] [--retry]
 */

import fs from 'node:fs'
import path from 'node:path'
import { parseArgs } from 'node:util'

import { calibration, mainnet } from '@filoz/synapse-sdk'
import { isAddress } from 'viem/utils'
import { runCommit } from './commands/commit.mjs'
import { runCreate } from './commands/create.mjs'
import { runDownload } from './commands/download.mjs'
import { runPrepare } from './commands/prepare.mjs'

function usage() {
  console.error(`Usage:
  backup-helper create   --db <space-inventory.db> --dir <output-dir>
  backup-helper download --dir <output-dir> [--concurrency N] [--port N]
  backup-helper prepare  --dir <output-dir> [--concurrency N]
  backup-helper commit   --dir <output-dir> --target <curio-piece-dir> --provider-id N --session-key 0x... --customer-wallet 0x... [--network mainnet|calibration] [--concurrency N] [--retry]`)
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

/**
 * @param {string | undefined} network
 */
function parseCommitNetwork(network) {
  if (network == null || network === '' || network === 'mainnet') {
    return mainnet
  }

  if (network === 'calibration') {
    return calibration
  }

  console.error(`Error: invalid network "${network}". Expected "mainnet" or "calibration".`)
  process.exit(1)
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

/**
 * @param {string[]} argv
 */
async function commit(argv) {
  let values
  try {
    ;({ values } = parseArgs({
      args: argv,
      options: {
        dir: { type: 'string' },
        target: { type: 'string' },
        'provider-id': { type: 'string' },
        'session-key': { type: 'string' },
        'customer-wallet': { type: 'string' },
        network: { type: 'string' },
        concurrency: { type: 'string' },
        retry: { type: 'boolean' },
      },
      allowPositionals: false,
      strict: true,
    }))
  } catch (err) {
    console.error(`Error: ${err instanceof Error ? err.message : String(err)}`)
    process.exit(1)
  }

  if (!values['session-key']) {
    console.error('Error: commit: missing required --session-key <0x...> argument')
    process.exit(1)
  }
  if (!values.target) {
    console.error('Error: commit: missing required --target <path> argument')
    process.exit(1)
  }
  if (!values['customer-wallet']) {
    console.error('Error: commit: missing required --customer-wallet <0x...> argument')
    process.exit(1)
  }
  if (!values['provider-id']) {
    console.error('Error: commit: missing required --provider-id <number> argument')
    process.exit(1)
  }

  const providerId = Number(values['provider-id'])
  if (!Number.isInteger(providerId) || providerId < 1) {
    console.error(`Error: commit: --provider-id must be a positive integer (got ${values['provider-id']})`)
    process.exit(1)
  }

  const customerWallet = values['customer-wallet']
  if (!isAddress(customerWallet)) {
    console.error(`commit: invalid customer wallet address: ${customerWallet}`)
    process.exit(1)
  }

  const sessionKey = values['session-key']
  if (!/^0x[0-9a-fA-F]{64}$/.test(sessionKey)) {
    console.error('commit: --session-key must be a 32-byte hex private key')
    process.exit(1)
  }

  await runCommit({
    dir: requireDir(values.dir, 'commit'),
    providerId,
    sessionKey,
    customerWallet,
    chain: parseCommitNetwork(values.network),
    target: requireDir(values.target, 'commit'),
    concurrency: parseConcurrency(values.concurrency, 'commit'),
    retry: values.retry === true,
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
    case 'commit':
      await commit(rest)
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
