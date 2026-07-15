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
 *   commit    --dir <output-dir> --target <curio-piece-dir> --service-url https://... --provider-address 0x... --session-key 0x... --customer-wallet 0x...
 *             [--network mainnet|calibration] [--concurrency N] [--retry]
 *   verify    --db <space-inventory.db> --dir <output-dir> [--network mainnet|calibration] [--concurrency N]
 *   aggregate-plan --dir <output-dir> [--max-size-bytes N]
 *   aggregate-submit --dir <output-dir> --private-key 0x... [--data-set-id N] [--retry] [--service-url https://...] [--provider-address 0x...] [--network mainnet|calibration] [--batch-size N]
 */

import fs from 'node:fs'
import path from 'node:path'
import { parseArgs } from 'node:util'

import { calibration, mainnet } from '@filoz/synapse-core/chains'
import { isAddress } from 'viem/utils'
import { parsePositiveBigInt, runAggregatePlan } from './commands/aggregate-plan.mjs'
import { runAggregateSubmit } from './commands/aggregate-submit.mjs'
import { runCommit } from './commands/commit.mjs'
import { runCreate } from './commands/create.mjs'
import { runDownload } from './commands/download.mjs'
import { runPrepare } from './commands/prepare.mjs'
import { runRemovePieces } from './commands/remove-pieces.mjs'
import { runReportDuplicates } from './commands/report-duplicates.mjs'
import { runVerify } from './commands/verify.mjs'

function usage() {
  console.error(`Usage:
  backup-helper create   --db <space-inventory.db> --dir <output-dir>
  backup-helper download --dir <output-dir> [--concurrency N] [--port N]
  backup-helper prepare  --dir <output-dir> [--concurrency N]
  backup-helper commit   --dir <output-dir> --target <curio-piece-dir> --service-url https://... --provider-address 0x... --session-key 0x... --customer-wallet 0x... [--network mainnet|calibration] [--concurrency N] [--retry]
  backup-helper verify   --db <space-inventory.db> --dir <output-dir> [--network mainnet|calibration] [--concurrency N] [--foc-api-url https://...]
  backup-helper aggregate-plan --dir <output-dir> [--max-size-bytes N]
  backup-helper aggregate-submit --dir <output-dir> --private-key 0x... [--data-set-id N] [--retry] [--service-url https://...] [--provider-address 0x...] [--network mainnet|calibration] [--batch-size N]`)
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

/**
 * @param {string | undefined} value
 */
function requireServiceUrl(value) {
  if (!value) {
    console.error('Error: commit: missing required --service-url <https://...> argument')
    process.exit(1)
  }

  try {
    return new URL(value).toString()
  } catch {
    console.error(`Error: commit: invalid --service-url value: ${value}`)
    process.exit(1)
  }
}

/**
 * @param {string | undefined} value
 * @param {string} optionName
 */
function parseAddress(value, optionName) {
  if (!value) {
    console.error(`Error: commit: missing required ${optionName} <0x...> argument`)
    process.exit(1)
  }

  if (!isAddress(value)) {
    console.error(`commit: invalid ${optionName} address: ${value}`)
    process.exit(1)
  }

  return value
}

/**
 * @param {string | undefined} value
 * @param {string} command
 * @param {string} optionName
 */
function parsePrivateKey(value, command, optionName) {
  if (!value || !/^0x[0-9a-fA-F]{64}$/.test(value)) {
    console.error(`Error: ${command}: ${optionName} must be a 32-byte hex private key`)
    process.exit(1)
  }

  return /** @type {`0x${string}`} */ (value)
}

/**
 * @param {string | undefined} value
 * @param {string} command
 * @param {string} optionName
 */
function parseOptionalPositiveInteger(value, command, optionName) {
  if (value == null || value === '') return undefined

  const n = Number(value)
  if (!Number.isSafeInteger(n) || n < 1) {
    console.error(`Error: ${command}: ${optionName} must be a positive integer (got ${value})`)
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
        'service-url': { type: 'string' },
        'provider-address': { type: 'string' },
        'session-key': { type: 'string' },
        'customer-wallet': { type: 'string' },
        network: { type: 'string' },
        concurrency: { type: 'string' },
        retry: { type: 'boolean' },
        'dry-run': { type: 'boolean' },
      },
      allowPositionals: false,
      strict: true,
    }))
  } catch (err) {
    console.error(`Error: ${err instanceof Error ? err.message : String(err)}`)
    process.exit(1)
  }

  if (!values.target) {
    console.error('Error: commit: missing required --target <path> argument')
    process.exit(1)
  }

  await runCommit({
    sessionKey: parsePrivateKey(values['session-key'], 'commit', '--session-key'),
    dir: requireDir(values.dir, 'commit'),
    serviceUrl: requireServiceUrl(values['service-url']),
    providerAddress: parseAddress(values['provider-address'], '--provider-address'),
    customerWallet: parseAddress(values['customer-wallet'], '--customer-wallet'),
    chain: parseCommitNetwork(values.network),
    target: requireDir(values.target, 'commit'),
    concurrency: parseConcurrency(values.concurrency, 'commit'),
    retry: values.retry === true,
    dryRun: values['dry-run'] === true,
  })
}

async function removePieces(argv) {
  let values
  try {
    ;({ values } = parseArgs({
      args: argv,
      options: {
        dir: { type: 'string' },
        'service-url': { type: 'string' },
        'session-key': { type: 'string' },
        'customer-wallet': { type: 'string' },
        network: { type: 'string' },
        'ids-file': { type: 'string' },
        limit: { type: 'string' },
        'delay-ms': { type: 'string' },
        'batch-size': { type: 'string' },
      },
      allowPositionals: false,
      strict: true,
    }))
  } catch (err) {
    console.error(`Error: ${err instanceof Error ? err.message : String(err)}`)
    process.exit(1)
  }

  if (!values['ids-file']) {
    console.error('Error: remove-pieces: missing required --ids-file <path> argument')
    process.exit(1)
  }

  await runRemovePieces({
    dir: requireDir(values.dir, 'remove-pieces'),
    serviceUrl: requireServiceUrl(values['service-url']),
    customerWallet: parseAddress(values['customer-wallet'], '--customer-wallet'),
    sessionKey: parsePrivateKey(values['session-key'], 'remove-pieces', '--session-key'),
    chain: parseCommitNetwork(values.network),
    idsFile: values['ids-file'],
    limit: values.limit != null ? Number(values.limit) : undefined,
    delayMs: values['delay-ms'] != null ? Number(values['delay-ms']) : undefined,
    batchSize: values['batch-size'] != null ? Number(values['batch-size']) : undefined,
  })
}

async function aggregatePlan(argv) {
  let values
  try {
    ;({ values } = parseArgs({
      args: argv,
      options: {
        dir: { type: 'string' },
        'max-size-bytes': { type: 'string' },
      },
      allowPositionals: false,
      strict: true,
    }))
  } catch (err) {
    console.error(`Error: ${err instanceof Error ? err.message : String(err)}`)
    process.exit(1)
  }

  let maxSizeBytes
  try {
    maxSizeBytes = parsePositiveBigInt(values['max-size-bytes'], '--max-size-bytes')
  } catch (err) {
    console.error(`Error: aggregate-plan: ${err instanceof Error ? err.message : String(err)}`)
    process.exit(1)
  }

  await runAggregatePlan({
    dir: requireDir(values.dir, 'aggregate-plan'),
    maxSizeBytes,
  })
}

async function aggregateSubmit(argv) {
  let values
  try {
    ;({ values } = parseArgs({
      args: argv,
      options: {
        dir: { type: 'string' },
        'private-key': { type: 'string' },
        'service-url': { type: 'string' },
        'provider-address': { type: 'string' },
        network: { type: 'string' },
        'batch-size': { type: 'string' },
        'data-set-id': { type: 'string' },
        retry: { type: 'boolean' },
      },
      allowPositionals: false,
      strict: true,
    }))
  } catch (err) {
    console.error(`Error: ${err instanceof Error ? err.message : String(err)}`)
    process.exit(1)
  }

  await runAggregateSubmit({
    dir: requireDir(values.dir, 'aggregate-submit'),
    privateKey: parsePrivateKey(values['private-key'], 'aggregate-submit', '--private-key'),
    chain: parseCommitNetwork(values.network),
    serviceUrl: values['service-url'] ? requireServiceUrl(values['service-url']) : undefined,
    providerAddress: values['provider-address']
      ? parseAddress(values['provider-address'], '--provider-address')
      : undefined,
    batchSize: parseConcurrency(values['batch-size'], 'aggregate-submit'),
    dataSetId: parseOptionalPositiveInteger(values['data-set-id'], 'aggregate-submit', '--data-set-id'),
    retry: values.retry === true,
  })
}

async function reportDuplicates(argv) {
  let values
  try {
    ;({ values } = parseArgs({
      args: argv,
      options: {
        dir: { type: 'string' },
        'service-url': { type: 'string' },
        network: { type: 'string' },
        out: { type: 'string' },
      },
      allowPositionals: false,
      strict: true,
    }))
  } catch (err) {
    console.error(`Error: ${err instanceof Error ? err.message : String(err)}`)
    process.exit(1)
  }

  await runReportDuplicates({
    dir: requireDir(values.dir, 'report-duplicates'),
    serviceUrl: requireServiceUrl(values['service-url']),
    chain: parseCommitNetwork(values.network),
    outFile: values.out,
  })
}

/**
 * @param {string[]} argv
 */
async function verify(argv) {
  let values
  try {
    ;({ values } = parseArgs({
      args: argv,
      options: {
        db: { type: 'string' },
        dir: { type: 'string' },
        network: { type: 'string' },
        concurrency: { type: 'string' },
        'foc-api-url': { type: 'string' },
      },
      allowPositionals: false,
      strict: true,
    }))
  } catch (err) {
    console.error(`Error: ${err instanceof Error ? err.message : String(err)}`)
    process.exit(1)
  }

  await runVerify({
    db: requireDb(values.db, 'verify'),
    dir: requireDir(values.dir, 'verify'),
    chain: parseCommitNetwork(values.network),
    concurrency: parseConcurrency(values.concurrency, 'verify'),
    focApiUrl: values['foc-api-url'],
  })
}

const [, , sub, ...rest] = process.argv

async function main() {
  if (sub === '--help' || sub === '-h') {
    usage()
    return
  }

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
    case 'report-duplicates':
      await reportDuplicates(rest)
      break
    case 'remove-pieces':
      await removePieces(rest)
      break
    case 'verify':
      await verify(rest)
      break
    case 'aggregate-plan':
      await aggregatePlan(rest)
      break
    case 'aggregate-submit':
      await aggregateSubmit(rest)
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
