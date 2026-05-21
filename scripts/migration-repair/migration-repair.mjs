#!/usr/bin/env node
/**
 * Storacha migration JSON repair tool.
 *
 * Subcommands:
 *   scan      list shard entries missing pieceCID
 *   repiece   download missing shards, compute pieceCID, checkpoint to SQLite
 *   patch     apply checkpoint to migration JSON (fill pieceCID + rewrite sourceURL)
 *   validate  assert every shard has pieceCID + roundabout sourceURL + sizeBytes>0
 */

import { writeFile } from 'node:fs/promises'

import { runRepiece } from './repiece-runner.mjs'
import { openCheckpointDb } from './checkpoint-db.mjs'
import { jsonStateAdapter } from './adapters/json-state-adapter.mjs'
import { sqliteStateAdapter } from './adapters/sqlite-state-adapter.mjs'

const DEFAULT_CONCURRENCY = 8

const ADAPTERS = {
  json: jsonStateAdapter,
  sqlite: sqliteStateAdapter,
}

function parseArgs(argv) {
  const out = {}
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]
    if (!a.startsWith('--')) continue
    const key = a.slice(2)
    const next = argv[i + 1]
    if (next && !next.startsWith('--')) {
      out[key] = next
      i++
    } else {
      out[key] = true
    }
  }
  return out
}

function usage() {
  console.error(`Usage:
  migration-repair.mjs scan      --input <state.json|state.db> [--format json|sqlite] [--out missing.json]
  migration-repair.mjs repiece   --input <state.json|state.db> --db <checkpoint.sqlite> [--format json|sqlite] [--concurrency 8] [--limit N]
  migration-repair.mjs patch     --input <state.json|state.db> --db <checkpoint.sqlite> [--format json|sqlite] [--out patched.json]
  migration-repair.mjs validate  --input <state.json|state.db> [--format json|sqlite]
  migration-repair.mjs manual    --input <state.json|state.db> [--format json|sqlite] [--threshold-bytes 1073741824] [--out manual.json]`)
}

function resolveAdapter({ input, format }) {
  const explicit = format && String(format).toLowerCase()
  if (explicit) {
    const adapter = ADAPTERS[explicit]
    if (!adapter) {
      throw new Error(`unsupported --format: ${format}`)
    }
    return adapter
  }

  if (!input) {
    throw new Error('--input required')
  }

  const lowered = String(input).toLowerCase()
  if (lowered.endsWith('.json')) return jsonStateAdapter
  if (lowered.endsWith('.db') || lowered.endsWith('.sqlite') || lowered.endsWith('.sqlite3')) {
    return sqliteStateAdapter
  }

  throw new Error(`unable to infer state format from ${input}; pass --format json or --format sqlite`)
}

function parsePositiveInteger(value, name) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed) || parsed < 1) {
    throw new Error(`invalid ${name}`)
  }
  return parsed
}

async function scan(adapter, { input, out }) {
  if (!input) throw new Error('--input required')
  const report = await adapter.scan(input, { includeMissing: Boolean(out) })
  if (out) {
    const serializableMissing = (report.missing ?? []).map((item) => ({
      ...item,
      sizeBytes: Number(item.sizeBytes),
    }))
    await writeFile(out, JSON.stringify(serializableMissing, null, 2))
    console.error(`Wrote ${out}`)
    return
  }
  console.log(
    JSON.stringify(
      {
        total: report.totalShardEntries,
        withPiece: report.withPieceCID,
        missing: report.uniqueMissingCids,
        totalBytes: Number(report.totalBytesToDownload),
      },
      null,
      2,
    ),
  )
}

async function repiece(adapter, opts) {
  const { input, db: dbPath, concurrency, limit: limitArg } = opts
  if (!input || !dbPath) throw new Error('--input and --db required')

  const maxConcurrency = concurrency ? parsePositiveInteger(concurrency, '--concurrency') : DEFAULT_CONCURRENCY
  const maxItems = limitArg != null ? parsePositiveInteger(limitArg, '--limit') : Infinity

  const checkpoint = openCheckpointDb(dbPath)
  try {
    const summary = await adapter.scan(input, { includeMissing: false })
    await runRepiece({
      checkpoint,
      candidates: adapter.iterateRepairCandidates(input),
      concurrency: maxConcurrency,
      limit: maxItems,
      summary: {
        totalCandidates: summary.uniqueMissingCids,
        totalBytes: summary.totalBytesToDownload,
      },
    })
  } finally {
    checkpoint.close()
  }
}

async function patch(adapter, { input, db: dbPath, out }) {
  if (!input || !dbPath) throw new Error('--input and --db required')
  const checkpoint = openCheckpointDb(dbPath)
  try {
    const report = await adapter.patch(input, checkpoint, { out })
    console.error(`Patched: ${report.patched}`)
    console.error(`Moved shardsToStore → shards: ${report.movedToShards}`)
    console.error(`Still missing: ${report.stillMissing}`)
    if (report.outputPath) {
      console.error(`Wrote ${report.outputPath}`)
    } else if (report.inPlace) {
      console.error(`Patched in place: ${input}`)
    }
    if (report.stillMissing > 0) process.exitCode = 2
  } finally {
    checkpoint.close()
  }
}

async function validate(adapter, { input }) {
  if (!input) throw new Error('--input required')
  const report = await adapter.validate(input)
  console.log(JSON.stringify(report, null, 2))
  if (!report.migratable) process.exitCode = 1
}

async function manual(adapter, { input, 'threshold-bytes': thresholdArg, out }) {
  if (!input) throw new Error('--input required')
  const threshold = thresholdArg != null ? parsePositiveInteger(thresholdArg, '--threshold-bytes') : 1024 ** 3
  const report = await adapter.manual(input, { thresholdBytes: threshold })

  console.error(
    `skippedUploads:      ${report.skippedUploadsCount} (known size: ${report.skippedUploadsBytes} bytes — usually 0, schema has only CIDs)`,
  )
  console.error(
    `largeRoots (>= ${(threshold / 1024 ** 3).toFixed(2)} GiB): ${report.largeRootsCount} (${(report.largeRootsBytes / 1024 ** 3).toFixed(2)} GiB)`,
  )

  if (out) {
    await writeFile(out, JSON.stringify(report, null, 2))
    console.error(`Wrote ${out}`)
  } else {
    console.log(JSON.stringify(report, null, 2))
  }
}

const [, , sub, ...rest] = process.argv
const opts = parseArgs(rest)
try {
  const adapter = sub ? resolveAdapter(opts) : null
  switch (sub) {
    case 'scan':
      await scan(adapter, opts)
      break
    case 'repiece':
      await repiece(adapter, opts)
      break
    case 'patch':
      await patch(adapter, opts)
      break
    case 'validate':
      await validate(adapter, opts)
      break
    case 'manual':
      await manual(adapter, opts)
      break
    default:
      usage()
      process.exit(1)
  }
} catch (err) {
  console.error(`Error: ${err?.message || err}`)
  process.exit(1)
}
