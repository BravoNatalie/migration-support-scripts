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

import { readFile, rename, writeFile } from 'node:fs/promises'
import { DatabaseSync } from 'node:sqlite'
import { calculateFromIterable } from '@filoz/synapse-core/piece'

const DEFAULT_CONCURRENCY = 8

function parseArgs(argv) {
  const out = {}
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]
    if (a.startsWith('--')) {
      const key = a.slice(2)
      const next = argv[i + 1]
      if (next && !next.startsWith('--')) {
        out[key] = next
        i++
      } else {
        out[key] = true
      }
    }
  }
  return out
}

function usage() {
  console.error(`Usage:
  migration-repair.mjs scan      --input <migration.json> [--out missing.json]
  migration-repair.mjs repiece   --input <migration.json> --db <checkpoint.sqlite> [--concurrency 8]
  migration-repair.mjs patch     --input <migration.json> --db <checkpoint.sqlite> --out <patched.json>
  migration-repair.mjs validate  --input <migration.json>
  migration-repair.mjs manual    --input <migration.json> [--threshold-bytes 1073741824] [--out manual.json]`)
}

function* walkShards(node) {
  if (node === null || typeof node !== 'object') return
  if (Array.isArray(node)) {
    for (const v of node) yield* walkShards(v)
    return
  }
  if (typeof node.cid === 'string' && (typeof node.sizeBytes === 'string' || typeof node.sizeBytes === 'number')) {
    yield node
  }
  for (const k of Object.keys(node)) {
    const v = node[k]
    if (v && typeof v === 'object') yield* walkShards(v)
  }
}

async function loadJson(path) {
  const buf = await readFile(path)
  return JSON.parse(buf.toString('utf8'))
}

function openDb(path) {
  const db = new DatabaseSync(path)
  db.exec(`
    CREATE TABLE IF NOT EXISTS pieces (
      cid TEXT PRIMARY KEY,
      piece_cid TEXT NOT NULL,
      size_bytes INTEGER NOT NULL,
      computed_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS failures (
      cid TEXT PRIMARY KEY,
      source_url TEXT NOT NULL,
      error TEXT NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 1,
      last_attempt INTEGER NOT NULL
    );
  `)
  return db
}

async function scan({ input, out }) {
  if (!input) throw new Error('--input required')
  console.error(`Loading ${input}...`)
  const json = await loadJson(input)
  const missing = []
  let total = 0
  let withPiece = 0
  for (const entry of walkShards(json)) {
    total++
    if (entry.pieceCID) {
      withPiece++
      continue
    }
    if (!entry.sourceURL) continue
    missing.push({
      cid: entry.cid,
      sizeBytes: Number(entry.sizeBytes),
      sourceURL: entry.sourceURL,
      root: entry.root,
    })
  }
  const byCid = new Map()
  for (const m of missing) {
    if (!byCid.has(m.cid)) byCid.set(m.cid, m)
  }
  const unique = [...byCid.values()]
  const totalBytes = unique.reduce((s, m) => s + m.sizeBytes, 0)
  console.error(`Total shard entries: ${total}`)
  console.error(`With pieceCID:       ${withPiece}`)
  console.error(`Missing pieceCID:    ${missing.length} (unique by cid: ${unique.length})`)
  console.error(`Bytes to download:   ${totalBytes} (~${(totalBytes / 1024 ** 3).toFixed(2)} GiB)`)
  if (out) {
    await writeFile(out, JSON.stringify(unique, null, 2))
    console.error(`Wrote ${out}`)
  } else {
    console.log(JSON.stringify({ total, withPiece, missing: unique.length, totalBytes }, null, 2))
  }
}

async function streamPieceCid(url, signal) {
  const res = await fetch(url, { signal, redirect: 'follow' })
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`)
  if (!res.body) throw new Error('no response body')
  async function* iter() {
    for await (const chunk of res.body) yield chunk
  }
  return calculateFromIterable(iter())
}

async function repiece({ input, db: dbPath, concurrency, limit: limitArg }) {
  if (!input || !dbPath) throw new Error('--input and --db required')
  const limit = Number(concurrency || DEFAULT_CONCURRENCY)
  if (!Number.isFinite(limit) || limit < 1) throw new Error('invalid --concurrency')
  const maxItems = limitArg != null ? Number(limitArg) : Infinity

  console.error(`Loading ${input}...`)
  const json = await loadJson(input)
  const db = openDb(dbPath)
  const insertOk = db.prepare(
    'INSERT OR REPLACE INTO pieces (cid, piece_cid, size_bytes, computed_at) VALUES (?, ?, ?, ?)',
  )
  const insertFail = db.prepare(`
    INSERT INTO failures (cid, source_url, error, attempts, last_attempt) VALUES (?, ?, ?, 1, ?)
    ON CONFLICT(cid) DO UPDATE SET attempts = attempts + 1, error = excluded.error, last_attempt = excluded.last_attempt
  `)
  const hasPiece = db.prepare('SELECT 1 FROM pieces WHERE cid = ?')

  const queue = []
  const seen = new Set()
  for (const entry of walkShards(json)) {
    if (entry.pieceCID) continue
    if (!entry.sourceURL) continue
    if (seen.has(entry.cid)) continue
    seen.add(entry.cid)
    if (hasPiece.get(entry.cid)) continue
    queue.push({ cid: entry.cid, sourceURL: entry.sourceURL, sizeBytes: Number(entry.sizeBytes) })
  }
  queue.sort((a, b) => a.sizeBytes - b.sizeBytes)
  if (Number.isFinite(maxItems)) queue.length = Math.min(queue.length, maxItems)
  console.error(`Queue: ${queue.length} shards to compute (concurrency=${limit})`)
  if (queue.length === 0) {
    console.error('Nothing to do.')
    db.close()
    return
  }

  let done = 0
  let failed = 0
  let bytesDone = 0
  const totalBytes = queue.reduce((s, e) => s + e.sizeBytes, 0)
  const startedAt = Date.now()

  const tick = () => {
    const elapsed = (Date.now() - startedAt) / 1000
    const mbps = bytesDone / 1024 / 1024 / Math.max(elapsed, 0.001)
    process.stderr.write(
      `\r[${done + failed}/${queue.length}] ok=${done} fail=${failed} ${(bytesDone / 1024 ** 3).toFixed(2)}/${(totalBytes / 1024 ** 3).toFixed(2)} GiB @ ${mbps.toFixed(1)} MiB/s   `,
    )
  }

  let idx = 0
  async function worker() {
    while (idx < queue.length) {
      const myIdx = idx++
      const item = queue[myIdx]
      try {
        const pieceCid = await streamPieceCid(item.sourceURL)
        insertOk.run(item.cid, pieceCid.toString(), item.sizeBytes, Date.now())
        done++
        bytesDone += item.sizeBytes
      } catch (err) {
        insertFail.run(item.cid, item.sourceURL, String(err?.message || err), Date.now())
        failed++
      }
      if ((done + failed) % 10 === 0) tick()
    }
  }

  await Promise.all(Array.from({ length: limit }, worker))
  tick()
  process.stderr.write('\n')
  console.error(`Done. ok=${done} fail=${failed}`)
  db.close()
}

async function patch({ input, db: dbPath, out }) {
  if (!input || !dbPath || !out) throw new Error('--input, --db, --out required')
  console.error(`Loading ${input}...`)
  const json = await loadJson(input)
  const db = openDb(dbPath)
  const lookup = db.prepare('SELECT piece_cid FROM pieces WHERE cid = ?')

  let patched = 0
  let stillMissing = 0
  let movedToShards = 0

  // Walk each space inventory, move shardsToStore entries with known pieceCID into shards.
  const inventories = json?.spacesInventories
  if (inventories && typeof inventories === 'object') {
    for (const inv of Object.values(inventories)) {
      if (!inv || !Array.isArray(inv.shardsToStore)) continue
      if (!Array.isArray(inv.shards)) inv.shards = []
      const keep = []
      for (const entry of inv.shardsToStore) {
        const row = lookup.get(entry.cid)
        if (!row) {
          stillMissing++
          keep.push(entry)
          continue
        }
        entry.pieceCID = row.piece_cid
        // sourceURL stays as carpark location URL — SP pulls directly via Pull flow.
        inv.shards.push(entry)
        patched++
        movedToShards++
      }
      inv.shardsToStore = keep
    }
  } else {
    console.error('Warning: no spacesInventories found, falling back to generic walk')
    for (const entry of walkShards(json)) {
      if (entry.pieceCID) continue
      const row = lookup.get(entry.cid)
      if (!row) {
        stillMissing++
        continue
      }
      entry.pieceCID = row.piece_cid
      patched++
    }
  }

  db.close()

  const tmp = `${out}.tmp`
  await writeFile(tmp, JSON.stringify(json, null, 2))
  await rename(tmp, out)
  console.error(`Patched: ${patched}`)
  console.error(`Moved shardsToStore → shards: ${movedToShards}`)
  console.error(`Still missing: ${stillMissing}`)
  console.error(`Wrote ${out}`)
  if (stillMissing > 0) process.exitCode = 2
}

async function validate({ input }) {
  if (!input) throw new Error('--input required')
  console.error(`Loading ${input}...`)
  const json = await loadJson(input)
  let total = 0
  let missingPiece = 0
  let missingSourceURL = 0
  let badSize = 0
  const samples = { missingPiece: [], missingSourceURL: [], badSize: [] }
  for (const entry of walkShards(json)) {
    total++
    if (!entry.pieceCID) {
      missingPiece++
      if (samples.missingPiece.length < 3) samples.missingPiece.push(entry.cid)
    }
    const url = entry.sourceURL || ''
    if (!url) {
      missingSourceURL++
      if (samples.missingSourceURL.length < 3) samples.missingSourceURL.push(entry.cid)
    }
    const size = Number(entry.sizeBytes)
    if (!Number.isFinite(size) || size <= 0) {
      badSize++
      if (samples.badSize.length < 3) samples.badSize.push({ cid: entry.cid, sizeBytes: entry.sizeBytes })
    }
  }
  let shardsToStoreRemaining = 0
  let shardsCount = 0
  const inventories = json?.spacesInventories
  if (inventories && typeof inventories === 'object') {
    for (const inv of Object.values(inventories)) {
      if (Array.isArray(inv?.shardsToStore)) shardsToStoreRemaining += inv.shardsToStore.length
      if (Array.isArray(inv?.shards)) shardsCount += inv.shards.length
    }
  }
  const report = {
    total,
    missingPiece,
    missingSourceURL,
    badSize,
    shardsCount,
    shardsToStoreRemaining,
    samples,
    migratable: missingPiece === 0 && missingSourceURL === 0 && badSize === 0 && shardsToStoreRemaining === 0,
  }
  console.log(JSON.stringify(report, null, 2))
  if (!report.migratable) process.exitCode = 1
}

async function manual({ input, 'threshold-bytes': thresholdArg, out }) {
  if (!input) throw new Error('--input required')
  const threshold = Number(thresholdArg ?? 1024 ** 3) // 1 GiB default
  console.error(`Loading ${input}...`)
  const json = await loadJson(input)

  const skipped = []
  const rootSizes = new Map() // root -> {size, shards:[{cid, sourceURL, sizeBytes}]}

  const inventories = json?.spacesInventories
  if (!inventories) throw new Error('no spacesInventories in JSON')

  for (const [space, inv] of Object.entries(inventories)) {
    for (const root of inv.skippedUploads ?? []) {
      // skippedUploads is an array of strings (root CIDs). No size info available.
      skipped.push({
        space,
        root: typeof root === 'string' ? root : root.root,
        sizeBytes: typeof root === 'object' && root?.sizeBytes ? Number(root.sizeBytes) : null,
        gatewayURL: `https://trustless-gateway.link/ipfs/${typeof root === 'string' ? root : root.root}?format=car`,
      })
    }
    for (const arr of [inv.shards ?? [], inv.shardsToStore ?? []]) {
      for (const s of arr) {
        const r = s.root
        if (!r) continue
        if (!rootSizes.has(r)) rootSizes.set(r, { space, size: 0, shards: [] })
        const acc = rootSizes.get(r)
        acc.size += Number(s.sizeBytes) || 0
        acc.shards.push({ cid: s.cid, sourceURL: s.sourceURL, sizeBytes: Number(s.sizeBytes) || 0 })
      }
    }
  }

  const largeRoots = []
  for (const [root, info] of rootSizes) {
    if (info.size >= threshold) {
      largeRoots.push({
        root,
        space: info.space,
        sizeBytes: info.size,
        shardCount: info.shards.length,
        shards: info.shards,
      })
    }
  }
  largeRoots.sort((a, b) => b.sizeBytes - a.sizeBytes)

  const report = {
    thresholdBytes: threshold,
    skippedUploadsCount: skipped.length,
    skippedUploadsBytes: skipped.reduce((s, x) => s + (x.sizeBytes || 0), 0),
    largeRootsCount: largeRoots.length,
    largeRootsBytes: largeRoots.reduce((s, x) => s + x.sizeBytes, 0),
    skippedUploads: skipped,
    largeRoots,
  }

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
  switch (sub) {
    case 'scan':
      await scan(opts)
      break
    case 'repiece':
      await repiece(opts)
      break
    case 'patch':
      await patch(opts)
      break
    case 'validate':
      await validate(opts)
      break
    case 'manual':
      await manual(opts)
      break
    default:
      usage()
      process.exit(1)
  }
} catch (err) {
  console.error(`Error: ${err?.message || err}`)
  process.exit(1)
}
