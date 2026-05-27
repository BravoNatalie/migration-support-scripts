#!/usr/bin/env node

import fs from 'node:fs'
import path from 'node:path'
import process from 'node:process'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { parseArgs } from 'node:util'
import { deserializeState } from '@storacha/filecoin-pin-migration'
import { z } from 'zod'
import { pathExists, withRunDirLock, writeFileAtomic } from '../utils.js'

const ScriptArgsSchema = z.object({
  stateFile: z.string().min(1),
  downloadDir: z.string().min(1),
  space: z.string().min(1).optional(),
})

/**
 * @typedef {'shards' | 'shardsToStore' | 'both'} DownloadRecordSource
 */

/**
 * @typedef {object} DownloadRecord
 * @property {import('@storacha/filecoin-pin-migration/types').SpaceDID} spaceDID
 * @property {string} shardCid
 * @property {string | undefined} pieceCID
 * @property {bigint} sizeBytes
 * @property {string} sourceURL
 * @property {string} relativePath
 * @property {string[]} roots
 * @property {DownloadRecordSource} from
 */

async function main() {
  const args = parseScriptArgs()
  const result = await prepareCarsDownload({
    stateFile: path.resolve(args.stateFile),
    downloadDir: path.resolve(args.downloadDir),
    spaceDID: args.space,
  })

  console.log(`\nPrepared CAR downloads for ${result.spaceDID}\n`)
  console.log(`Download dir: ${result.downloadDir}`)
  console.log(`Unique shard files: ${result.summary.uniqueShardCount}`)
  console.log(`Already present: ${result.summary.alreadyPresentCount}`)
  console.log(`Queued for download: ${result.summary.queuedDownloadCount}`)
  console.log(`Skipped uploads: ${result.summary.skippedUploadCount}`)
  console.log(`Aria2 input: ${result.aria2Path}`)
  const launcherPath = path.resolve(path.dirname(fileURLToPath(import.meta.url)), 'run-cars-download.sh')
  console.log(`\n=========================================`)
  console.log(`Run the following command to start: \n\n  bash ${launcherPath} ${result.downloadDir}`)
  console.log(`\n=========================================\n`)
}

function parseScriptArgs() {
  let values
  try {
    ;({ values } = parseArgs({
      args: process.argv.slice(2),
      options: {
        'state-file': { type: 'string' },
        'download-dir': { type: 'string' },
        space: { type: 'string' },
      },
      allowPositionals: false,
      strict: true,
    }))
  } catch (error) {
    console.error(`Error: ${error instanceof Error ? error.message : String(error)}`)
    process.exit(1)
  }

  const parsed = ScriptArgsSchema.safeParse({
    stateFile: values['state-file'],
    downloadDir: values['download-dir'],
    space: values.space,
  })
  if (!parsed.success) {
    const issue = parsed.error.issues[0]
    const optionName =
      issue?.path[0] === 'stateFile' ? '--state-file' : issue?.path[0] === 'downloadDir' ? '--download-dir' : '--space'
    console.error(`Error: invalid or missing option "${optionName}"`)
    process.exit(1)
  }

  return parsed.data
}

/**
 * @param {object} args
 * @param {string} args.stateFile
 * @param {string} args.downloadDir
 * @param {string | undefined} [args.spaceDID]
 */
export async function prepareCarsDownload({ stateFile, downloadDir, spaceDID }) {
  if (!path.isAbsolute(downloadDir)) {
    throw new Error(`prepareCarsDownload: downloadDir must be an absolute path, got: ${downloadDir}`)
  }
  return withRunDirLock(downloadDir, 'prepareCarsDownload', async () => {
    const state = await loadStateFile(stateFile)
    const resolvedSpaceDID = selectSpaceDID(state, spaceDID)
    const inventory = state.spacesInventories[resolvedSpaceDID]
    if (!inventory) {
      throw new Error(`prepareCarsDownload: state is missing inventory for ${resolvedSpaceDID}`)
    }

    const records = buildDownloadRecords({
      spaceDID: resolvedSpaceDID,
      inventory,
    })

    const carsDir = path.join(downloadDir, 'cars')
    const conflictsDir = path.join(carsDir, '.conflicts')
    await fs.promises.mkdir(carsDir, { recursive: true })

    /** @type {Array<{ shardCid: string, fromPath: string, toPath: string }>} */
    const conflicts = []
    /** @type {DownloadRecord[]} */
    const queuedRecords = []
    let alreadyPresentCount = 0
    let queuedBytes = 0n

    for (const record of records) {
      const filePath = path.join(downloadDir, record.relativePath)
      const status = await inspectTargetFile(filePath, record.sizeBytes)

      if (status.type === 'complete') {
        alreadyPresentCount += 1
        continue
      }

      if (status.type === 'conflict') {
        const controlFile = `${filePath}.aria2`
        if (await pathExists(controlFile)) {
          // Partial download from a prior aria2 run — leave it in place so aria2 can resume.
          queuedRecords.push(record)
          queuedBytes += record.sizeBytes
          continue
        }

        const conflictPath = await allocateConflictPath(conflictsDir, record.shardCid)
        await fs.promises.mkdir(path.dirname(conflictPath), { recursive: true })
        await fs.promises.rename(filePath, conflictPath)

        conflicts.push({
          shardCid: record.shardCid,
          fromPath: filePath,
          toPath: conflictPath,
        })
      }

      queuedRecords.push(record)
      queuedBytes += record.sizeBytes
    }

    const summary = {
      spaceDID: resolvedSpaceDID,
      uniqueShardCount: records.length,
      alreadyPresentCount,
      queuedDownloadCount: queuedRecords.length,
      duplicateRootReferencesCollapsed: countRootReferences(records) - records.length,
      totalBytes: sumRecordBytes(records).toString(10),
      queuedBytes: queuedBytes.toString(10),
      shardsCount: inventory.shards.length,
      shardsToStoreCount: inventory.shardsToStore.length,
      skippedUploadCount: inventory.skippedUploads.length,
    }

    const downloadNdjsonPath = path.join(downloadDir, 'download.ndjson')
    const aria2Path = path.join(downloadDir, 'download.aria2')
    const summaryPath = path.join(downloadDir, 'summary.json')
    const conflictsPath = path.join(downloadDir, 'conflicts.ndjson')

    await writeFileAtomic(downloadNdjsonPath, serializeDownloadRecords(records))
    await writeFileAtomic(aria2Path, serializeAria2Input(downloadDir, queuedRecords))
    await writeFileAtomic(summaryPath, `${JSON.stringify(summary, null, 2)}\n`)
    await writeFileAtomic(conflictsPath, serializeConflicts(conflicts))

    return {
      downloadDir,
      spaceDID: resolvedSpaceDID,
      aria2Path,
      summary,
    }
  })
}

/**
 * @param {object} args
 * @param {import('@storacha/filecoin-pin-migration/types').SpaceDID} args.spaceDID
 * @param {import('@storacha/filecoin-pin-migration/types').SpaceInventory} args.inventory
 * @returns {DownloadRecord[]}
 */
export function buildDownloadRecords({ spaceDID, inventory }) {
  /** @type {Map<string, {
   *   spaceDID: import('@storacha/filecoin-pin-migration/types').SpaceDID
   *   shardCid: string
   *   pieceCID: string | undefined
   *   sizeBytes: bigint
   *   sourceURL: string
   *   relativePath: string
   *   roots: Set<string>
   *   seenFromShards: boolean
   *   seenFromStore: boolean
   * }>} */
  const byShard = new Map()

  /**
   * @param {import('@storacha/filecoin-pin-migration/types').ResolvedShard | import('@storacha/filecoin-pin-migration/types').StoreShard} shard
   * @param {'shards' | 'shardsToStore'} from
   */
  const addShard = (shard, from) => {
    const relativePath = path.join('cars', `${shard.cid}.car`)
    const existing = byShard.get(shard.cid)
    if (!existing) {
      byShard.set(shard.cid, {
        spaceDID,
        shardCid: shard.cid,
        pieceCID: 'pieceCID' in shard && typeof shard.pieceCID === 'string' ? shard.pieceCID : undefined,
        sizeBytes: shard.sizeBytes,
        sourceURL: shard.sourceURL,
        relativePath,
        roots: new Set([shard.root]),
        seenFromShards: from === 'shards',
        seenFromStore: from === 'shardsToStore',
      })
      return
    }

    if (existing.sizeBytes !== shard.sizeBytes) {
      throw new Error(`prepareCarsDownload: shard ${shard.cid} has inconsistent sizeBytes values`)
    }
    existing.sourceURL = selectPreferredSourceURL(existing.sourceURL, shard.sourceURL)

    const incomingPieceCID = 'pieceCID' in shard && typeof shard.pieceCID === 'string' ? shard.pieceCID : undefined
    if (existing.pieceCID && incomingPieceCID && existing.pieceCID !== incomingPieceCID) {
      throw new Error(`prepareCarsDownload: shard ${shard.cid} has inconsistent pieceCID values`)
    }
    if (!existing.pieceCID && incomingPieceCID) {
      existing.pieceCID = incomingPieceCID
    }

    existing.roots.add(shard.root)
    if (from === 'shards') existing.seenFromShards = true
    if (from === 'shardsToStore') existing.seenFromStore = true
  }

  for (const shard of inventory.shards) addShard(shard, 'shards')
  for (const shard of inventory.shardsToStore) addShard(shard, 'shardsToStore')

  return [...byShard.values()]
    .sort((a, b) => a.shardCid.localeCompare(b.shardCid))
    .map((record) => ({
      spaceDID: record.spaceDID,
      shardCid: record.shardCid,
      pieceCID: record.pieceCID,
      sizeBytes: record.sizeBytes,
      sourceURL: record.sourceURL,
      relativePath: record.relativePath,
      roots: [...record.roots].sort(),
      from: record.seenFromShards && record.seenFromStore ? 'both' : record.seenFromShards ? 'shards' : 'shardsToStore',
    }))
}

/**
 * @param {string} existingURL
 * @param {string} incomingURL
 */
function selectPreferredSourceURL(existingURL, incomingURL) {
  if (existingURL === incomingURL) return existingURL
  if (isDirectR2URL(incomingURL) && !isDirectR2URL(existingURL)) return incomingURL
  return existingURL
}

/**
 * @param {string} sourceURL
 */
function isDirectR2URL(sourceURL) {
  try {
    return new URL(sourceURL).hostname.endsWith('.r2.w3s.link')
  } catch {
    return false
  }
}

/**
 * @param {DownloadRecord[]} records
 */
export function serializeDownloadRecords(records) {
  return records
    .map((record) =>
      JSON.stringify({
        spaceDID: record.spaceDID,
        shardCid: record.shardCid,
        ...(record.pieceCID ? { pieceCID: record.pieceCID } : {}),
        sizeBytes: record.sizeBytes.toString(10),
        sourceURL: record.sourceURL,
        relativePath: record.relativePath,
        roots: record.roots,
        from: record.from,
      }),
    )
    .join('\n')
    .concat(records.length > 0 ? '\n' : '')
}

/**
 * @param {string} downloadDir
 * @param {DownloadRecord[]} records
 */
export function serializeAria2Input(downloadDir, records) {
  return records
    .map((record) => {
      const absolutePath = path.join(downloadDir, record.relativePath)
      return `${record.sourceURL}\n dir=${path.dirname(absolutePath)}\n out=${path.basename(absolutePath)}`
    })
    .join('\n')
    .concat(records.length > 0 ? '\n' : '')
}

/**
 * @param {Array<{ shardCid: string, fromPath: string, toPath: string }>} conflicts
 */
export function serializeConflicts(conflicts) {
  return conflicts
    .map((conflict) => JSON.stringify(conflict))
    .join('\n')
    .concat(conflicts.length > 0 ? '\n' : '')
}

/**
 * @param {string} stateFile
 */
async function loadStateFile(stateFile) {
  let raw
  try {
    raw = await fs.promises.readFile(stateFile, 'utf8')
  } catch (error) {
    throw new Error(
      `prepareCarsDownload: failed to read state file ${stateFile}: ${
        error instanceof Error ? error.message : String(error)
      }`,
    )
  }

  try {
    return deserializeState(JSON.parse(raw))
  } catch (error) {
    throw new Error(
      `prepareCarsDownload: failed to parse state file ${stateFile}: ${
        error instanceof Error ? error.message : String(error)
      }`,
    )
  }
}

/**
 * @param {import('@storacha/filecoin-pin-migration/types').MigrationState} state
 * @param {string | undefined} requestedSpaceDID
 */
export function selectSpaceDID(state, requestedSpaceDID) {
  if (requestedSpaceDID) {
    if (!(requestedSpaceDID in state.spaces)) {
      throw new Error(`prepareCarsDownload: state is missing requested space ${requestedSpaceDID}`)
    }
    return /** @type {import('@storacha/filecoin-pin-migration/types').SpaceDID} */ (requestedSpaceDID)
  }

  const inventoryDIDs = Object.keys(state.spacesInventories)
  if (inventoryDIDs.length === 1) {
    return /** @type {import('@storacha/filecoin-pin-migration/types').SpaceDID} */ (inventoryDIDs[0])
  }

  throw new Error(
    `prepareCarsDownload: --space is required when the state contains ${inventoryDIDs.length} space inventories`,
  )
}

/**
 * @param {DownloadRecord[]} records
 */
function sumRecordBytes(records) {
  let total = 0n
  for (const record of records) total += record.sizeBytes
  return total
}

/**
 * @param {DownloadRecord[]} records
 */
function countRootReferences(records) {
  let total = 0
  for (const record of records) total += record.roots.length
  return total
}

/**
 * @param {string} filePath
 * @param {bigint} expectedSize
 * @returns {Promise<{ type: 'missing' | 'complete' | 'conflict' }>}
 */
async function inspectTargetFile(filePath, expectedSize) {
  try {
    const stats = await fs.promises.stat(filePath, { bigint: true })
    if (!stats.isFile()) return { type: 'conflict' }
    return stats.size === expectedSize ? { type: 'complete' } : { type: 'conflict' }
  } catch (error) {
    if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') {
      return { type: 'missing' }
    }
    throw error
  }
}

/**
 * @param {string} conflictsDir
 * @param {string} shardCid
 */
async function allocateConflictPath(conflictsDir, shardCid) {
  await fs.promises.mkdir(conflictsDir, { recursive: true })
  for (let index = 1; index <= 100; index++) {
    const candidate = path.join(conflictsDir, `${shardCid}.${index}.car`)
    if (!(await pathExists(candidate))) {
      return candidate
    }
  }
  throw new Error(`prepareCarsDownload: could not allocate a conflict path for shard ${shardCid} after 100 attempts`)
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error('Error: failed to prepare CAR downloads')
    console.error(error instanceof Error ? (error.stack ?? error.message) : String(error))
    process.exit(1)
  })
}
