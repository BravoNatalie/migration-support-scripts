/**
 * `backup-helper prepare` — local pieceCID enrichment and CAR renaming.
 *
 * Uses tracking.db as the source of truth for completed shards, then validates
 * local `.car` file presence before renaming each completed shard to its
 * `<pieceCid>.car` filename. Missing piece CIDs are computed from the local
 * CAR bytes and persisted back into tracking.db.
 */

import { createReadStream } from 'node:fs'
import fs from 'node:fs/promises'

import { calculateFromIterable } from '@filoz/synapse-core/piece'
import pMap from 'p-map'

import { pathExists, renderProgressLine } from '../../utils.js'
import { pieceCarPath, shardCarPath } from '../lib/layout.mjs'
import { openTrackingDb } from '../lib/tracking-db.mjs'

const DEFAULT_PREPARE_CONCURRENCY = 16
const PREPARE_BATCH_SIZE = 1_000

/**
 * @typedef {object} PrepareWorkItem
 * @property {string} shardCid
 * @property {string | null} pieceCid
 * @property {string} carPath
 */

/**
 * @param {string} carPath
 */
async function calculateLocalPieceCid(carPath) {
  const stream = createReadStream(carPath)
  try {
    const pieceCid = await calculateFromIterable(stream)
    return pieceCid.toString()
  } finally {
    stream.destroy()
  }
}

/**
 * @param {string} carPath
 */
function aria2ControlPath(carPath) {
  return `${carPath}.aria2`
}

/**
 * Resolve the local CAR path for prepare, accepting either shard- or piece-named files.
 *
 * @param {string} dir
 * @param {string} shardCid
 * @param {string | null} pieceCid
 */
async function resolvePrepareCarPath(dir, shardCid, pieceCid) {
  const shardPath = shardCarPath(dir, shardCid)
  const hasShardPath = await pathExists(shardPath)

  if (!pieceCid) {
    return hasShardPath ? shardPath : null
  }

  const piecePath = pieceCarPath(dir, pieceCid)
  const hasPiecePath = await pathExists(piecePath)

  if (hasShardPath && hasPiecePath) {
    throw new Error(`prepare: both shard and piece CAR files exist for ${shardCid}`)
  }
  if (hasPiecePath) return piecePath
  if (hasShardPath) return shardPath
  return null
}

/**
 * Rename a completed shard CAR from `<shardCid>.car` to `<pieceCid>.car`, carrying any `.aria2` sidecar with it.
 *
 * @param {string} dir
 * @param {string} shardCid
 * @param {string} pieceCid
 * @param {string} carPath
 */
async function renameCarToPieceCid(dir, shardCid, pieceCid, carPath) {
  const targetPath = pieceCarPath(dir, pieceCid)
  if (carPath === targetPath) return targetPath

  if (await pathExists(targetPath)) {
    throw new Error(`prepare: target piece CAR already exists for ${shardCid}`)
  }

  await fs.rename(carPath, targetPath)

  const aria2SourceControlPath = aria2ControlPath(carPath)
  if (!(await pathExists(aria2SourceControlPath))) return targetPath

  try {
    await fs.rename(aria2SourceControlPath, aria2ControlPath(targetPath))
    return targetPath
  } catch (err) {
    try {
      // if renaming the control file fails, roll the CAR rename back
      await fs.rename(targetPath, carPath)
    } catch {}
    throw err
  }
}

/**
 * @param {object} summary
 * @param {number} summary.total
 * @param {number} summary.done
 * @param {number} summary.computed
 * @param {number} summary.failed
 */
function renderPrepareProgress(summary) {
  renderProgressLine(
    `prepare: total=${summary.total} done=${summary.done}/${summary.total} computed=${summary.computed} failed=${summary.failed}`,
  )
}

/**
 * @param {object} args
 * @param {string} args.dir
 * @param {number | undefined} args.concurrency
 */
export async function runPrepare({ dir, concurrency }) {
  const tracking = openTrackingDb(dir)
  const workerConcurrency = concurrency ?? DEFAULT_PREPARE_CONCURRENCY

  try {
    const total = tracking.getDownloadStats().complete
    if (total === 0) {
      console.error('prepare: nothing to do')
      return
    }

    const summary = {
      total,
      done: 0,
      computed: 0,
      failed: 0,
    }

    renderPrepareProgress(summary)

    let afterShardCid = ''

    while (true) {
      const candidates = tracking.listPrepareCandidates(PREPARE_BATCH_SIZE, afterShardCid)
      if (candidates.length === 0) break

      const results = await pMap(
        candidates,
        async (candidate) => {
          /** @type {PrepareWorkItem | null} */
          let workItem = null
          let computedPieceCid = false

          try {
            const carPath = await resolvePrepareCarPath(dir, candidate.shardCid, candidate.pieceCid)
            if (!carPath) {
              throw new Error(`prepare: missing shard file for ${candidate.shardCid}`)
            }

            workItem = {
              shardCid: candidate.shardCid,
              pieceCid: candidate.pieceCid,
              carPath,
            }

            let pieceCid = candidate.pieceCid
            if (!pieceCid) {
              pieceCid = await calculateLocalPieceCid(workItem.carPath)
              tracking.setPieceCid(workItem.shardCid, pieceCid)
              computedPieceCid = true
            } else {
              tracking.setRootShardsPieceCid(workItem.shardCid, pieceCid)
            }

            await renameCarToPieceCid(dir, workItem.shardCid, pieceCid, workItem.carPath)
            tracking.clearPrepareFailure(workItem.shardCid)
            return { computedPieceCid, failed: false }
          } catch (err) {
            const shardCid = workItem?.shardCid || candidate.shardCid
            tracking.markPrepareFailure({
              shardCid,
              error: String(err?.message || err),
              retryable: false,
            })
            return { computedPieceCid: false, failed: true }
          }
        },
        { concurrency: workerConcurrency },
      )

      for (const result of results) {
        if (result.computedPieceCid) summary.computed++
        if (result.failed) summary.failed++
        summary.done++
        renderPrepareProgress(summary)
      }

      afterShardCid = candidates[candidates.length - 1].shardCid
    }

    if (process.stdout.isTTY) process.stdout.write('\n')
    console.error(
      `prepare: done. total=${summary.total} done=${summary.done}/${summary.total} computed=${summary.computed} failed=${summary.failed}`,
    )
  } finally {
    tracking.close()
  }
}
