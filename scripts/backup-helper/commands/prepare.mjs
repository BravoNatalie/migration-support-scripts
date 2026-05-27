/**
 * `backup-helper prepare` — local pieceCID enrichment and sidecar generation.
 *
 * Uses tracking.db as the source of truth for completed shards, then validates
 * local `.car` file presence before routing each shard into one of two lanes:
 * - sidecar lane: completed shard with an existing piece_cid
 * - piece lane: completed shard missing piece_cid, which must be computed from
 *   the local CAR bytes before writing the sidecar
 */

import { createReadStream } from 'node:fs'
import fs from 'node:fs/promises'

import { calculateFromIterable } from '@filoz/synapse-core/piece'
import pMap from 'p-map'

import { pathExists, renderProgressLine } from '../../utils.js'
import { pieceCarPath, pieceJsonPath, shardCarPath } from '../lib/layout.mjs'
import { openTrackingDb } from '../lib/tracking-db.mjs'

const DEFAULT_PREPARE_CONCURRENCY = 8

/**
 * @typedef {object} PrepareWorkItem
 * @property {string} shardCid
 * @property {string | null} pieceCid
 * @property {number} sizeBytes
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
 * @param {string} dir
 * @param {string} shardCid
 * @param {string} pieceCid
 * @param {number} sizeBytes
 * @param {string[]} rootCids
 */
async function writePieceSidecar(dir, shardCid, pieceCid, sizeBytes, rootCids) {
  const sidecarPath = pieceJsonPath(dir, pieceCid)
  if (await pathExists(sidecarPath)) return

  const content = JSON.stringify(
    {
      shardCid,
      pieceCid,
      sizeBytes,
      rootCids,
    },
    null,
    2,
  )

  await fs.writeFile(sidecarPath, `${content}\n`)
}

/**
 * @param {object} summary
 * @param {number} summary.totalEligible
 * @param {number} summary.sidecarTotal
 * @param {number} summary.pieceTotal
 * @param {number} summary.sidecarDone
 * @param {number} summary.pieceDone
 * @param {number} summary.failed
 */
function renderPrepareProgress(summary) {
  renderProgressLine(
    `prepare: eligible=${summary.totalEligible} sidecar=${summary.sidecarDone}/${summary.sidecarTotal} piece=${summary.pieceDone}/${summary.pieceTotal} failed=${summary.failed}`,
  )
}

/**
 * @param {object} args
 * @param {string} args.dir
 * @param {number | undefined} args.concurrency
 */
export async function runPrepare({ dir, concurrency }) {
  const tracking = openTrackingDb(dir)
  const laneConcurrency = concurrency ?? DEFAULT_PREPARE_CONCURRENCY

  try {
    const completedCount = tracking.getDownloadStats().complete
    const candidates = tracking.listPrepareCandidates(completedCount)

    /** @type {PrepareWorkItem[]} */
    const sidecarLane = []
    /** @type {PrepareWorkItem[]} */
    const pieceLane = []
    let failed = 0

    for (const candidate of candidates) {
      let carPath
      try {
        carPath = await resolvePrepareCarPath(dir, candidate.shardCid, candidate.pieceCid)
      } catch (err) {
        tracking.markPrepareFailure({
          shardCid: candidate.shardCid,
          error: String(err?.message || err),
          retryable: false,
        })
        failed++
        continue
      }

      if (!carPath) {
        tracking.markPrepareFailure({
          shardCid: candidate.shardCid,
          error: `prepare: missing shard file for ${candidate.shardCid}`,
          retryable: false,
        })
        failed++
        continue
      }

      const workItem = {
        shardCid: candidate.shardCid,
        pieceCid: candidate.pieceCid,
        sizeBytes: candidate.sizeBytes,
        carPath,
      }

      if (candidate.pieceCid) {
        sidecarLane.push(workItem)
      } else {
        pieceLane.push(workItem)
      }
    }

    const summary = {
      totalEligible: sidecarLane.length + pieceLane.length,
      sidecarTotal: sidecarLane.length,
      pieceTotal: pieceLane.length,
      sidecarDone: 0,
      pieceDone: 0,
      failed,
    }

    if (summary.totalEligible === 0) {
      console.error('prepare: nothing to do')
      return
    }

    renderPrepareProgress(summary)

    const laneResults = await Promise.allSettled([
      pMap(
        sidecarLane,
        async (item) => {
          try {
            await renameCarToPieceCid(dir, item.shardCid, item.pieceCid, item.carPath)
            const rootCids = tracking.listRootCidsForShard(item.shardCid)
            await writePieceSidecar(dir, item.shardCid, item.pieceCid, item.sizeBytes, rootCids)
            tracking.clearPrepareFailure(item.shardCid)
          } catch (err) {
            tracking.markPrepareFailure({
              shardCid: item.shardCid,
              error: String(err?.message || err),
              retryable: false,
            })
            summary.failed++
          } finally {
            summary.sidecarDone++
            renderPrepareProgress(summary)
          }
        },
        { concurrency: laneConcurrency },
      ),
      pMap(
        pieceLane,
        async (item) => {
          try {
            const pieceCid = await calculateLocalPieceCid(item.carPath)
            tracking.setPieceCid(item.shardCid, pieceCid)
            await renameCarToPieceCid(dir, item.shardCid, pieceCid, item.carPath)
            const rootCids = tracking.listRootCidsForShard(item.shardCid)
            await writePieceSidecar(dir, item.shardCid, pieceCid, item.sizeBytes, rootCids)
            tracking.clearPrepareFailure(item.shardCid)
          } catch (err) {
            tracking.markPrepareFailure({
              shardCid: item.shardCid,
              error: String(err?.message || err),
              retryable: false,
            })
            summary.failed++
          } finally {
            summary.pieceDone++
            renderPrepareProgress(summary)
          }
        },
        { concurrency: laneConcurrency },
      ),
    ])

    for (const [index, result] of laneResults.entries()) {
      if (result.status === 'rejected') {
        const laneName = index === 0 ? 'sidecar' : 'piece'
        console.error(`prepare: ${laneName} lane rejected unexpectedly: ${result.reason?.message || result.reason}`)
      }
    }

    if (process.stdout.isTTY) process.stdout.write('\n')
    console.error(
      `prepare: done. eligible=${summary.totalEligible} sidecar=${summary.sidecarDone}/${summary.sidecarTotal} piece=${summary.pieceDone}/${summary.pieceTotal} failed=${summary.failed}`,
    )
  } finally {
    tracking.close()
  }
}
