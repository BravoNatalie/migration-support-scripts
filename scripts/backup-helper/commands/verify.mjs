import { getActivePieces } from '@filoz/synapse-core/pdp-verifier'
import { getPdpDataSet } from '@filoz/synapse-core/warm-storage'
import { createPublicClient, http } from 'viem'

import { openTrackingDb } from '../lib/tracking-db.mjs'

const VERIFY_ACTIVE_PIECES_PAGE_SIZE = 1000n
const VERIFY_DB_BATCH_SIZE = 1_000

/** @typedef {ReturnType<typeof openTrackingDb>} TrackingDb */

/**
 * @typedef {object} PieceVerificationResult
 * @property {number} activePieceCount
 * @property {number} dbPieceCount
 * @property {string[]} missingPieceCids
 * @property {string[]} extraPieceCids
 */

/**
 * @param {import('viem').PublicClient} publicClient
 * @param {number} dataSetId
 */
async function listActivePieceCids(publicClient, dataSetId) {
  /** @type {Set<string>} */
  const activePieceCids = new Set()

  // This workflow has a known upper bound around 3M pieces, so an in-memory
  // Set is acceptable here. If that bound becomes unknown or approaches Node
  // memory limits, this should switch to a temp SQLite-table comparison.
  for (let offset = 0n; ; offset += VERIFY_ACTIVE_PIECES_PAGE_SIZE) {
    const { pieces, hasMore } = await getActivePieces(publicClient, {
      dataSetId: BigInt(dataSetId),
      offset,
      limit: VERIFY_ACTIVE_PIECES_PAGE_SIZE,
    })

    for (const piece of pieces) {
      activePieceCids.add(piece.cid.toString())
    }

    if (!hasMore) return activePieceCids
  }
}

/**
 * @param {TrackingDb} tracking
 * @param {Set<string>} activePieceCids
 * @returns {PieceVerificationResult}
 */
function verifyCommittedPieces(tracking, activePieceCids) {
  /** @type {string[]} */
  const missingPieceCids = []
  const activePieceCount = activePieceCids.size
  let dbPieceCount = 0
  let afterPieceCid = ''

  while (true) {
    const pieceCids = tracking.listCommittedPieceCids(VERIFY_DB_BATCH_SIZE, afterPieceCid)
    if (pieceCids.length === 0) break

    dbPieceCount += pieceCids.length
    for (const pieceCid of pieceCids) {
      if (!activePieceCids.delete(pieceCid)) {
        missingPieceCids.push(pieceCid)
      }
    }

    afterPieceCid = pieceCids[pieceCids.length - 1]
  }

  return {
    activePieceCount,
    dbPieceCount,
    missingPieceCids,
    extraPieceCids: [...activePieceCids],
  }
}

/**
 * @param {string} state
 * @param {ReturnType<TrackingDb['getCommitStats']>} commitStats
 * @param {Awaited<ReturnType<typeof getPdpDataSet>>} dataSet
 * @param {PieceVerificationResult} pieces
 */
function summarizeState(state, commitStats, dataSet, pieces) {
  if (state === 'failed') return 'failed'
  if (!dataSet?.live) return 'failed'
  if (pieces.missingPieceCids.length > 0) return 'failed'
  if (commitStats.pending > 0 || commitStats.parked > 0 || commitStats.committing > 0 || commitStats.failed > 0) {
    return 'incomplete'
  }
  return 'complete'
}

/**
 * @param {object} args
 * @param {string} args.dir
 * @param {import('viem').Chain} args.chain
 * @param {number | undefined} args.concurrency
 */
export async function runVerify({ dir, chain, concurrency: _concurrency }) {
  const tracking = openTrackingDb(dir)
  try {
    const metadata = tracking.getMigrationMetadata()
    if (!metadata) {
      throw new Error('verify: migration metadata was not initialized')
    }
    if (metadata.dataSetId == null) {
      throw new Error('verify: migration metadata does not contain a dataSetId')
    }

    const publicClient = createPublicClient({ chain, transport: http() })
    const commitStats = tracking.getCommitStats()
    const dataSet = await getPdpDataSet(publicClient, { dataSetId: BigInt(metadata.dataSetId) })
    const activePieceCids = dataSet?.live ? await listActivePieceCids(publicClient, metadata.dataSetId) : new Set()
    const pieces = verifyCommittedPieces(tracking, activePieceCids)
    const state = summarizeState(metadata.state, commitStats, dataSet, pieces)

    console.log(`verify: dataset=${metadata.dataSetId} live=${dataSet?.live === true} state=${state}`)
    console.log(
      `verify: pieces active=${pieces.activePieceCount} db=${pieces.dbPieceCount} missing=${pieces.missingPieceCids.length} extra=${pieces.extraPieceCids.length}`,
    )
  } finally {
    tracking.close()
  }
}
