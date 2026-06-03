/**
 * `backup-helper commit` — park prepared pieces and commit them on-chain.
 *
 * The real parking flow is delegated to an external binary that runs on the
 * provider machine and returns ready-to-commit `pieceCid`s.
 */

import { parse as parsePieceCid } from '@filoz/synapse-core/piece'
import { fromSecp256k1 } from '@filoz/synapse-core/session-key'
import { addPieces, createDataSet, waitForAddPieces, waitForCreateDataSet } from '@filoz/synapse-core/sp'
import { getDataSet } from '@filoz/synapse-core/warm-storage'
import { execa } from 'execa'
import { getAddress } from 'viem/utils'
import { z } from 'zod'

import { renderProgressLine } from '../../utils.js'
import { openTrackingDb } from '../lib/tracking-db.mjs'

const PARKING_BATCH_SIZE = 50
const COMMIT_BATCH_SIZE = 20
const DEFAULT_COMMIT_CONCURRENCY = 4
const EMPTY_POLL_INTERVAL_MS = 1_000

/**
 * @typedef {object} CommitRow
 * @property {string} rootCid
 * @property {string} shardCid
 * @property {string} pieceCid
 */

/** @typedef {ReturnType<typeof openTrackingDb>} TrackingDb */

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * @param {`0x${string}`} sessionKeyStr
 * @param {`0x${string}`} customerWallet
 * @param {import('viem').Chain} chain
 */
async function createSessionKey(sessionKeyStr, customerWallet, chain) {
  const customerAccount = getAddress(customerWallet)
  const sessionKey = fromSecp256k1({
    privateKey: sessionKeyStr,
    root: customerAccount, // owner address, not private key
    chain,
  })
  await sessionKey.syncExpirations()
  return sessionKey
}

/**
 * Create or load the dataset before any parking/commit work starts.
 *
 * @param {object} args
 * @param {TrackingDb} args.tracking
 * @param {ReturnType<typeof fromSecp256k1>} args.sessionKey
 * @param {string} args.serviceUrl
 * @param {`0x${string}`} args.providerAddress
 */
async function ensureDataSet({ tracking, sessionKey, serviceUrl, providerAddress }) {
  const metadata = tracking.getMigrationMetadata()
  if (!metadata) {
    throw new Error('commit: migration metadata was not initialized')
  }

  if (metadata.dataSetId != null) {
    if (metadata.clientDataSetId == null) {
      const dataSetInfo = await getDataSet(sessionKey.client, {
        dataSetId: BigInt(metadata.dataSetId),
      })
      if (!dataSetInfo) {
        throw new Error(`commit: dataset ${metadata.dataSetId} was not found on-chain`)
      }
      tracking.setMigrationClientDataSetId(Number(dataSetInfo.clientDataSetId))
      return {
        dataSetId: metadata.dataSetId,
        clientDataSetId: Number(dataSetInfo.clientDataSetId),
      }
    }

    if (metadata.state !== tracking.MIGRATION_STATE.complete) {
      tracking.setMigrationState(tracking.MIGRATION_STATE.migrating)
    }

    return {
      dataSetId: metadata.dataSetId,
      clientDataSetId: metadata.clientDataSetId,
    }
  }

  const createResult = await createDataSet(sessionKey.client, {
    cdn: true,
    payee: providerAddress,
    payer: sessionKey.rootAddress,
    serviceURL: serviceUrl,
    metadata: {
      source: 'filecoin-pin',
      withIPFSIndexing: '',
    },
  })
  const { dataSetId } = await waitForCreateDataSet(createResult)
  const dataSetInfo = await getDataSet(sessionKey.client, { dataSetId })
  if (!dataSetInfo) {
    throw new Error(`commit: dataset ${dataSetId} was created but could not be fetched`)
  }

  tracking.markMigrationDataSetCreated({
    dataSetId: Number(dataSetId),
    clientDataSetId: Number(dataSetInfo.clientDataSetId),
  })

  return {
    dataSetId: Number(dataSetId),
    clientDataSetId: Number(dataSetInfo.clientDataSetId),
  }
}

function renderCommitProgress(summary) {
  renderProgressLine(
    `commit: pending=${summary.pending} parked=${summary.parked} committing=${summary.committing} committed=${summary.committed} failed=${summary.failed}`,
  )
}

/**
 * @typedef {object} ParkingResult
 * @property {number} count
 * @property {string[]} pieces
 */

const parkingResultSchema = z
  .object({
    count: z.number().int().nonnegative(),
    pieces: z.array(z.string()),
  })
  .refine((value) => value.count === value.pieces.length, {
    message: 'count must match pieces length',
  })

/**
 * @param {string} dir
 * @param {string} target
 * @returns {Promise<ParkingResult>}
 */
async function runParkingBinary(dir, target) {
  const result = await execa('curio', [
    'toolbox',
    'import-pieces',
    '--source',
    dir,
    '--target',
    target,
    '--batch-size',
    String(PARKING_BATCH_SIZE),
  ])

  let parsed
  try {
    parsed = JSON.parse(result.stdout || '{}')
  } catch (err) {
    throw new Error(`commit: parking command returned invalid JSON: ${err?.message || err}`)
  }

  return parkingResultSchema.parse(parsed)
}

/**
 * @param {CommitRow[]} rows
 */
function buildCommitPieces(rows) {
  return rows.map((row) => ({
    pieceCid: parsePieceCid(row.pieceCid),
    metadata: { ipfsRootCID: row.rootCid },
  }))
}

/**
 * @param {object} args
 * @param {ReturnType<typeof fromSecp256k1>} args.sessionKey
 * @param {string} args.serviceUrl
 * @param {number} args.dataSetId
 * @param {number} args.clientDataSetId
 * @param {TrackingDb} args.tracking
 * @param {CommitRow[]} args.rows
 */
async function commitBatch({ sessionKey, serviceUrl, dataSetId, clientDataSetId, tracking, rows }) {
  try {
    const addResult = await addPieces(sessionKey.client, {
      serviceURL: serviceUrl,
      dataSetId: BigInt(dataSetId),
      clientDataSetId: BigInt(clientDataSetId),
      pieces: buildCommitPieces(rows),
    })
    const addStatus = await waitForAddPieces({ statusUrl: addResult.statusUrl })

    tracking.markCommitBatchSucceeded(rows, addStatus.txHash.toString())

    return { success: true }
  } catch (err) {
    tracking.markCommitBatchFailed(rows, String(err?.message || err))
    return { success: false }
  }
}

/**
 * @param {object} args
 * @param {string} args.dir
 * @param {string} args.serviceUrl
 * @param {`0x${string}`} args.providerAddress
 * @param {`0x${string}`} args.customerWallet
 * @param {string} args.sessionKey
 * @param {import('viem').Chain} args.chain
 * @param {string} args.target
 * @param {number | undefined} args.concurrency
 * @param {boolean} args.retry
 */
export async function runCommit({
  dir,
  serviceUrl,
  providerAddress,
  customerWallet,
  sessionKey,
  chain,
  target,
  concurrency,
  retry,
}) {
  const tracking = openTrackingDb(dir)
  const commitConcurrency = concurrency ?? DEFAULT_COMMIT_CONCURRENCY
  let metadataInitialized = false
  let finalizedMigrationState = false

  let parkingSettled = false
  let parkingFailed = false

  try {
    tracking.initMigrationMetadata({ clientWallet: customerWallet, serviceUrl, providerAddress })
    metadataInitialized = true
    if (retry) {
      tracking.resetCommitRowsForRetry()
    }

    const session = await createSessionKey(/** @type {`0x${string}`} */ (sessionKey), customerWallet, chain)
    const ensuredDataSet = await ensureDataSet({
      tracking,
      sessionKey: session,
      serviceUrl,
      providerAddress,
    })

    const parkingLane = (async () => {
      try {
        while (true) {
          const parkingResult = await runParkingBinary(dir, target)
          if (parkingResult.count === 0) return
          tracking.markParkedByPieceCids(parkingResult.pieces)
          renderCommitProgress(tracking.getCommitStats())
        }
      } finally {
        parkingSettled = true
      }
    })().catch((err) => {
      parkingFailed = true
      throw err
    })

    const commitWorker = async () => {
      while (true) {
        const rows = tracking.claimCommitBatch(COMMIT_BATCH_SIZE)
        if (rows.length === 0) {
          if (parkingSettled) return
          await sleep(EMPTY_POLL_INTERVAL_MS)
          continue
        }

        await commitBatch({
          sessionKey: session,
          serviceUrl,
          dataSetId: ensuredDataSet.dataSetId,
          clientDataSetId: ensuredDataSet.clientDataSetId,
          tracking,
          rows,
        })
        renderCommitProgress(tracking.getCommitStats())
      }
    }

    const commitLane = (async () => {
      const workers = Array.from({ length: commitConcurrency }, () => commitWorker())
      const workerResults = await Promise.allSettled(workers)
      let rejectedWorker = null

      for (const [index, result] of workerResults.entries()) {
        if (result.status === 'rejected') {
          console.error(`commit: worker ${index + 1} rejected unexpectedly: ${result.reason?.message || result.reason}`)
          rejectedWorker ??= result
        }
      }
      if (rejectedWorker?.status === 'rejected') {
        throw rejectedWorker.reason
      }
    })()

    renderCommitProgress(tracking.getCommitStats())
    const laneResults = await Promise.allSettled([parkingLane, commitLane])
    let rejectedReason = null
    let rejectedLane = false
    for (const [index, result] of laneResults.entries()) {
      if (result.status === 'rejected') {
        const laneName = index === 0 ? 'parking' : 'commit'
        console.error(`commit: ${laneName} lane rejected unexpectedly: ${result.reason?.message || result.reason}`)
        rejectedLane = true
        rejectedReason ??= result.reason
      }
    }

    const stats = tracking.getCommitStats()
    const isComplete =
      stats.pending === 0 && stats.parked === 0 && stats.committing === 0 && stats.failed === 0 && !parkingFailed

    tracking.setMigrationState(
      !rejectedLane && isComplete ? tracking.MIGRATION_STATE.complete : tracking.MIGRATION_STATE.incomplete,
    )
    finalizedMigrationState = true

    if (rejectedReason) {
      throw rejectedReason
    }

    if (process.stdout.isTTY) process.stdout.write('\n')
    console.error(
      `commit: done. pending=${stats.pending} parked=${stats.parked} committing=${stats.committing} committed=${stats.committed} failed=${stats.failed}`,
    )
  } catch (err) {
    if (metadataInitialized && !finalizedMigrationState) {
      tracking.setMigrationState(tracking.MIGRATION_STATE.failed)
    }
    throw err
  } finally {
    tracking.close()
  }
}
