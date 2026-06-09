import { getActivePieces } from '@filoz/synapse-core/pdp-verifier'
import { getPdpDataSet } from '@filoz/synapse-core/warm-storage'
import pMap from 'p-map'
import pRetry, { AbortError } from 'p-retry'
import { createPublicClient, http } from 'viem'

import { openInventoryDb } from '../lib/inventory-db.mjs'
import { openTrackingDb } from '../lib/tracking-db.mjs'

const VERIFY_ACTIVE_PIECES_PAGE_SIZE = 1000n
const VERIFY_DB_BATCH_SIZE = 1_000
const DEFAULT_VERIFY_CONCURRENCY = 20
const VERIFY_ROOT_RETRY_ATTEMPTS = 3
const VERIFY_ROOT_RETRY_DELAY_MS = 300

/** @typedef {ReturnType<typeof openTrackingDb>} TrackingDb */
/** @typedef {ReturnType<typeof openInventoryDb>} InventoryDb */

/**
 * @typedef {object} InventoryVerificationResult
 * @property {number} sourceShardCount
 * @property {number} trackingShardCount
 * @property {number} sourceRootCount
 * @property {number} trackingRootCount
 */

/**
 * @typedef {object} PieceVerificationResult
 * @property {number} activePieceCount
 * @property {number} dbCommittedPieceCount
 * @property {string[]} missingPieceCids
 * @property {string[]} extraPieceCids
 */

/**
 * @typedef {object} RootVerificationResult
 * @property {number} total
 * @property {number} reachable
 * @property {number} missing
 * @property {number} errored
 * @property {string[]} missingRootCids
 * @property {{ rootCid: string, error: string }[]} erroredRoots
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
  let dbCommittedPieceCount = 0
  let afterPieceCid = ''

  while (true) {
    const pieceCids = tracking.listCommittedPieceCids(VERIFY_DB_BATCH_SIZE, afterPieceCid)
    if (pieceCids.length === 0) break

    dbCommittedPieceCount += pieceCids.length
    for (const pieceCid of pieceCids) {
      if (!activePieceCids.delete(pieceCid)) {
        missingPieceCids.push(pieceCid)
      }
    }

    afterPieceCid = pieceCids[pieceCids.length - 1]
  }

  return {
    activePieceCount,
    dbCommittedPieceCount,
    missingPieceCids,
    extraPieceCids: [...activePieceCids],
  }
}

/**
 * @param {InventoryDb} inventory
 * @param {TrackingDb} tracking
 * @returns {InventoryVerificationResult}
 */
function verifyInventoryCounts(inventory, tracking) {
  const sourceShardCount = inventory.countDistinctShards()
  const trackingShardCount = tracking.countShards()
  const sourceRootCount = inventory.countDistinctRoots()
  const trackingRootCount = tracking.countDistinctRoots()

  return {
    sourceShardCount,
    trackingShardCount,
    sourceRootCount,
    trackingRootCount,
  }
}

/**
 * @param {string} serviceUrl
 * @param {string} rootCid
 */
async function checkRootReachability(serviceUrl, rootCid) {
  /**
   * Probe the trustless gateway CAR endpoint we actually rely on for
   * retrieval. HEAD is enough here: pieces are already verified on-chain,
   * and this check only needs to prove the root CID resolves on the provider.
   *
   * `dag-scope=entity` is a no-op for HEAD. The server resolves
   *  the root block and returns 200, it does not walk the DAG
   *  on a HEAD regardless of scope. We include it anyway for two reasons:
   *     1. Explicitness: the URL declares "entity-level addressability"
   *        rather than the spec's implicit `dag-scope=all` default.
   *     2. Forward-compat: if this ever switches to a GET-and-drain probe
   *        (e.g., for a sampled deep check), the scope is already bounded
   *        to the entity instead of pulling the full DAG.
   */
  const url = new URL(
    `ipfs/${rootCid}?format=car&dag-scope=entity`,
    serviceUrl.endsWith('/') ? serviceUrl : `${serviceUrl}/`,
  )

  try {
    const response = await pRetry(
      async () => {
        const response = await fetch(url, {
          method: 'HEAD',
          headers: { Accept: 'application/vnd.ipld.car' },
        })
        if (response.status === 200 || response.status === 404) {
          return response
        }

        const isTransient = response.status === 408 || response.status === 429 || response.status >= 500
        if (isTransient) {
          throw new Error(`transient HTTP status ${response.status}`)
        }

        throw new AbortError(`unexpected HTTP status ${response.status}`)
      },
      {
        retries: VERIFY_ROOT_RETRY_ATTEMPTS - 1,
        minTimeout: VERIFY_ROOT_RETRY_DELAY_MS,
        factor: 2,
      },
    )

    if (response.status === 200) {
      return { rootCid, status: 'reachable' }
    }

    return { rootCid, status: 'missing' }
  } catch (err) {
    return {
      rootCid,
      status: 'errored',
      error: err instanceof Error ? err.message : String(err),
    }
  }
}

/**
 * @param {TrackingDb} tracking
 * @param {string} serviceUrl
 * @param {number} concurrency
 * @returns {Promise<RootVerificationResult>}
 */
async function verifyCommittedRoots(tracking, serviceUrl, concurrency) {
  const missingRootCids = []
  const erroredRoots = []
  let total = 0
  let reachable = 0
  let missing = 0
  let errored = 0
  let afterRootCid = ''

  while (true) {
    const rootCids = tracking.listCommittedRootCids(VERIFY_DB_BATCH_SIZE, afterRootCid)
    if (rootCids.length === 0) break

    total += rootCids.length
    const results = await pMap(rootCids, (rootCid) => checkRootReachability(serviceUrl, rootCid), {
      concurrency,
    })

    for (const result of results) {
      if (result.status === 'reachable') {
        reachable++
        continue
      }

      if (result.status === 'missing') {
        missing++
        missingRootCids.push(result.rootCid)
        continue
      }

      errored++
      erroredRoots.push({
        rootCid: result.rootCid,
        error: result.error,
      })
    }

    afterRootCid = rootCids[rootCids.length - 1]
  }

  return {
    total,
    reachable,
    missing,
    errored,
    missingRootCids,
    erroredRoots,
  }
}

/**
 * @param {string} state
 * @param {InventoryVerificationResult} inventory
 * @param {ReturnType<TrackingDb['getCommitStats']>} commitStats
 * @param {Awaited<ReturnType<typeof getPdpDataSet>>} dataSet
 * @param {PieceVerificationResult} pieces
 * @param {RootVerificationResult} roots
 */
function summarizeState(state, inventory, commitStats, dataSet, pieces, roots) {
  if (inventory.sourceShardCount !== inventory.trackingShardCount) return 'failed'
  if (inventory.sourceRootCount !== inventory.trackingRootCount) return 'failed'
  if (state === 'failed') return 'failed'
  if (!dataSet?.live) return 'failed'
  if (pieces.missingPieceCids.length > 0) return 'failed'
  if (roots.missing > 0) return 'failed'
  if (roots.errored > 0) return 'incomplete'
  if (commitStats.pending > 0 || commitStats.parked > 0 || commitStats.committing > 0 || commitStats.failed > 0) {
    return 'incomplete'
  }
  return 'complete'
}

/**
 * @param {object} args
 * @param {string} args.db
 * @param {string} args.dir
 * @param {import('viem').Chain} args.chain
 * @param {number | undefined} args.concurrency
 */
export async function runVerify({ db, dir, chain, concurrency }) {
  const inventory = openInventoryDb(db)
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
    const inventoryCounts = verifyInventoryCounts(inventory, tracking)
    const commitStats = tracking.getCommitStats()
    const dataSet = await getPdpDataSet(publicClient, { dataSetId: BigInt(metadata.dataSetId) })
    const pieces = dataSet?.live
      ? verifyCommittedPieces(tracking, await listActivePieceCids(publicClient, metadata.dataSetId))
      : {
          activePieceCount: 0,
          dbCommittedPieceCount: 0,
          missingPieceCids: [],
          extraPieceCids: [],
        }
    const roots = await verifyCommittedRoots(tracking, metadata.serviceUrl, concurrency ?? DEFAULT_VERIFY_CONCURRENCY)
    const state = summarizeState(metadata.state, inventoryCounts, commitStats, dataSet, pieces, roots)

    console.log(`verify: dataset=${metadata.dataSetId} live=${dataSet?.live === true} state=${state}`)
    console.log(
      `verify: inventory shards source=${inventoryCounts.sourceShardCount} tracking=${inventoryCounts.trackingShardCount} roots source=${inventoryCounts.sourceRootCount} tracking=${inventoryCounts.trackingRootCount}`,
    )
    console.log(
      `verify: pieces active=${pieces.activePieceCount} db=${pieces.dbCommittedPieceCount} missing=${pieces.missingPieceCids.length} extra=${pieces.extraPieceCids.length}`,
    )
    console.log(
      `verify: roots total=${roots.total} reachable=${roots.reachable} missing=${roots.missing} errored=${roots.errored}`,
    )

    if (state !== 'complete') {
      throw new Error(`verify: migration verification ended with state=${state}`)
    }
  } finally {
    inventory.close()
    tracking.close()
  }
}
