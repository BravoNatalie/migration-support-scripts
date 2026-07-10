import { dataSetLive, getContract as getPdpVerifierContract } from '@filoz/synapse-core/pdp-verifier'
import { hexToPieceCID } from '@filoz/synapse-core/piece'
import pMap from 'p-map'
import pRetry, { AbortError } from 'p-retry'
import { createPublicClient, http } from 'viem'

import { renderProgressLine, writeFileAtomic } from '../../utils.js'
import { verifyReportPath } from '../lib/layout.mjs'
import { openTrackingDb } from '../lib/tracking-db.mjs'

const VERIFY_ACTIVE_PIECES_PAGE_SIZE = 100n
const VERIFY_DB_BATCH_SIZE = 1_000
const DEFAULT_VERIFY_CONCURRENCY = 100
const VERIFY_ROOT_RETRY_ATTEMPTS = 3
const VERIFY_ROOT_RETRY_DELAY_MS = 200
const FOC_OBSERVER_BATCH_SIZE = 100

/** @typedef {ReturnType<typeof openTrackingDb>} TrackingDb */

/**
 * @typedef {object} InventoryVerificationResult
 * @property {number} missingShards
 * @property {number} missingRoots
 */

/**
 * @typedef {object} PieceVerificationResult
 * @property {number} activePieceCount
 * @property {number} dbCommittedPieceCount
 * @property {string[]} missingPieceCids
 * @property {string[]} extraPieceCids
 */

/**
 * @typedef {'confirmed' | 'mismatch' | 'notIndexed' | 'errored'} PieceOnChainStatus
 */

/**
 * @typedef {object} MissingRootPiece
 * @property {string} pieceCid
 * @property {string | null} pieceId
 * @property {string | null} txHash
 * @property {PieceOnChainStatus} status
 * @property {string | null} onChainRootCid
 */

/**
 * @typedef {object} MissingRoot
 * @property {string} rootCid
 * @property {'sp_not_serving' | 'metadata_missing' | 'metadata_mismatch' | 'partial' | 'errored' | null} diagnosis
 * @property {MissingRootPiece[]} pieces
 */

/**
 * @typedef {object} RootReachabilityResult
 * @property {number} total
 * @property {number} reachable
 * @property {number} missing
 * @property {number} errored
 * @property {string[]} missingRootCids
 * @property {{ rootCid: string, error: string }[]} erroredRoots
 */

/**
 * @param {string | null | undefined} txHash
 * @returns {boolean}
 */
function isValidTxHash(txHash) {
  return typeof txHash === 'string' && /^0x[0-9a-fA-F]{64}$/.test(txHash)
}

/**
 * @template T
 * @param {T[]} arr
 * @param {number} size
 * @returns {T[][]}
 */
function chunk(arr, size) {
  const result = []
  for (let i = 0; i < arr.length; i += size) {
    result.push(arr.slice(i, i + size))
  }
  return result
}

/**
 * @param {string} apiUrl
 * @param {string} network
 * @param {string} sql
 * @returns {Promise<Record<string, unknown>[]>}
 */
async function queryFocObserver(apiUrl, network, sql) {
  const res = await fetch(`${apiUrl}/sql`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ network, sql }),
  })
  if (!res.ok) {
    throw new Error(`foc-observer /sql responded ${res.status} ${res.statusText}`)
  }
  const data = await res.json()
  if (!Array.isArray(data?.rows)) {
    throw new Error('foc-observer /sql response missing rows array')
  }
  return data.rows
}

/**
 * @param {MissingRootPiece[]} pieces
 * @returns {MissingRoot['diagnosis']}
 */
function deriveDiagnosis(pieces) {
  if (pieces.length === 0) return 'errored'
  if (pieces.every((p) => p.status === 'confirmed')) return 'sp_not_serving'
  if (pieces.every((p) => p.status === 'notIndexed')) return 'metadata_missing'
  if (pieces.some((p) => p.status === 'errored')) return 'errored'
  if (pieces.every((p) => p.status === 'mismatch' || p.status === 'confirmed')) return 'metadata_mismatch'
  return 'partial'
}

/**
 * @param {string[]} missingRootCids
 * @param {TrackingDb} tracking
 * @returns {{ piecesByRoot: Map<string, { pieceCid: string, txHash: string | null }[]>, allPieces: { pieceCid: string, txHash: string | null }[] }}
 */
function collectPiecesForRoots(missingRootCids, tracking) {
  /** @type {Map<string, { pieceCid: string, txHash: string | null }[]>} */
  const piecesByRoot = new Map()
  /** @type {{ pieceCid: string, txHash: string | null }[]} */
  const allPieces = []
  const seen = new Set()

  for (const rootCid of missingRootCids) {
    const pieces = tracking.listCommittedPiecesByRootCid(rootCid)
    piecesByRoot.set(rootCid, pieces)
    for (const piece of pieces) {
      if (!seen.has(piece.pieceCid)) {
        seen.add(piece.pieceCid)
        allPieces.push(piece)
      }
    }
  }
  return { piecesByRoot, allPieces }
}

/**
 * Fetch fwss_piece_added rows for the given pieces in batches. Prefers
 * tx_hash queries (one tx covers many pieces) and falls back to piece_cid
 * for pieces whose tx_hash is missing or malformed.
 *
 * @param {{ pieceCid: string, txHash: string | null }[]} allPieces
 * @param {number} dataSetId
 * @param {string} network
 * @param {string} focApiUrl
 * @returns {Promise<Map<string, Record<string, unknown>>>}
 */
async function fetchFocPieceMap(allPieces, dataSetId, network, focApiUrl) {
  /** @type {Set<string>} */
  const byTxHash = new Set()
  /** @type {string[]} */
  const byPieceCid = []

  for (const piece of allPieces) {
    if (isValidTxHash(piece.txHash)) {
      byTxHash.add(piece.txHash)
    } else {
      byPieceCid.push(piece.pieceCid)
    }
  }

  /** @type {Map<string, Record<string, unknown>>} */
  const focMap = new Map()

  for (const batch of chunk([...byTxHash], FOC_OBSERVER_BATCH_SIZE)) {
    const inList = batch.map((h) => `'${h}'`).join(', ')
    const rows = await queryFocObserver(
      focApiUrl,
      network,
      `SELECT piece_id, piece_cid, tx_hash, metadata FROM fwss_piece_added WHERE data_set_id = ${dataSetId} AND tx_hash IN (${inList})`,
    )
    for (const row of rows) {
      focMap.set(String(row.piece_cid), row)
    }
  }

  for (const batch of chunk(byPieceCid, FOC_OBSERVER_BATCH_SIZE)) {
    const inList = batch.map((c) => `'${c}'`).join(', ')
    const rows = await queryFocObserver(
      focApiUrl,
      network,
      `SELECT piece_id, piece_cid, tx_hash, metadata FROM fwss_piece_added WHERE data_set_id = ${dataSetId} AND piece_cid IN (${inList})`,
    )
    for (const row of rows) {
      focMap.set(String(row.piece_cid), row)
    }
  }

  return focMap
}

/**
 * @param {string} rootCid
 * @param {{ pieceCid: string, txHash: string | null }[]} dbPieces
 * @param {Map<string, Record<string, unknown>>} focMap
 * @returns {MissingRoot}
 */
function buildMissingRootResult(rootCid, dbPieces, focMap) {
  /** @type {MissingRootPiece[]} */
  const pieces = dbPieces.map((dbPiece) => {
    const focRow = focMap.get(dbPiece.pieceCid)
    if (!focRow) {
      return {
        pieceCid: dbPiece.pieceCid,
        pieceId: null,
        txHash: dbPiece.txHash,
        status: /** @type {PieceOnChainStatus} */ ('notIndexed'),
        onChainRootCid: null,
      }
    }

    const txHash = focRow.tx_hash != null ? String(focRow.tx_hash) : dbPiece.txHash
    const pieceId = String(focRow.piece_id)

    let onChainRootCid = null
    try {
      const metadata = JSON.parse(String(focRow.metadata ?? '{}'))
      onChainRootCid = typeof metadata.ipfsRootCID === 'string' ? metadata.ipfsRootCID : null
    } catch {
      return {
        pieceCid: dbPiece.pieceCid,
        pieceId,
        txHash,
        status: /** @type {PieceOnChainStatus} */ ('errored'),
        onChainRootCid: null,
      }
    }

    /** @type {PieceOnChainStatus} */
    let status
    if (onChainRootCid === rootCid) {
      status = 'confirmed'
    } else if (onChainRootCid === null) {
      status = 'notIndexed'
    } else {
      status = 'mismatch'
    }

    return { pieceCid: dbPiece.pieceCid, pieceId, txHash, status, onChainRootCid }
  })

  return { rootCid, diagnosis: deriveDiagnosis(pieces), pieces }
}

/**
 * Enrich missing root CIDs with per-piece on-chain data from foc-observer.
 * The indexed fwss_piece_added.metadata field contains the ipfsRootCID stored
 * at commit time, so no additional chain RPC is needed.
 *
 * @param {string[]} missingRootCids
 * @param {TrackingDb} tracking
 * @param {number} dataSetId
 * @param {string} network
 * @param {string} focApiUrl
 * @returns {Promise<MissingRoot[]>}
 */
async function enrichMissingRoots(missingRootCids, tracking, dataSetId, network, focApiUrl) {
  const { piecesByRoot, allPieces } = collectPiecesForRoots(missingRootCids, tracking)
  const focMap = await fetchFocPieceMap(allPieces, dataSetId, network, focApiUrl)
  return missingRootCids.map((rootCid) => buildMissingRootResult(rootCid, piecesByRoot.get(rootCid) ?? [], focMap))
}

/**
 * End an in-place progress line before normal console output resumes. This is
 * a no-op for non-interactive output because renderProgressLine does not write
 * anything there.
 */
function finishProgressLine() {
  if (process.stdout.isTTY) process.stdout.write('\n')
}

/**
 * List every active piece CID currently present on-chain for a data set.
 * Uses cursor pagination to avoid the high-offset gas cost of
 * PDPVerifier.getActivePieces().
 *
 * @param {import('viem').PublicClient} publicClient
 * @param {number} dataSetId
 * @returns {Promise<Set<string>>}
 */
async function listActivePieceCids(publicClient, dataSetId) {
  /** @type {Set<string>} */
  const activePieceCids = new Set()
  const pdpVerifier = getPdpVerifierContract({ client: publicClient })
  let startPieceId = 0n
  let hasMore = true

  // This workflow has a known upper bound around 3M pieces, so an in-memory
  // Set is acceptable here. If that bound becomes unknown or approaches Node
  // memory limits, this should switch to a temp SQLite-table comparison.
  try {
    while (hasMore) {
      const [pieces, pieceIds, nextHasMore] = await pdpVerifier.read.getActivePiecesByCursor([
        BigInt(dataSetId),
        startPieceId,
        VERIFY_ACTIVE_PIECES_PAGE_SIZE,
      ])

      for (const piece of pieces) {
        activePieceCids.add(hexToPieceCID(piece.data).toString())
      }

      renderProgressLine(
        `verify: active pieces fetched=${activePieceCids.size.toLocaleString()} nextPieceId=${startPieceId.toString()}`,
      )

      if (pieceIds.length === 0) {
        if (!nextHasMore) break
        throw new Error(`verify: getActivePiecesByCursor returned an empty page for dataSetId=${dataSetId}`)
      }

      startPieceId = pieceIds[pieceIds.length - 1] + 1n
      hasMore = nextHasMore
    }
  } finally {
    finishProgressLine()
  }

  return activePieceCids
}

/**
 * Compare committed piece CIDs in tracking.db against the active on-chain
 * piece set. Missing pieces are committed locally but absent on-chain; extra
 * pieces are on-chain but not represented by committed local rows.
 *
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
    renderProgressLine(
      `verify: committed pieces checked=${dbCommittedPieceCount.toLocaleString()} missing=${missingPieceCids.length.toLocaleString()}`,
    )
  }
  finishProgressLine()

  return {
    activePieceCount,
    dbCommittedPieceCount,
    missingPieceCids,
    extraPieceCids: [...activePieceCids],
  }
}

/**
 * Verify that every shard and root in the input inventory db is present in
 * tracking. Tracking may legitimately be a superset (create can be re-run with
 * additional inventories targeting the same dir), so the check is one-way:
 * inventory ⊆ tracking. Returns zero counts when the inventory is fully covered.
 *
 * @param {TrackingDb} tracking
 * @param {string} inventoryPath
 * @returns {InventoryVerificationResult}
 */
function verifyInventoryCoverage(tracking, inventoryPath) {
  return {
    missingShards: tracking.countInventoryShardsMissing(inventoryPath),
    missingRoots: tracking.countInventoryRootsMissing(inventoryPath),
  }
}

/**
 * Probe one committed root through the provider's trustless gateway endpoint.
 * Returns a normalized reachable, missing, or errored result so batch
 * verification can distinguish confirmed 404s from retry-exhausted failures.
 *
 * @param {string} serviceUrl
 * @param {string} rootCid
 * @returns {Promise<{ rootCid: string, status: 'reachable' | 'missing' } | { rootCid: string, status: 'errored', error: string }>}
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
 * Verify that committed roots resolve from the provider service URL. Roots are
 * read from tracking.db in bounded batches and probed with bounded concurrency
 * so large migrations do not load all roots into memory or fan out unbounded
 * network requests.
 *
 * @param {TrackingDb} tracking
 * @param {string} serviceUrl
 * @param {number} concurrency
 * @returns {Promise<RootReachabilityResult>}
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

    const results = await pMap(
      rootCids,
      async (rootCid) => {
        const result = await checkRootReachability(serviceUrl, rootCid)
        renderProgressLine(
          `verify: roots checked=${(total + 1).toLocaleString()} reachable=${reachable.toLocaleString()} missing=${missing.toLocaleString()} errored=${errored.toLocaleString()}`,
        )
        total++
        return result
      },
      {
        concurrency,
      },
    )

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
  finishProgressLine()

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
 * Collapse the individual verification results into the final migration state.
 * Hard failures mean required inventory, dataset, piece, or root checks failed;
 * transient root probe errors leave the result incomplete so the operator can
 * retry without treating the migration as definitively failed.
 *
 * @param {string} state
 * @param {InventoryVerificationResult} inventory
 * @param {ReturnType<TrackingDb['getCommitStats']>} commitStats
 * @param {boolean} iSdataSetLive
 * @param {PieceVerificationResult} pieces
 * @param {RootReachabilityResult} roots
 */
function summarizeState(state, inventory, commitStats, iSdataSetLive, pieces, roots) {
  if (inventory.missingShards > 0 || inventory.missingRoots > 0) return 'failed'
  if (state === 'failed') return 'failed'
  if (!iSdataSetLive) return 'failed'
  if (pieces.missingPieceCids.length > 0) return 'failed'
  if (roots.missing > 0) return 'failed'
  if (roots.errored > 0) return 'incomplete'
  if (commitStats.pending > 0 || commitStats.parked > 0 || commitStats.committing > 0 || commitStats.failed > 0) {
    return 'incomplete'
  }
  return 'complete'
}

/**
 * Write the detailed verification result to verify-report.json in the backup
 * output directory. The report keeps the console summary small while preserving
 * the root errors and CID-level differences needed for follow-up.
 *
 * @param {string} dir
 * @param {object} report
 */
async function writeVerifyReport(dir, report) {
  const reportPath = verifyReportPath(dir)
  await writeFileAtomic(reportPath, `${JSON.stringify(report, null, 2)}\n`)
  console.log(`verify: wrote report ${reportPath}`)
}

/**
 * Run the backup verification workflow for one output directory. The command
 * checks input inventory coverage, on-chain dataset liveness, committed piece
 * agreement with PDP state, and committed root reachability through the
 * provider service URL, then prints an operator-facing summary.
 *
 * @param {object} args
 * @param {string} args.db
 * @param {string} args.dir
 * @param {import('viem').Chain} args.chain
 * @param {number | undefined} args.concurrency
 * @param {string | undefined} args.focApiUrl
 */
export async function runVerify({ db, dir, chain, concurrency, focApiUrl }) {
  const tracking = openTrackingDb(dir)
  console.log(`\n-------- VERIFY START --------\n`)
  try {
    const metadata = tracking.getMigrationMetadata()
    if (!metadata) {
      throw new Error('verify: migration metadata was not initialized')
    }
    if (metadata.dataSetId == null) {
      throw new Error('verify: migration metadata does not contain a dataSetId')
    }

    // Filecoin mainnet chainId=314, calibration=314159
    const network = chain.id === 314 ? 'mainnet' : 'calibnet'

    const publicClient = createPublicClient({ chain, transport: http() })
    console.log('checking inventory coverage')
    const inventoryCoverage = verifyInventoryCoverage(tracking, db)

    console.log('reading commit status')
    const commitStats = tracking.getCommitStats()

    console.log('checking on-chain dataset liveness')
    const iSdataSetLive = await dataSetLive(publicClient, { dataSetId: BigInt(metadata.dataSetId) })
    console.log('checking committed root reachability')

    const roots = await verifyCommittedRoots(tracking, metadata.serviceUrl, concurrency ?? DEFAULT_VERIFY_CONCURRENCY)

    console.log('checking committed pieces against PDP state')
    const pieces = iSdataSetLive
      ? verifyCommittedPieces(tracking, await listActivePieceCids(publicClient, metadata.dataSetId))
      : {
          activePieceCount: 0,
          dbCommittedPieceCount: 0,
          missingPieceCids: [],
          extraPieceCids: [],
        }

    /** @type {MissingRoot[]} */
    let missingRoots
    if (focApiUrl && roots.missingRootCids.length > 0) {
      console.log(`enriching ${roots.missingRootCids.length} missing roots with on-chain data`)
      try {
        missingRoots = await enrichMissingRoots(roots.missingRootCids, tracking, metadata.dataSetId, network, focApiUrl)
      } catch (err) {
        console.error(`warn: foc-observer enrichment failed: ${err instanceof Error ? err.message : String(err)}`)
        missingRoots = roots.missingRootCids.map((rootCid) => ({
          rootCid,
          diagnosis: 'errored',
          pieces: [],
        }))
      }
    } else {
      missingRoots = roots.missingRootCids.map((rootCid) => ({ rootCid, diagnosis: null, pieces: [] }))
    }

    const state = summarizeState(metadata.state, inventoryCoverage, commitStats, iSdataSetLive, pieces, roots)
    await writeVerifyReport(dir, {
      generatedAt: new Date().toISOString(),
      state,
      dataSet: {
        id: metadata.dataSetId,
        live: iSdataSetLive,
      },
      inventory: inventoryCoverage,
      commitStats,
      pieces,
      roots: {
        total: roots.total,
        reachable: roots.reachable,
        missing: roots.missing,
        errored: roots.errored,
        missingRoots,
        erroredRoots: roots.erroredRoots,
      },
    })

    console.log(`\n-------- VERIFY RESULTS --------`)
    console.log(`verify: dataset=${metadata.dataSetId} live=${iSdataSetLive} state=${state}`)
    console.log(
      `inventory missingShards=${inventoryCoverage.missingShards} missingRoots=${inventoryCoverage.missingRoots}`,
    )
    console.log(
      `pieces active=${pieces.activePieceCount} db=${pieces.dbCommittedPieceCount} missing=${pieces.missingPieceCids.length} extra=${pieces.extraPieceCids.length}`,
    )
    console.log(
      `roots total=${roots.total} reachable=${roots.reachable} missing=${roots.missing} errored=${roots.errored}`,
    )

    if (state !== 'complete') {
      throw new Error(`migration verification ended with state=${state}`)
    }
  } catch (err) {
    console.error(`Error: ${err instanceof Error ? err.message : String(err)}`)
    throw err
  } finally {
    tracking.close()
  }
}
