/**
 * `backup-helper aggregate-submit` — create/reuse an aggregate dataset and
 * submit planned aggregate PieceCID rows with their ordered sub-pieces.
 */

import { dataSetLive, getContract as getPdpVerifierContract } from '@filoz/synapse-core/pdp-verifier'
import { hexToPieceCID, parse as parsePieceCid } from '@filoz/synapse-core/piece'
import { createDataSet, waitForAddPieces, waitForCreateDataSet } from '@filoz/synapse-core/sp'
import { signAddPieces } from '@filoz/synapse-core/typed-data'
import { getDataSet } from '@filoz/synapse-core/warm-storage'
import { createPublicClient, createWalletClient, http, isHex } from 'viem'
import { privateKeyToAccount } from 'viem/accounts'

import { renderProgressLine } from '../lib/progress.mjs'
import { openTrackingDb } from '../lib/tracking-db.mjs'

const AGGREGATE_PAGE_SIZE = 100
const AGGREGATE_ADD_BATCH_SIZE = 40
const AGGREGATE_SUB_PIECE_BATCH_SIZE = 10_000
const ACTIVE_PIECES_PAGE_SIZE = 100n
const WAIT_ADD_PIECES_TIMEOUT_MS = 60 * 60 * 1_000
const REQUEST_TIMEOUT_MS = 5 * 60 * 1_000

/** @typedef {ReturnType<typeof openTrackingDb>} TrackingDb */

function finishProgressLine() {
  if (process.stdout.isTTY) process.stdout.write('\n')
}

/**
 * @param {number} value
 */
function formatCount(value) {
  return value.toLocaleString('en-US')
}

/**
 * @param {unknown} err
 */
function errorMessage(err) {
  return err instanceof Error ? err.message : String(err)
}

/**
 * @param {string} serviceUrl
 */
function normalizeServiceUrl(serviceUrl) {
  return serviceUrl.endsWith('/') ? serviceUrl : `${serviceUrl}/`
}

/**
 * @param {string} serviceUrl
 * @param {number} dataSetId
 * @param {string} txHash
 */
function addStatusUrl(serviceUrl, dataSetId, txHash) {
  return new URL(`pdp/data-sets/${dataSetId}/pieces/added/${txHash}`, normalizeServiceUrl(serviceUrl)).toString()
}

/**
 * Load active aggregate piece CIDs from the target dataset. The piece id is
 * kept so already-landed planned rows can be reconciled as committed.
 *
 * @param {any} publicClient
 * @param {number} dataSetId
 * @returns {Promise<Map<string, bigint>>}
 */
async function listActivePieceMap(publicClient, dataSetId) {
  /** @type {Map<string, bigint>} */
  const activePieces = new Map()
  const pdpVerifier = /** @type {any} */ (getPdpVerifierContract)(/** @type {any} */ ({ client: publicClient }))
  let startPieceId = 0n
  try {
    let hasMore = true
    while (hasMore) {
      const [pieces, pieceIds, nextHasMore] = await pdpVerifier.read.getActivePiecesByCursor([
        BigInt(dataSetId),
        startPieceId,
        ACTIVE_PIECES_PAGE_SIZE,
      ])

      for (const [index, piece] of pieces.entries()) {
        activePieces.set(hexToPieceCID(piece.data).toString(), pieceIds[index])
      }

      renderProgressLine(
        `Loading active aggregate pieces: ${formatCount(activePieces.size)} found, next piece id ${startPieceId.toString()}`,
      )

      if (pieceIds.length === 0) {
        if (!nextHasMore) break
        throw new Error(`getActivePiecesByCursor returned an empty page for dataSetId=${dataSetId}`)
      }

      startPieceId = pieceIds[pieceIds.length - 1] + 1n
      hasMore = nextHasMore
    }
  } finally {
    finishProgressLine()
  }

  return activePieces
}

/**
 * @param {TrackingDb} tracking
 */
function getSingleAggregateDataSetId(tracking) {
  const dataSetIds = tracking.listAggregateDataSetIds()
  if (dataSetIds.length > 1) {
    throw new Error(`aggregate-submit: aggregate rows reference multiple datasets: ${dataSetIds.join(', ')}`)
  }
  return dataSetIds[0] ?? null
}

/**
 * @param {object} args
 * @param {TrackingDb} args.tracking
 * @param {any} args.walletClient
 * @param {any} args.publicClient
 * @param {string} args.serviceUrl
 * @param {`0x${string}`} args.providerAddress
 * @param {number | undefined} args.requestedDataSetId
 */
async function ensureAggregateDataSet({
  tracking,
  walletClient,
  publicClient,
  serviceUrl,
  providerAddress,
  requestedDataSetId,
}) {
  const existingDataSetId = getSingleAggregateDataSetId(tracking)
  if (existingDataSetId != null && requestedDataSetId != null && existingDataSetId !== requestedDataSetId) {
    throw new Error(
      `aggregate-submit: --data-set-id ${requestedDataSetId} does not match existing aggregate dataset ${existingDataSetId}`,
    )
  }

  const dataSetIdToUse = existingDataSetId ?? requestedDataSetId
  if (dataSetIdToUse != null) {
    const live = await dataSetLive(/** @type {any} */ (publicClient), { dataSetId: BigInt(dataSetIdToUse) })
    if (!live) throw new Error(`aggregate-submit: aggregate dataset ${dataSetIdToUse} is not live`)

    const dataSetInfo = await getDataSet(publicClient, { dataSetId: BigInt(dataSetIdToUse) })
    if (!dataSetInfo) {
      throw new Error(`aggregate-submit: aggregate dataset ${dataSetIdToUse} was not found on-chain`)
    }
    tracking.assignAggregateDataSetId(dataSetIdToUse)
    return {
      dataSetId: dataSetIdToUse,
      clientDataSetId: dataSetInfo.clientDataSetId,
      created: false,
    }
  }

  console.log('Creating aggregate dataset')
  const createResult = await createDataSet(walletClient, {
    cdn: true,
    payee: providerAddress,
    payer: walletClient.account.address,
    serviceURL: serviceUrl,
    metadata: {
      source: 'filecoin-pin',
      withIPFSIndexing: '',
    },
  })

  console.log(`Waiting for dataset to be created...
    txHash=${createResult.txHash}
    statusUrl=${createResult.statusUrl}
  `)
  const { dataSetId } = await waitForCreateDataSet(createResult)
  console.log(`Dataset created: id=${dataSetId.toString()}`)

  const dataSetInfo = await getDataSet(publicClient, { dataSetId })
  if (!dataSetInfo) {
    throw new Error(`aggregate-submit: aggregate dataset ${dataSetId.toString()} was created but could not be fetched`)
  }

  const numericDataSetId = Number(dataSetId)
  tracking.assignAggregateDataSetId(numericDataSetId)
  return {
    dataSetId: numericDataSetId,
    clientDataSetId: dataSetInfo.clientDataSetId,
    created: true,
  }
}

/**
 * @param {TrackingDb} tracking
 * @param {number} aggregateId
 */
function listAllSubPieces(tracking, aggregateId) {
  /** @type {string[]} */
  const subPieces = []
  let afterPosition = -1

  while (true) {
    const rows = tracking.listAggregateSubPieces(aggregateId, AGGREGATE_SUB_PIECE_BATCH_SIZE, afterPosition)
    if (rows.length === 0) break
    for (const row of rows) subPieces.push(row.subPieceCid)
    afterPosition = rows[rows.length - 1].position
  }

  if (subPieces.length === 0) {
    throw new Error(`aggregate ${aggregateId} has no sub-pieces`)
  }

  return subPieces
}

/**
 * @param {any} walletClient
 * @param {bigint} clientDataSetId
 * @param {{ aggregatePieceCid: string }[]} aggregates
 */
async function signAggregateAdd(walletClient, clientDataSetId, aggregates) {
  return signAddPieces(walletClient, {
    clientDataSetId,
    pieces: aggregates.map((aggregate) => ({
      pieceCid: parsePieceCid(aggregate.aggregatePieceCid),
      metadata: [],
    })),
  })
}

/**
 * @param {object} args
 * @param {string} args.serviceUrl
 * @param {number} args.dataSetId
 * @param {{ aggregatePieceCid: string, subPieces: string[] }[]} args.aggregates
 * @param {`0x${string}`} args.extraData
 */
async function submitAggregateAdd({ serviceUrl, dataSetId, aggregates, extraData }) {
  const response = await fetch(new URL(`pdp/data-sets/${dataSetId}/pieces`, normalizeServiceUrl(serviceUrl)), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      pieces: aggregates.map((aggregate) => ({
        pieceCid: aggregate.aggregatePieceCid,
        subPieces: aggregate.subPieces.map((subPieceCid) => ({ subPieceCid })),
      })),
      extraData,
    }),
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  })

  if (!response.ok) {
    const body = await response.text().catch(() => '')
    throw new Error(`aggregate add request failed with ${response.status}: ${body.slice(0, 1000)}`)
  }

  const location = response.headers.get('Location')
  const txHash = location?.split('/').pop()
  if (!location || !txHash || !isHex(txHash)) {
    throw new Error(`aggregate add response did not include a valid Location tx hash: ${location ?? '<missing>'}`)
  }

  return {
    txHash,
    statusUrl: new URL(location, normalizeServiceUrl(serviceUrl)).toString(),
  }
}

/**
 * @param {object} args
 * @param {TrackingDb} args.tracking
 * @param {any} args.publicClient
 * @param {number} args.dataSetId
 * @param {Map<string, bigint>} args.activePieces
 * @param {{ aggregateId: number, aggregatePieceCid: string, txHash: string | null }} args.aggregate
 * @param {string | null} args.txHash
 */
async function reconcileIfActive({ tracking, publicClient, dataSetId, activePieces, aggregate, txHash }) {
  let pieceId = activePieces.get(aggregate.aggregatePieceCid)
  if (pieceId == null) {
    const refreshed = await listActivePieceMap(publicClient, dataSetId)
    activePieces.clear()
    for (const [cid, id] of refreshed) activePieces.set(cid, id)
    pieceId = activePieces.get(aggregate.aggregatePieceCid)
  }

  if (pieceId == null) return false

  tracking.markAggregateCommitted(
    aggregate.aggregateId,
    dataSetId,
    txHash ?? aggregate.txHash ?? 'already-active',
    pieceId,
  )
  return true
}

/**
 * Poll aggregate rows that already have a submitted transaction from a previous run.
 *
 * @param {object} args
 * @param {TrackingDb} args.tracking
 * @param {any} args.publicClient
 * @param {string} args.serviceUrl
 * @param {number} args.dataSetId
 * @param {Map<string, bigint>} args.activePieces
 * @param {(afterAggregateId: number) => { aggregateId: number, aggregatePieceCid: string, txHash: string | null }[]} args.listAggregates
 * @param {string} args.progressLabel
 */
async function recoverAggregateTransactions({
  tracking,
  publicClient,
  serviceUrl,
  dataSetId,
  activePieces,
  listAggregates,
  progressLabel,
}) {
  const pdpVerifier = /** @type {any} */ (getPdpVerifierContract)(/** @type {any} */ ({ client: publicClient }))
  /** @type {Map<string, { confirmedPieceIds: bigint[] }>} */
  const resolvedTxStatus = new Map()
  let afterAggregateId = 0
  let polled = 0
  let committed = 0
  let failed = 0

  while (true) {
    const aggregates = listAggregates(afterAggregateId)
    if (aggregates.length === 0) break

    for (const aggregate of aggregates) {
      afterAggregateId = aggregate.aggregateId
      if (!aggregate.txHash) continue

      polled += 1
      try {
        let addStatus = resolvedTxStatus.get(aggregate.txHash)
        if (!addStatus) {
          addStatus = await waitForAddPieces({
            statusUrl: addStatusUrl(serviceUrl, dataSetId, aggregate.txHash),
            timeout: WAIT_ADD_PIECES_TIMEOUT_MS,
          })
          await Promise.all(
            addStatus.confirmedPieceIds.map(async (pieceId) => {
              const result = await pdpVerifier.read.getPieceCid([BigInt(dataSetId), pieceId])
              activePieces.set(hexToPieceCID(result.data).toString(), pieceId)
            }),
          )
          resolvedTxStatus.set(aggregate.txHash, addStatus)
        }

        const pieceId = activePieces.get(aggregate.aggregatePieceCid)
        if (pieceId == null) {
          throw new Error(
            `aggregate add ${aggregate.txHash} confirmed but piece ${aggregate.aggregatePieceCid} not found in confirmed set`,
          )
        }

        tracking.markAggregateCommitted(aggregate.aggregateId, dataSetId, aggregate.txHash, pieceId)
        committed += 1
      } catch (err) {
        const landed = await reconcileIfActive({
          tracking,
          publicClient,
          dataSetId,
          activePieces,
          aggregate,
          txHash: aggregate.txHash,
        })
        if (landed) {
          committed += 1
        } else {
          tracking.markAggregateFailed(aggregate.aggregateId, errorMessage(err))
          failed += 1
        }
      }

      renderProgressLine(
        `${progressLabel}: polled ${formatCount(polled)}, committed ${formatCount(committed)}, failed ${formatCount(failed)}`,
      )
    }
  }

  return { committed, failed }
}

/**
 * Check orphaned submitting rows — claimed but with no tx hash — against on-chain
 * state. These rows are not retried because the POST may have reached the provider.
 *
 * @param {object} args
 * @param {TrackingDb} args.tracking
 * @param {any} args.publicClient
 * @param {number} args.dataSetId
 * @param {Map<string, bigint>} args.activePieces
 * @param {number} args.limit
 */
async function reconcileOrphanedSubmissions({ tracking, publicClient, dataSetId, activePieces, limit }) {
  let afterAggregateId = 0
  let committed = 0
  let unknown = 0

  while (true) {
    const aggregates = tracking.listUnsubmittedAggregateClaims(limit, afterAggregateId)
    if (aggregates.length === 0) break

    for (const aggregate of aggregates) {
      afterAggregateId = aggregate.aggregateId

      const landed = await reconcileIfActive({
        tracking,
        publicClient,
        dataSetId,
        activePieces,
        aggregate,
        txHash: null,
      })
      if (landed) {
        committed += 1
        continue
      }

      unknown += 1
    }
  }

  return { committed, unknown }
}

/**
 * Submit one batch of aggregate roots in a single AddPieces request.
 * All rows receive the same transaction hash, while success/failure recovery
 * still reconciles each aggregate root independently.
 *
 * @param {object} args
 * @param {TrackingDb} args.tracking
 * @param {any} args.walletClient
 * @param {any} args.publicClient
 * @param {string} args.serviceUrl
 * @param {{ dataSetId: number, clientDataSetId: bigint }} args.dataSet
 * @param {Map<string, bigint>} args.activePieces
 * @param {{ aggregateId: number, aggregatePieceCid: string, txHash: string | null, subPieces: string[] }[]} args.aggregates
 */
async function submitAggregateBatch({
  tracking,
  walletClient,
  publicClient,
  serviceUrl,
  dataSet,
  activePieces,
  aggregates,
}) {
  /** @type {{ aggregateId: number, aggregatePieceCid: string, txHash: string | null, subPieces: string[] }[]} */
  const claimedAggregates = []
  for (const aggregate of aggregates) {
    if (tracking.claimAggregatePiece(aggregate.aggregateId)) claimedAggregates.push(aggregate)
  }

  if (claimedAggregates.length === 0) {
    return { submitted: 0, addRequests: 0, committed: 0, failed: 0, unknown: 0 }
  }

  let txHash = /** @type {string | null} */ (null)
  let postStarted = false

  try {
    const extraData = await signAggregateAdd(walletClient, dataSet.clientDataSetId, claimedAggregates)
    postStarted = true
    const addResult = await submitAggregateAdd({
      serviceUrl,
      dataSetId: dataSet.dataSetId,
      aggregates: claimedAggregates,
      extraData,
    })
    txHash = addResult.txHash

    for (const aggregate of claimedAggregates) {
      if (!tracking.recordAggregateSubmitTx(aggregate.aggregateId, dataSet.dataSetId, txHash)) {
        throw new Error(
          `aggregate ${aggregate.aggregateId} was not in submitting state when recording tx hash ${txHash}`,
        )
      }
    }

    const addStatus = await waitForAddPieces({
      statusUrl: addResult.statusUrl,
      timeout: WAIT_ADD_PIECES_TIMEOUT_MS,
    })
    if (addStatus.confirmedPieceIds.length < claimedAggregates.length) {
      throw new Error(
        `aggregate add ${txHash} confirmed ${addStatus.confirmedPieceIds.length} piece ids for ${claimedAggregates.length} aggregate pieces`,
      )
    }

    const pieceIds = claimedAggregates.map((_, index) => addStatus.confirmedPieceIds[index])
    if (pieceIds.some((pieceId) => pieceId == null)) {
      throw new Error(`aggregate add ${txHash} succeeded without all confirmed piece ids`)
    }

    for (const [index, aggregate] of claimedAggregates.entries()) {
      const pieceId = /** @type {bigint} */ (pieceIds[index])
      tracking.markAggregateCommitted(aggregate.aggregateId, dataSet.dataSetId, txHash, pieceId)
      activePieces.set(aggregate.aggregatePieceCid, pieceId)
    }

    return {
      submitted: claimedAggregates.length,
      addRequests: 1,
      committed: claimedAggregates.length,
      failed: 0,
      unknown: 0,
    }
  } catch (err) {
    let committed = 0
    let failed = 0
    let unknown = 0

    for (const aggregate of claimedAggregates) {
      const landed = await reconcileIfActive({
        tracking,
        publicClient,
        dataSetId: dataSet.dataSetId,
        activePieces,
        aggregate,
        txHash,
      })
      if (landed) {
        committed += 1
        continue
      }

      if (!txHash && postStarted) {
        unknown += 1
        continue
      }

      if (txHash) {
        tracking.updateAggregateStatus({
          aggregateId: aggregate.aggregateId,
          status: tracking.AGGREGATE_STATUS.failed,
          dataSetId: dataSet.dataSetId,
          txHash,
          lastError: errorMessage(err),
        })
      } else {
        tracking.markAggregateFailed(aggregate.aggregateId, errorMessage(err))
      }
      failed += 1
    }

    return {
      submitted: txHash ? claimedAggregates.length : 0,
      addRequests: txHash ? 1 : 0,
      committed,
      failed,
      unknown,
    }
  }
}

/**
 * @param {object} options
 * @param {string} options.dir
 * @param {`0x${string}`} options.privateKey
 * @param {import('viem').Chain} options.chain
 * @param {string | undefined} options.serviceUrl
 * @param {`0x${string}` | undefined} options.providerAddress
 * @param {number | undefined} options.batchSize
 * @param {number | undefined} options.dataSetId
 * @param {boolean} options.retry
 */
export async function runAggregateSubmit({
  dir,
  privateKey,
  chain,
  serviceUrl,
  providerAddress,
  batchSize,
  dataSetId,
  retry,
}) {
  console.log('\n-------AGGREGATE SUBMIT-------\n')
  const tracking = openTrackingDb(dir)
  const limit = batchSize ?? AGGREGATE_PAGE_SIZE

  let submitted = 0
  let batches = 0
  let committed = 0
  let recovered = 0
  let alreadyActive = 0
  let failed = 0
  let unknown = 0

  try {
    const hasPlannedAggregates = tracking.listPlannedAggregatePieces(1).length > 0
    const hasRecoverableAggregates = tracking.listRecoverableAggregatePieces(1).length > 0
    const hasUnsubmittedClaims = tracking.listUnsubmittedAggregateClaims(1).length > 0
    const hasFailedAggregates = tracking.listFailedAggregatePieces(1).length > 0
    if (
      !hasPlannedAggregates &&
      !hasRecoverableAggregates &&
      !hasUnsubmittedClaims &&
      (!retry || !hasFailedAggregates)
    ) {
      console.log(
        hasFailedAggregates
          ? 'Only failed aggregate pieces remain; rerun with --retry to resubmit retryable failures'
          : 'No planned, submitted, or retryable aggregate pieces to process',
      )
      return
    }

    const metadata = tracking.getMigrationMetadata()
    const resolvedServiceUrl = serviceUrl ?? metadata?.serviceUrl
    const resolvedProviderAddress = providerAddress ?? metadata?.providerAddress
    if (!resolvedServiceUrl)
      throw new Error('aggregate-submit: missing --service-url and no migration metadata service URL')
    if (!resolvedProviderAddress) {
      throw new Error('aggregate-submit: missing --provider-address and no migration metadata provider address')
    }

    const account = privateKeyToAccount(privateKey)
    const publicClient = createPublicClient({ chain, transport: http() })
    const walletClient = createWalletClient({ account, chain, transport: http() })
    const dataSet = await ensureAggregateDataSet({
      tracking,
      walletClient,
      publicClient,
      serviceUrl: resolvedServiceUrl,
      providerAddress: /** @type {`0x${string}`} */ (resolvedProviderAddress),
      requestedDataSetId: dataSetId,
    })

    const activePieces = dataSet.created ? new Map() : await listActivePieceMap(publicClient, dataSet.dataSetId)

    const recoveryResult = await recoverAggregateTransactions({
      tracking,
      publicClient,
      serviceUrl: resolvedServiceUrl,
      dataSetId: dataSet.dataSetId,
      activePieces,
      listAggregates: (afterAggregateId) => tracking.listRecoverableAggregatePieces(limit, afterAggregateId),
      progressLabel: 'Recovering aggregate transactions',
    })
    recovered += recoveryResult.committed
    failed += recoveryResult.failed

    const orphaned = await reconcileOrphanedSubmissions({
      tracking,
      publicClient,
      dataSetId: dataSet.dataSetId,
      activePieces,
      limit,
    })
    recovered += orphaned.committed
    unknown += orphaned.unknown

    const renderSubmitProgress = (progressLabel) => {
      renderProgressLine(
        `${progressLabel}: submitted ${formatCount(submitted)}, committed ${formatCount(committed)}, failed ${formatCount(failed)}`,
      )
    }

    const processRows = async (listRows, progressLabel) => {
      let afterAggregateId = 0
      /** @type {{ aggregateId: number, aggregatePieceCid: string, txHash: string | null, subPieces: string[] }[]} */
      let batch = []

      const flushBatch = async () => {
        if (batch.length === 0) return

        const result = await submitAggregateBatch({
          tracking,
          walletClient,
          publicClient,
          serviceUrl: resolvedServiceUrl,
          dataSet,
          activePieces,
          aggregates: batch,
        })
        submitted += result.submitted
        batches += result.addRequests
        committed += result.committed
        failed += result.failed
        unknown += result.unknown
        batch = []
        renderSubmitProgress(progressLabel)
      }

      while (true) {
        const aggregates = listRows(afterAggregateId)
        if (aggregates.length === 0) break

        for (const aggregate of aggregates) {
          afterAggregateId = aggregate.aggregateId
          if (aggregate.txHash) continue

          const activePieceId = activePieces.get(aggregate.aggregatePieceCid)
          if (activePieceId != null) {
            tracking.markAggregateCommitted(
              aggregate.aggregateId,
              dataSet.dataSetId,
              aggregate.txHash ?? 'already-active',
              activePieceId,
            )
            alreadyActive += 1
          } else {
            batch.push({ ...aggregate, subPieces: listAllSubPieces(tracking, aggregate.aggregateId) })
          }

          if (batch.length >= AGGREGATE_ADD_BATCH_SIZE) await flushBatch()
          renderSubmitProgress(progressLabel)
        }
      }

      await flushBatch()
    }

    await processRows(
      (afterAggregateId) => tracking.listPlannedAggregatePieces(limit, afterAggregateId),
      'Submitting aggregate pieces',
    )

    if (retry) {
      await processRows(
        (afterAggregateId) => tracking.listFailedAggregatePieces(limit, afterAggregateId),
        'Retrying failed aggregate pieces',
      )
    }
  } finally {
    tracking.close()
    finishProgressLine()
  }

  const total = committed + recovered + alreadyActive
  console.log('\nSUMMARY:')
  console.log(`- Submitted to provider: ${formatCount(submitted)} (in ${formatCount(batches)} batches)`)
  console.log(`- Committed this run: ${formatCount(committed)}`)
  if (recovered > 0) console.log(`- Recovered from previous runs: ${formatCount(recovered)}`)
  if (alreadyActive > 0) console.log(`- Already active on-chain: ${formatCount(alreadyActive)}`)
  console.log(`- Total pieces in dataset: ${formatCount(total)}`)
  if (failed > 0) console.log(`- Failed: ${formatCount(failed)}`)
  if (unknown > 0) console.log(`- Unknown POST outcome: ${formatCount(unknown)}`)
}
