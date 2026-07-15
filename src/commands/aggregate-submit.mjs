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

const AGGREGATE_SUBMIT_BATCH_SIZE = 100
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
 * @param {string} aggregatePieceCid
 */
async function signAggregateAdd(walletClient, clientDataSetId, aggregatePieceCid) {
  return signAddPieces(walletClient, {
    clientDataSetId,
    pieces: [
      {
        pieceCid: parsePieceCid(aggregatePieceCid),
        metadata: [],
      },
    ],
  })
}

/**
 * @param {object} args
 * @param {string} args.serviceUrl
 * @param {number} args.dataSetId
 * @param {string} args.aggregatePieceCid
 * @param {string[]} args.subPieces
 * @param {`0x${string}`} args.extraData
 */
async function submitAggregateAdd({ serviceUrl, dataSetId, aggregatePieceCid, subPieces, extraData }) {
  const response = await fetch(new URL(`pdp/data-sets/${dataSetId}/pieces`, normalizeServiceUrl(serviceUrl)), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      pieces: [
        {
          pieceCid: aggregatePieceCid,
          subPieces: subPieces.map((subPieceCid) => ({ subPieceCid })),
        },
      ],
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
    for (const entry of refreshed) activePieces.set(entry[0], entry[1])
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
  let afterAggregateId = 0
  let checked = 0
  let committed = 0
  let failed = 0

  while (true) {
    const aggregates = listAggregates(afterAggregateId)
    if (aggregates.length === 0) break

    for (const aggregate of aggregates) {
      afterAggregateId = aggregate.aggregateId
      checked += 1
      if (!aggregate.txHash) continue

      try {
        const addStatus = await waitForAddPieces({
          statusUrl: addStatusUrl(serviceUrl, dataSetId, aggregate.txHash),
          timeout: WAIT_ADD_PIECES_TIMEOUT_MS,
        })
        const [pieceId] = addStatus.confirmedPieceIds
        if (pieceId == null) {
          throw new Error(`aggregate add ${aggregate.txHash} succeeded without a confirmed piece id`)
        }

        tracking.markAggregateCommitted(aggregate.aggregateId, dataSetId, aggregate.txHash, pieceId)
        activePieces.set(aggregate.aggregatePieceCid, pieceId)
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
        `${progressLabel}: checked ${formatCount(checked)}, committed ${formatCount(committed)}, failed ${formatCount(
          failed,
        )}`,
      )
    }
  }

  return { checked, committed, failed }
}

/**
 * Reconcile rows claimed before a tx hash was persisted.
 * These rows are not retried because the POST may have reached the provider.
 *
 * @param {object} args
 * @param {TrackingDb} args.tracking
 * @param {any} args.publicClient
 * @param {number} args.dataSetId
 * @param {Map<string, bigint>} args.activePieces
 * @param {number} args.limit
 */
async function resolveUnsubmittedClaims({ tracking, publicClient, dataSetId, activePieces, limit }) {
  let afterAggregateId = 0
  let checked = 0
  let committed = 0
  const failed = 0
  let unknown = 0

  while (true) {
    const aggregates = tracking.listUnsubmittedAggregateClaims(limit, afterAggregateId)
    if (aggregates.length === 0) break

    for (const aggregate of aggregates) {
      afterAggregateId = aggregate.aggregateId
      checked += 1

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

  return { checked, committed, failed, unknown }
}

/**
 * @param {object} args
 * @param {TrackingDb} args.tracking
 * @param {any} args.walletClient
 * @param {any} args.publicClient
 * @param {string} args.serviceUrl
 * @param {{ dataSetId: number, clientDataSetId: bigint }} args.dataSet
 * @param {Map<string, bigint>} args.activePieces
 * @param {{ aggregateId: number, aggregatePieceCid: string, txHash: string | null }} args.aggregate
 */
async function submitAggregate({ tracking, walletClient, publicClient, serviceUrl, dataSet, activePieces, aggregate }) {
  const activePieceId = activePieces.get(aggregate.aggregatePieceCid)
  if (activePieceId != null) {
    tracking.markAggregateCommitted(
      aggregate.aggregateId,
      dataSet.dataSetId,
      aggregate.txHash ?? 'already-active',
      activePieceId,
    )
    return { skippedActive: 1, submitted: 0, committed: 1, failed: 0, unknown: 0 }
  }

  if (!tracking.claimAggregatePiece(aggregate.aggregateId)) {
    return { skippedActive: 0, submitted: 0, committed: 0, failed: 0, unknown: 0 }
  }

  let txHash = aggregate.txHash
  try {
    const subPieces = listAllSubPieces(tracking, aggregate.aggregateId)
    const extraData = await signAggregateAdd(walletClient, dataSet.clientDataSetId, aggregate.aggregatePieceCid)
    const addResult = await submitAggregateAdd({
      serviceUrl,
      dataSetId: dataSet.dataSetId,
      aggregatePieceCid: aggregate.aggregatePieceCid,
      subPieces,
      extraData,
    })
    txHash = addResult.txHash
    tracking.markAggregateSubmitting(aggregate.aggregateId, dataSet.dataSetId, txHash)

    const addStatus = await waitForAddPieces({
      statusUrl: addResult.statusUrl,
      timeout: WAIT_ADD_PIECES_TIMEOUT_MS,
    })
    const [pieceId] = addStatus.confirmedPieceIds
    if (pieceId == null) {
      throw new Error(`aggregate add ${txHash} succeeded without a confirmed piece id`)
    }

    tracking.markAggregateCommitted(aggregate.aggregateId, dataSet.dataSetId, txHash, pieceId)
    activePieces.set(aggregate.aggregatePieceCid, pieceId)
    return { skippedActive: 0, submitted: 1, committed: 1, failed: 0, unknown: 0 }
  } catch (err) {
    const landed = await reconcileIfActive({
      tracking,
      publicClient,
      dataSetId: dataSet.dataSetId,
      activePieces,
      aggregate,
      txHash,
    })
    if (landed) {
      return { skippedActive: 0, submitted: txHash ? 1 : 0, committed: 1, failed: 0, unknown: 0 }
    }

    if (!txHash) {
      return { skippedActive: 0, submitted: 0, committed: 0, failed: 0, unknown: 1 }
    }

    tracking.markAggregateFailed(aggregate.aggregateId, errorMessage(err))
    return { skippedActive: 0, submitted: 1, committed: 0, failed: 1, unknown: 0 }
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
  const limit = batchSize ?? AGGREGATE_SUBMIT_BATCH_SIZE

  let submitted = 0
  let skippedActive = 0
  let recoveredFromPreviousRuns = 0
  let unknownPostOutcomes = 0
  let committed = 0
  let failed = 0
  let checked = 0

  try {
    const hasPlannedAggregates = tracking.listPlannedAggregatePieces(1).length > 0
    const hasSubmittedAggregates = tracking.listSubmittingAggregatePieces(1).length > 0
    const hasUnsubmittedClaims = tracking.listUnsubmittedAggregateClaims(1).length > 0
    const hasFailedWithTx = tracking.listFailedAggregatePieces(1, 0, true).length > 0
    const hasFailedAggregates = tracking.listFailedAggregatePieces(1).length > 0
    if (
      !hasPlannedAggregates &&
      !hasSubmittedAggregates &&
      !hasUnsubmittedClaims &&
      !hasFailedWithTx &&
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

    const live = await dataSetLive(/** @type {any} */ (publicClient), { dataSetId: BigInt(dataSet.dataSetId) })
    if (!live) throw new Error(`aggregate-submit: aggregate dataset ${dataSet.dataSetId} is not live`)

    const activePieces = dataSet.created ? new Map() : await listActivePieceMap(publicClient, dataSet.dataSetId)

    const recoveredSubmitting = await recoverAggregateTransactions({
      tracking,
      publicClient,
      serviceUrl: resolvedServiceUrl,
      dataSetId: dataSet.dataSetId,
      activePieces,
      listAggregates: (afterAggregateId) => tracking.listSubmittingAggregatePieces(limit, afterAggregateId),
      progressLabel: 'Recovering submitted aggregate pieces',
    })
    checked += recoveredSubmitting.checked
    committed += recoveredSubmitting.committed
    failed += recoveredSubmitting.failed
    recoveredFromPreviousRuns += recoveredSubmitting.committed

    const recoveredFailed = await recoverAggregateTransactions({
      tracking,
      publicClient,
      serviceUrl: resolvedServiceUrl,
      dataSetId: dataSet.dataSetId,
      activePieces,
      listAggregates: (afterAggregateId) => tracking.listFailedAggregatePieces(limit, afterAggregateId, true),
      progressLabel: 'Recovering failed aggregate transactions',
    })
    checked += recoveredFailed.checked
    committed += recoveredFailed.committed
    failed += recoveredFailed.failed
    recoveredFromPreviousRuns += recoveredFailed.committed

    const unsubmitted = await resolveUnsubmittedClaims({
      tracking,
      publicClient,
      dataSetId: dataSet.dataSetId,
      activePieces,
      limit,
    })
    checked += unsubmitted.checked
    committed += unsubmitted.committed
    failed += unsubmitted.failed
    unknownPostOutcomes += unsubmitted.unknown

    const processRows = async (listRows, progressLabel) => {
      let afterAggregateId = 0

      while (true) {
        const aggregates = listRows(afterAggregateId)
        if (aggregates.length === 0) break

        for (const aggregate of aggregates) {
          afterAggregateId = aggregate.aggregateId
          if (aggregate.txHash) continue

          checked += 1
          const result = await submitAggregate({
            tracking,
            walletClient,
            publicClient,
            serviceUrl: resolvedServiceUrl,
            dataSet,
            activePieces,
            aggregate,
          })
          skippedActive += result.skippedActive
          submitted += result.submitted
          committed += result.committed
          failed += result.failed
          unknownPostOutcomes += result.unknown

          renderProgressLine(
            `${progressLabel}: checked ${formatCount(checked)}, submitted ${formatCount(
              submitted,
            )}, committed ${formatCount(committed)}, failed ${formatCount(failed)}`,
          )
        }
      }
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

  console.log('\nSUMMARY:')
  console.log(`- Aggregate pieces checked: ${formatCount(checked)}`)
  console.log(`- Aggregate add requests submitted: ${formatCount(submitted)}`)
  if (skippedActive > 0) console.log(`- Aggregate pieces already active: ${formatCount(skippedActive)}`)
  if (recoveredFromPreviousRuns > 0) {
    console.log(`- Previous aggregate submissions recovered: ${formatCount(recoveredFromPreviousRuns)}`)
  }
  if (unknownPostOutcomes > 0) {
    console.log(`- Aggregate pieces with unknown POST outcome: ${formatCount(unknownPostOutcomes)}`)
  }
  console.log(`- Aggregate pieces committed: ${formatCount(committed)}`)
  if (failed > 0) console.log(`- Aggregate pieces failed: ${formatCount(failed)}`)
}
