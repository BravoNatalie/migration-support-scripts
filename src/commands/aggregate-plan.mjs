/**
 * `backup-helper aggregate-plan` — persist aggregate PieceCID submissions from
 * committed pieces already recorded in tracking.db.
 *
 * This command does not write aggregate CAR bytes. It groups committed
 * sub-pieces into aggregate pieces capped by padded size and stores the
 * provider submission order in tracking.db.
 */

import { parseSubPiece, pieceAggregateCommP } from '../lib/piece-aggregate.mjs'
import { renderProgressLine } from '../lib/progress.mjs'
import { openTrackingDb } from '../lib/tracking-db.mjs'

const MiB = 1024n ** 2n
const GiB = 1024n ** 3n
const TiB = 1024n ** 4n
const DEFAULT_AGGREGATE_SIZE_BYTES = 32n * GiB
const AGGREGATE_DB_BATCH_SIZE = 1_000

/**
 * @typedef {object} InputPiece
 * @property {string} pieceCid
 * @property {number} rawSize
 * @property {bigint} paddedSizeBytes
 */

/**
 * @param {bigint} value
 */
function formatSize(value) {
  const [unit, divisor] = value >= TiB ? ['TiB', TiB] : value >= GiB ? ['GiB', GiB] : ['MiB', MiB]
  return `${(Number(value) / Number(divisor)).toLocaleString('en-US', {
    maximumFractionDigits: 2,
    minimumFractionDigits: 2,
  })} ${unit}`
}

/**
 * @param {number} value
 */
function formatCount(value) {
  return value.toLocaleString('en-US')
}

/**
 * @param {string | undefined} value
 * @param {string} optionName
 */
export function parsePositiveBigInt(value, optionName) {
  if (value == null || value === '') return undefined
  if (!/^[1-9][0-9]*$/.test(value)) {
    throw new Error(`${optionName} must be a positive integer byte count`)
  }
  return BigInt(value)
}

/**
 * @param {string} pieceCid
 * @returns {InputPiece}
 */
function inputPieceFromCid(pieceCid) {
  const parsed = parseSubPiece(pieceCid)
  return {
    pieceCid,
    rawSize: parsed.rawSize,
    paddedSizeBytes: parsed.paddedSizeBytes,
  }
}

/**
 * @param {InputPiece[]} members
 * @param {bigint} aggregateUsedBytes
 */
function buildAggregatePlan(members, aggregateUsedBytes) {
  const aggregate = pieceAggregateCommP(members)
  return {
    aggregatePieceCid: aggregate.rootPieceCid,
    aggregateUsedBytes,
    subPieceCids: aggregate.orderedSubPieceCids,
  }
}

/**
 * @param {object} options
 * @param {string} options.dir
 * @param {bigint} [options.maxSizeBytes]
 */
export async function runAggregatePlan({ dir, maxSizeBytes }) {
  console.log('\n-------AGGREGATE PLAN-------\n')
  const aggregateSizeBytes = maxSizeBytes ?? DEFAULT_AGGREGATE_SIZE_BYTES
  const tracking = openTrackingDb(dir)

  /** @type {InputPiece[]} */
  const oversizedPieces = []
  /** @type {InputPiece[]} */
  let members = []
  let aggregateUsedBytes = 0n
  let aggregateCount = 0
  let scannedPieces = 0
  let plannedPieces = 0
  let totalPaddedSize = 0n
  let afterPieceCid = ''

  const flush = () => {
    if (members.length === 0) return

    const plan = buildAggregatePlan(members, aggregateUsedBytes)
    tracking.insertAggregatePiecePlan(plan)
    aggregateCount += 1
    members = []
    aggregateUsedBytes = 0n
  }

  const processPiece = (piece) => {
    scannedPieces += 1

    if (piece.paddedSizeBytes > aggregateSizeBytes) {
      oversizedPieces.push(piece)
      return
    }

    if (aggregateUsedBytes + piece.paddedSizeBytes > aggregateSizeBytes) {
      flush()
    }

    members.push(piece)
    aggregateUsedBytes += piece.paddedSizeBytes
    plannedPieces += 1
    totalPaddedSize += piece.paddedSizeBytes
  }

  const committedPieceCount = tracking.countCommittedPieceCids()
  const aggregateSubPieceCountBefore = tracking.countAggregateSubPieceCids()
  const unplannedPieceCountBefore = tracking.countUnplannedCommittedPieceCids()

  try {
    while (true) {
      const pieceCids = tracking.listUnplannedCommittedPieceCids(AGGREGATE_DB_BATCH_SIZE, afterPieceCid)
      if (pieceCids.length === 0) break

      for (const pieceCid of pieceCids) {
        processPiece(inputPieceFromCid(pieceCid))
      }

      afterPieceCid = pieceCids[pieceCids.length - 1]
      renderProgressLine(
        `Planning aggregate pieces: - checked ${formatCount(scannedPieces)} of ${formatCount(
          unplannedPieceCountBefore,
        )} unplanned committed pieces - aggregate pieces created: ${formatCount(aggregateCount)} - current aggregate: ${formatSize(
          aggregateUsedBytes,
        )} of ${formatSize(aggregateSizeBytes)}`,
      )
    }

    flush()

    if (oversizedPieces.length > 0) {
      const examples = oversizedPieces
        .slice(0, 5)
        .map((piece) => piece.pieceCid)
        .join(', ')
      throw new Error(`aggregate-plan completed with ${oversizedPieces.length} oversized committed pieces: ${examples}`)
    }
  } catch (err) {
    tracking.close()
    throw err
  }

  const unplannedPieceCountAfter = tracking.countUnplannedCommittedPieceCids()
  tracking.close()
  const fullyMapped = unplannedPieceCountAfter === 0

  if (process.stdout.isTTY) process.stdout.write('\n')

  console.log('\nSUMMARY:')
  console.log(`- Committed pieces to aggregate: ${formatCount(committedPieceCount)}`)
  if (aggregateSubPieceCountBefore > 0) {
    console.log(`- Pieces already aggregated before this run: ${formatCount(aggregateSubPieceCountBefore)}`)
  }
  console.log(`- Pieces aggregated in this run: ${formatCount(plannedPieces)}`)
  if (unplannedPieceCountAfter > 0) {
    console.log(`- Pieces still not aggregated: ${formatCount(unplannedPieceCountAfter)}`)
  }
  console.log(`- Aggregate pieces created: ${formatCount(aggregateCount)}`)
  console.log(`- Total padded size planned in this run: ${formatSize(totalPaddedSize)}`)
  console.log(`- Fully mapped: ${fullyMapped ? 'yes' : 'no'}`)

  if (oversizedPieces.length > 0) {
    console.log(`- Oversized pieces skipped: ${formatCount(oversizedPieces.length)}`)
  }
}
