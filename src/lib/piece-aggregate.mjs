import { createHash } from 'node:crypto'

import * as Piece from '@web3-storage/data-segment/piece'
import { Unpadded } from '@web3-storage/data-segment/piece/size'

const NODE_SIZE = 32

/**
 * @typedef {object} AggregateSubPiece
 * @property {string} pieceCid
 * @property {number} rawSize
 */

/**
 * @typedef {AggregateSubPiece & {
 *   height: number,
 *   root: Uint8Array,
 *   paddedSizeBytes: bigint,
 * }} ParsedSubPiece
 */

/**
 * @typedef {object} PieceAggregate
 * @property {string} rootPieceCid
 * @property {string[]} orderedSubPieceCids
 * @property {number} rawSize
 */

/**
 * Filecoin piece tree node: SHA-256(left || right), truncated to 254 bits.
 *
 * @param {Uint8Array} left
 * @param {Uint8Array} right
 */
function computeNode(left, right) {
  const digest = createHash('sha256').update(left).update(right).digest()
  digest[NODE_SIZE - 1] &= 0b0011_1111
  return new Uint8Array(digest)
}

const zeroCache = [new Uint8Array(NODE_SIZE)]

/**
 * @param {number} level
 */
function zeroComm(level) {
  while (zeroCache.length <= level) {
    const previous = zeroCache[zeroCache.length - 1]
    zeroCache.push(computeNode(previous, previous))
  }
  return zeroCache[level]
}

/**
 * @param {string} pieceCid
 * @returns {ParsedSubPiece}
 */
export function parseSubPiece(pieceCid) {
  const piece = Piece.fromString(pieceCid)
  const rawSize = Unpadded.fromPiece({ height: piece.height, padding: piece.padding })

  if (rawSize > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error(`PieceCID raw size exceeds maximum safe integer: ${pieceCid}`)
  }

  return {
    pieceCid,
    rawSize: Number(rawSize),
    height: piece.height,
    root: piece.root,
    paddedSizeBytes: piece.size,
  }
}

/**
 * Order sub-pieces in the layout expected by Curio's aggregate commitment
 * calculation.
 *
 * Pieces are sorted by descending tree height so larger padded pieces are placed
 * first. Equal-height pieces keep their original input order because their
 * left-to-right position affects the aggregate root.
 *
 * @param {AggregateSubPiece[]} subPieces
 */
export function orderSubPiecesForAggregate(subPieces) {
  return subPieces
    .map((subPiece, index) => ({ ...subPiece, index, ...parseSubPiece(subPiece.pieceCid) }))
    .sort((a, b) => b.height - a.height || a.index - b.index)
}

/**
 * Compute the aggregate PieceCID over sub-piece commitments, matching Curio's
 * PieceAggregateCommP layout: largest padded pieces first, then zero-padded to
 * reduce to one root.
 *
 * @param {AggregateSubPiece[]} subPieces
 * @returns {PieceAggregate}
 */
export function pieceAggregateCommP(subPieces) {
  if (subPieces.length === 0) {
    throw new Error('cannot aggregate an empty sub-piece list')
  }

  const entries = orderSubPiecesForAggregate(subPieces)
  /** @type {Array<{ height: number, node: Uint8Array }>} */
  const stack = []

  for (const entry of entries) {
    let current = { height: entry.height, node: entry.root }
    while (stack.length > 0 && stack[stack.length - 1].height === current.height) {
      const left = stack.pop()
      current = { height: current.height + 1, node: computeNode(left.node, current.node) }
    }
    stack.push(current)
  }

  while (stack.length > 1) {
    let right = stack.pop()
    const left = stack[stack.length - 1]
    while (right.height < left.height) {
      right = { height: right.height + 1, node: computeNode(right.node, zeroComm(right.height)) }
    }
    stack.pop()
    stack.push({ height: left.height + 1, node: computeNode(left.node, right.node) })
  }

  const rawSize = subPieces.reduce((sum, subPiece) => sum + subPiece.rawSize, 0)

  const rootPieceCid = Piece.toLink({
    root: stack[0].node,
    height: Unpadded.toHeight(BigInt(rawSize)),
    padding: Unpadded.toPadding(BigInt(rawSize)),
  }).toString()

  return {
    rootPieceCid,
    orderedSubPieceCids: entries.map((entry) => entry.pieceCid),
    rawSize,
  }
}
