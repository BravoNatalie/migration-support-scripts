/**
 * Single source of truth for all path conventions in backup-helper.
 * Pure path joiners — no I/O, no side effects.
 * F2/F3/F4 must import from here; never construct paths inline.
 */

import path from 'node:path'

/** @param {string} dir  Output directory passed to --dir */
export const manifestPath = (dir) => path.join(dir, 'manifest.aria2')

/** @param {string} dir */
export const metadataPath = (dir) => path.join(dir, 'metadata.json')

/** @param {string} dir */
export const trackingDbPath = (dir) => path.join(dir, 'tracking.db')

/** @param {string} dir */
export const shardsDir = (dir) => path.join(dir, 'shards')

/**
 * @param {string} dir
 * @param {string} shardCid
 */
export const shardCarPath = (dir, shardCid) => path.join(dir, 'shards', `${shardCid}.car`)

/**
 * @param {string} dir
 * @param {string} pieceCid
 */
export const pieceJsonPath = (dir, pieceCid) => path.join(dir, 'shards', `${pieceCid}.json`)
