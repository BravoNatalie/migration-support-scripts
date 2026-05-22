/**
 * Per-folder SQLite that holds the deduplicated shard inventory and
 * the failures log. Tracking.db is the canonical "things we know about this
 * backup folder" store.
 */

import { DatabaseSync } from 'node:sqlite'

import { renderProgressLine } from '../../utils.js'
import { trackingDbPath } from './layout.mjs'

/**
 * @typedef {object} ShardForManifest
 * @property {string} shardCid
 * @property {string} sourceUrl
 */

/**
 * @typedef {object} InventoryRow
 * @property {string} shardCid
 * @property {string} sourceUrl
 * @property {number} sizeBytes
 * @property {string | null} pieceCid
 */

/**
 * @param {string} dir  Absolute path to the output directory
 */
export function openTrackingDb(dir) {
  const db = new DatabaseSync(trackingDbPath(dir))

  db.exec('PRAGMA journal_mode = WAL')

  db.exec(`
    CREATE TABLE IF NOT EXISTS shards (
      shard_cid   TEXT PRIMARY KEY,
      source_url  TEXT NOT NULL,
      size_bytes  INTEGER NOT NULL,
      piece_cid   TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_shards_pending
      ON shards(piece_cid) WHERE piece_cid IS NULL;
  `)

  // UPSERT rule: once a non-NULL piece_cid is set keep it.
  const upsert = db.prepare(`
    INSERT INTO shards (shard_cid, source_url, size_bytes, piece_cid)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(shard_cid) DO UPDATE SET
      piece_cid = COALESCE(piece_cid, excluded.piece_cid)
  `)

  const countShardsStmt = db.prepare('SELECT COUNT(*) AS n FROM shards')

  const iterForManifestStmt = db.prepare(`
    SELECT shard_cid, source_url FROM shards ORDER BY shard_cid
  `)

  return {
    /**
     * Bulk-load shard rows from an iterator. Wraps the load in a single
     * IMMEDIATE transaction so large table loads complete fast.
     *
     * @param {Iterable<InventoryRow>} rows
     * @returns {number} count of input rows consumed (NOT unique shards)
     */
    populate(rows) {
      let count = 0
      db.exec('BEGIN IMMEDIATE')
      try {
        for (const row of rows) {
          upsert.run(row.shardCid, row.sourceUrl, row.sizeBytes, row.pieceCid)
          count++
          if (count % 50_000 === 0) {
            renderProgressLine(`populate: ${count} input rows processed`)
          }
        }
        db.exec('COMMIT')
      } catch (err) {
        db.exec('ROLLBACK')
        throw err
      }

      if (count > 0 && count % 50_000 !== 0) {
        renderProgressLine(`populate: ${count.toLocaleString()} input rows processed`)
      }
      if (process.stdout.isTTY) process.stdout.write('\n')
      return count
    },

    /** Total unique shards in tracking.db. */
    countShards() {
      return Number(countShardsStmt.get().n)
    },

    /**
     * Yields one entry per unique shard, in shard_cid order, for manifest
     * writing. Indexed scan over the PK — bounded memory.
     *
     * @returns {Generator<ShardForManifest>}
     */
    *iterateForManifest() {
      for (const row of iterForManifestStmt.iterate()) {
        yield { shardCid: row.shard_cid.toString(), sourceUrl: row.source_url.toString() }
      }
    },

    close() {
      db.close()
    },
  }
}
