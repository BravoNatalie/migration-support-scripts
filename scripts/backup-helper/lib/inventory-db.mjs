/**
 * Read-only accessor for space-inventory.db.
 *
 * Exposes a single iterator over EVERY row of the shards table — no dedup here.
 */

import { DatabaseSync } from 'node:sqlite'

/**
 * @typedef {object} ShardRow
 * @property {string} shardCid
 * @property {string} rootCid
 * @property {string} sourceUrl
 * @property {number} sizeBytes
 * @property {string | null} pieceCid
 */

/**
 * @param {string} dbPath  Absolute path to space-inventory.db
 */
export function openInventoryDb(dbPath) {
  // node:sqlite's DatabaseSync does not interpret SQLite URI filenames
  // (so `file:…?mode=ro` is not honored); use the readOnly option instead.
  const db = new DatabaseSync(dbPath, { readOnly: true })

  const stmt = db.prepare('SELECT shard_cid, root_cid, source_url, size_bytes, piece_cid FROM shards')
  const countDistinctShardsStmt = db.prepare('SELECT COUNT(DISTINCT shard_cid) AS n FROM shards')
  const countDistinctRootsStmt = db.prepare('SELECT COUNT(DISTINCT root_cid) AS n FROM shards')

  return {
    countDistinctShards() {
      return Number(countDistinctShardsStmt.get().n || 0)
    },

    countDistinctRoots() {
      return Number(countDistinctRootsStmt.get().n || 0)
    },

    /**
     * Yields every shards row in PK order. Caller dedupes.
     *
     * @returns {Generator<ShardRow>}
     */
    *iterateAllShards() {
      for (const row of stmt.iterate()) {
        yield {
          shardCid: row.shard_cid.toString(),
          rootCid: row.root_cid.toString(),
          sourceUrl: row.source_url.toString(),
          sizeBytes: Number(row.size_bytes),
          pieceCid: row.piece_cid?.toString() || null,
        }
      }
    },

    close() {
      db.close()
    },
  }
}
