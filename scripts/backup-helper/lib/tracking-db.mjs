/**
 * Per-folder SQLite that holds the deduplicated shard inventory and
 * download workflow state. tracking.db is the canonical "things we know about
 * this backup folder" store.
 */

import { DatabaseSync } from 'node:sqlite'

import { renderProgressLine } from '../../utils.js'
import { trackingDbPath } from './layout.mjs'

const DOWNLOAD_STATUS = {
  pending: 'pending',
  queued: 'queued',
  active: 'active',
  complete: 'complete',
  error: 'error',
}

const FAILURE_STAGE = {
  download: 'download',
  compute: 'compute',
}

/**
 * @typedef {object} ShardForManifest
 * @property {string} shardCid
 * @property {string} sourceUrl
 */

/**
 * @typedef {object} InventoryRow
 * @property {string} shardCid
 * @property {string} rootCid
 * @property {string} sourceUrl
 * @property {number} sizeBytes
 * @property {string | null} pieceCid
 */

/**
 * @typedef {object} DownloadCandidate
 * @property {string} shardCid
 * @property {string} sourceUrl
 * @property {string | null} effectiveUrl
 * @property {number} sizeBytes
 */

/**
 * @typedef {object} UnsignedFallbackCandidate
 * @property {string} shardCid
 * @property {string} sourceUrl
 */

function now() {
  return Date.now()
}

/**
 * @param {string} dir  Absolute path to the output directory
 */
export function openTrackingDb(dir) {
  const db = new DatabaseSync(trackingDbPath(dir))

  db.exec('PRAGMA journal_mode = WAL')

  db.exec(`
    CREATE TABLE IF NOT EXISTS shards (
      shard_cid       TEXT PRIMARY KEY,
      source_url      TEXT NOT NULL,
      size_bytes      INTEGER NOT NULL,
      piece_cid       TEXT,
      download_status TEXT NOT NULL DEFAULT 'pending',
      last_gid        TEXT,
      effective_url   TEXT,
      updated_at      INTEGER
    );

    CREATE INDEX IF NOT EXISTS idx_shards_pending
      ON shards(piece_cid) WHERE piece_cid IS NULL;

    CREATE INDEX IF NOT EXISTS idx_shards_download_status
      ON shards(download_status, shard_cid);

    CREATE TABLE IF NOT EXISTS root_shards (
      root_cid   TEXT NOT NULL,
      shard_cid  TEXT NOT NULL,
      PRIMARY KEY (root_cid, shard_cid),
      FOREIGN KEY (shard_cid) REFERENCES shards(shard_cid)
    );

    CREATE TABLE IF NOT EXISTS failures (
      stage         TEXT NOT NULL,
      shard_cid     TEXT NOT NULL,
      url           TEXT,
      status_code   INTEGER,
      error         TEXT NOT NULL,
      retryable     INTEGER NOT NULL DEFAULT 0,
      attempts      INTEGER NOT NULL DEFAULT 1,
      last_attempt  INTEGER NOT NULL,
      PRIMARY KEY (stage, shard_cid)
    );

    CREATE INDEX IF NOT EXISTS idx_failures_shard
      ON failures(shard_cid);
  `)

  const upsert = db.prepare(`
    INSERT INTO shards (shard_cid, source_url, size_bytes, piece_cid)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(shard_cid) DO UPDATE SET
      piece_cid = COALESCE(piece_cid, excluded.piece_cid)
  `)

  const insertRootShard = db.prepare(`
    INSERT OR IGNORE INTO root_shards (root_cid, shard_cid)
    VALUES (?, ?)
  `)

  const countShardsStmt = db.prepare('SELECT COUNT(*) AS n FROM shards')

  const iterForManifestStmt = db.prepare(`
    SELECT shard_cid, source_url FROM shards ORDER BY shard_cid
  `)

  const resetStaleDownloadsStmt = db.prepare(`
    UPDATE shards
    SET download_status = 'pending', last_gid = NULL, updated_at = ?
    WHERE download_status IN ('queued', 'active')
  `)

  const eligibleDownloadsStmt = db.prepare(`
    SELECT
      s.shard_cid,
      s.source_url,
      s.effective_url,
      s.size_bytes
    FROM shards AS s
    WHERE s.download_status = 'pending'
    ORDER BY s.shard_cid
    LIMIT ?
  `)

  const unsignedFallbackCandidatesStmt = db.prepare(`
    SELECT
      s.shard_cid,
      s.source_url
    FROM shards AS s
    JOIN failures AS f
      ON f.stage = 'download'
     AND f.shard_cid = s.shard_cid
    WHERE s.download_status = 'error'
      AND s.effective_url IS NULL
      AND COALESCE(f.retryable, 0) = 1
      AND COALESCE(f.attempts, 0) < ?
      AND f.status_code = 403
    ORDER BY s.shard_cid
    LIMIT ?
  `)

  const queueShardStmt = db.prepare(`
    UPDATE shards
    SET download_status = 'queued', updated_at = ?
    WHERE shard_cid = ?
  `)

  const markActiveStmt = db.prepare(`
    UPDATE shards
    SET download_status = 'active', last_gid = ?, updated_at = ?
    WHERE shard_cid = ?
  `)

  const markCompleteStmt = db.prepare(`
    UPDATE shards
    SET download_status = 'complete', last_gid = NULL, updated_at = ?
    WHERE shard_cid = ?
  `)

  const markPendingStmt = db.prepare(`
    UPDATE shards
    SET download_status = 'pending', last_gid = NULL, updated_at = ?
    WHERE shard_cid = ?
  `)

  const markErrorStmt = db.prepare(`
    UPDATE shards
    SET download_status = 'error', last_gid = NULL, updated_at = ?
    WHERE shard_cid = ?
  `)

  const setEffectiveUrlStmt = db.prepare(`
    UPDATE shards
    SET effective_url = ?, updated_at = ?
    WHERE shard_cid = ?
  `)

  const insertFailureStmt = db.prepare(`
    INSERT INTO failures (stage, shard_cid, url, status_code, error, retryable, attempts, last_attempt)
    VALUES (?, ?, ?, ?, ?, ?, 1, ?)
    ON CONFLICT(stage, shard_cid) DO UPDATE SET
      url = excluded.url,
      status_code = excluded.status_code,
      error = excluded.error,
      retryable = excluded.retryable,
      attempts = failures.attempts + 1,
      last_attempt = excluded.last_attempt
  `)

  const clearFailureStmt = db.prepare(`DELETE FROM failures WHERE stage = ? AND shard_cid = ?`)

  const statsStmt = db.prepare(`
    SELECT
      COUNT(*) AS total,
      SUM(CASE WHEN download_status = '${DOWNLOAD_STATUS.complete}' THEN 1 ELSE 0 END) AS complete,
      SUM(CASE WHEN download_status = '${DOWNLOAD_STATUS.pending}' THEN 1 ELSE 0 END) AS pending,
      SUM(CASE WHEN download_status = '${DOWNLOAD_STATUS.queued}' THEN 1 ELSE 0 END) AS queued,
      SUM(CASE WHEN download_status = '${DOWNLOAD_STATUS.active}' THEN 1 ELSE 0 END) AS active,
      SUM(CASE WHEN download_status = '${DOWNLOAD_STATUS.error}' THEN 1 ELSE 0 END) AS error
    FROM shards
  `)

  return {
    DOWNLOAD_STATUS,

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
          insertRootShard.run(row.rootCid, row.shardCid)
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

    resetStaleDownloads() {
      return Number(resetStaleDownloadsStmt.run(now()).changes || 0)
    },

    /**
     * Claim up to `limit` shards to enqueue in aria2.
     *
     * @param {number} limit
     * @returns {DownloadCandidate[]}
     */
    claimDownloadBatch(limit) {
      /** @type {DownloadCandidate[]} */
      const claimed = []
      const timestamp = now()
      db.exec('BEGIN IMMEDIATE')
      try {
        for (const row of eligibleDownloadsStmt.iterate(limit)) {
          queueShardStmt.run(timestamp, row.shard_cid)
          claimed.push({
            shardCid: row.shard_cid.toString(),
            sourceUrl: row.source_url.toString(),
            effectiveUrl: row.effective_url?.toString() || null,
            sizeBytes: Number(row.size_bytes),
          })
        }
        db.exec('COMMIT')
      } catch (err) {
        db.exec('ROLLBACK')
        throw err
      }
      return claimed
    },

    /**
     * List failed download rows eligible for unsigned fallback reconciliation.
     *
     * @param {number} limit
     * @param {number} maxAttempts
     * @returns {UnsignedFallbackCandidate[]}
     */
    listUnsignedFallbackCandidates(limit, maxAttempts) {
      /** @type {UnsignedFallbackCandidate[]} */
      const candidates = []
      for (const row of unsignedFallbackCandidatesStmt.iterate(maxAttempts, limit)) {
        candidates.push({
          shardCid: row.shard_cid.toString(),
          sourceUrl: row.source_url.toString(),
        })
      }
      return candidates
    },

    /**
     * @param {string} shardCid
     * @param {string} gid
     */
    markActive(shardCid, gid) {
      markActiveStmt.run(gid, now(), shardCid)
    },

    /**
     * @param {string} shardCid
     */
    markComplete(shardCid) {
      const timestamp = now()
      db.exec('BEGIN IMMEDIATE')
      try {
        markCompleteStmt.run(timestamp, shardCid)
        clearFailureStmt.run(FAILURE_STAGE.download, shardCid)
        db.exec('COMMIT')
      } catch (err) {
        db.exec('ROLLBACK')
        throw err
      }
    },

    /**
     * @param {string} shardCid
     */
    requeuePending(shardCid) {
      markPendingStmt.run(now(), shardCid)
    },

    /**
     * @param {string} shardCid
     * @param {string} effectiveUrl
     */
    setEffectiveUrl(shardCid, effectiveUrl) {
      setEffectiveUrlStmt.run(effectiveUrl, now(), shardCid)
    },

    /**
     * @param {object} failure
     * @param {string} failure.shardCid
     * @param {string | null} failure.url
     * @param {number | null} failure.statusCode
     * @param {string} failure.error
     * @param {boolean} failure.retryable
     */
    markFailure({ shardCid, url, statusCode, error, retryable }) {
      const timestamp = now()
      db.exec('BEGIN IMMEDIATE')
      try {
        markErrorStmt.run(timestamp, shardCid)
        insertFailureStmt.run(FAILURE_STAGE.download, shardCid, url, statusCode, error, retryable ? 1 : 0, timestamp)
        db.exec('COMMIT')
      } catch (err) {
        db.exec('ROLLBACK')
        throw err
      }
    },

    getDownloadStats() {
      const row = statsStmt.get()
      return {
        total: Number(row.total || 0),
        complete: Number(row.complete || 0),
        pending: Number(row.pending || 0),
        queued: Number(row.queued || 0),
        active: Number(row.active || 0),
        error: Number(row.error || 0),
      }
    },

    close() {
      db.close()
    },
  }
}
