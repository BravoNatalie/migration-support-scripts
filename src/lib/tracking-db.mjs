/**
 * Per-folder SQLite that holds the deduplicated shard inventory and
 * download workflow state. tracking.db is the canonical "things we know about
 * this backup folder" store.
 */

import { DatabaseSync } from 'node:sqlite'

import { trackingDbPath } from './layout.mjs'
import { renderProgressLine } from './progress.mjs'

const DOWNLOAD_STATUS = {
  pending: 'pending',
  queued: 'queued',
  active: 'active',
  complete: 'complete',
  error: 'error',
}

const FAILURE_STAGE = {
  download: 'download',
  prepare: 'prepare',
}

const COMMIT_STATUS = {
  pending: 'pending',
  parked: 'parked',
  committing: 'committing',
  committed: 'committed',
  failed: 'failed',
}

const MIGRATION_STATE = {
  pending: 'pending',
  migrating: 'migrating',
  complete: 'complete',
  incomplete: 'incomplete',
  failed: 'failed',
}

const AGGREGATE_STATUS = {
  planned: 'planned',
  submitting: 'submitting',
  committed: 'committed',
  failed: 'failed',
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

/**
 * @typedef {object} PrepareCandidate
 * @property {string} shardCid
 * @property {string | null} pieceCid
 */

/**
 * @typedef {object} CommitCandidate
 * @property {string} rootCid
 * @property {string} shardCid
 * @property {string} pieceCid
 */

/**
 * @typedef {object} MigrationMetadata
 * @property {string} clientWallet
 * @property {string} serviceUrl
 * @property {string} providerAddress
 * @property {number | null} dataSetId
 * @property {bigint | null} clientDataSetId
 * @property {string} state
 * @property {number} updatedAt
 */

/**
 * @typedef {object} AggregatePiecePlan
 * @property {string} aggregatePieceCid
 * @property {bigint | number} aggregateUsedBytes
 * @property {string[]} subPieceCids
 */

/**
 * @typedef {object} PlannedAggregatePiece
 * @property {number} aggregateId
 * @property {string} aggregatePieceCid
 */

/**
 * @typedef {object} AggregateSubPieceRow
 * @property {number} position
 * @property {string} subPieceCid
 */

function now() {
  return Date.now()
}

/**
 * @param {DatabaseSync} db
 * @param {string} tableName
 * @param {string} columnName
 */
function hasColumn(db, tableName, columnName) {
  const pragma = db.prepare(`PRAGMA table_info(${tableName})`)
  for (const row of pragma.iterate()) {
    if (row.name?.toString() === columnName) return true
  }
  return false
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

    CREATE INDEX IF NOT EXISTS idx_shards_piece_cid
      ON shards(piece_cid);

    CREATE INDEX IF NOT EXISTS idx_shards_download_status
      ON shards(download_status, shard_cid);

    CREATE TABLE IF NOT EXISTS root_shards (
      root_cid   TEXT NOT NULL,
      shard_cid  TEXT NOT NULL,
      PRIMARY KEY (root_cid, shard_cid),
      FOREIGN KEY (shard_cid) REFERENCES shards(shard_cid)
    );

    CREATE TABLE IF NOT EXISTS migration_metadata (
      client_wallet TEXT NOT NULL,
      service_url   TEXT NOT NULL,
      provider_address TEXT NOT NULL,
      data_set_id   INTEGER,
      client_data_set_id TEXT,
      state         TEXT NOT NULL DEFAULT 'pending',
      updated_at    INTEGER NOT NULL
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

    CREATE TABLE IF NOT EXISTS aggregate_pieces (
      aggregate_id        INTEGER PRIMARY KEY,
      data_set_id         INTEGER,
      aggregate_piece_cid TEXT NOT NULL,
      aggregate_used_bytes INTEGER NOT NULL,
      status              TEXT NOT NULL DEFAULT 'planned',
      tx_hash             TEXT,
      piece_id            TEXT,
      last_error          TEXT,
      attempts            INTEGER NOT NULL DEFAULT 0,
      created_at          INTEGER NOT NULL,
      updated_at          INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS aggregate_sub_pieces (
      aggregate_id  INTEGER NOT NULL,
      position      INTEGER NOT NULL,
      sub_piece_cid TEXT NOT NULL,
      PRIMARY KEY (aggregate_id, position),
      FOREIGN KEY (aggregate_id)
        REFERENCES aggregate_pieces(aggregate_id)
        ON DELETE CASCADE
    );

    CREATE UNIQUE INDEX IF NOT EXISTS idx_aggregate_pieces_planned_cid
      ON aggregate_pieces(aggregate_piece_cid)
      WHERE data_set_id IS NULL;

    CREATE INDEX IF NOT EXISTS idx_aggregate_pieces_status
      ON aggregate_pieces(status, aggregate_id);

    CREATE UNIQUE INDEX IF NOT EXISTS idx_aggregate_sub_pieces_cid
      ON aggregate_sub_pieces(sub_piece_cid);
  `)

  if (!hasColumn(db, 'root_shards', 'piece_cid')) {
    db.exec('ALTER TABLE root_shards ADD COLUMN piece_cid TEXT')
  }
  if (!hasColumn(db, 'root_shards', 'commit_status')) {
    db.exec(`ALTER TABLE root_shards ADD COLUMN commit_status TEXT NOT NULL DEFAULT '${COMMIT_STATUS.pending}'`)
  }
  if (!hasColumn(db, 'root_shards', 'commit_attempts')) {
    db.exec('ALTER TABLE root_shards ADD COLUMN commit_attempts INTEGER NOT NULL DEFAULT 0')
  }
  if (!hasColumn(db, 'root_shards', 'last_commit_error')) {
    db.exec('ALTER TABLE root_shards ADD COLUMN last_commit_error TEXT')
  }
  if (!hasColumn(db, 'root_shards', 'tx_hash')) {
    db.exec('ALTER TABLE root_shards ADD COLUMN tx_hash TEXT')
  }
  if (!hasColumn(db, 'root_shards', 'updated_at')) {
    db.exec('ALTER TABLE root_shards ADD COLUMN updated_at INTEGER')
  }

  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_root_shards_piece_cid
     ON root_shards(piece_cid);

    CREATE INDEX IF NOT EXISTS idx_root_shards_commit_status
      ON root_shards(commit_status, piece_cid, root_cid);

    CREATE INDEX IF NOT EXISTS idx_root_shards_shard_cid
      ON root_shards(shard_cid);
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

  const prepareCandidatesStmt = db.prepare(`
    SELECT
      s.shard_cid,
      s.piece_cid
    FROM shards AS s
    WHERE s.download_status = 'complete'
      AND s.shard_cid > ?
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

  const setPieceCidStmt = db.prepare(`
    UPDATE shards
    SET piece_cid = ?, updated_at = ?
    WHERE shard_cid = ?
  `)

  const setRootShardsPieceCidStmt = db.prepare(`
    UPDATE root_shards
    SET piece_cid = ?, updated_at = ?
    WHERE shard_cid = ?
  `)

  const insertMigrationMetadataStmt = db.prepare(`
    INSERT INTO migration_metadata (client_wallet, service_url, provider_address, data_set_id, client_data_set_id, state, updated_at)
    VALUES (?, ?, ?, NULL, NULL, ?, ?)
  `)

  const getMigrationMetadataStmt = db.prepare(`
    SELECT client_wallet, service_url, provider_address, data_set_id, client_data_set_id, state, updated_at
    FROM migration_metadata
    LIMIT 1
  `)

  const updateMigrationStateStmt = db.prepare(`
    UPDATE migration_metadata
    SET state = ?, updated_at = ?
  `)

  const updateMigrationClientDataSetIdStmt = db.prepare(`
    UPDATE migration_metadata
    SET client_data_set_id = COALESCE(client_data_set_id, ?),
        updated_at = ?
  `)

  const updateMigrationMetadataStmt = db.prepare(`
    UPDATE migration_metadata
    SET data_set_id = COALESCE(data_set_id, ?),
        client_data_set_id = COALESCE(client_data_set_id, ?),
        state = ?,
        updated_at = ?
  `)

  const retryCommitRowsStmt = db.prepare(`
    UPDATE root_shards
    SET commit_status = '${COMMIT_STATUS.parked}',
        last_commit_error = NULL,
        tx_hash = NULL,
        updated_at = ?
    WHERE commit_status IN ('${COMMIT_STATUS.failed}', '${COMMIT_STATUS.committing}')
  `)

  const unresolvedCommitPieceCidsStmt = db.prepare(`
    SELECT DISTINCT piece_cid
    FROM root_shards
    WHERE commit_status IN ('${COMMIT_STATUS.failed}', '${COMMIT_STATUS.committing}')
      AND piece_cid IS NOT NULL
  `)

  const parkedCommitPieceCidsStmt = db.prepare(`
    SELECT DISTINCT piece_cid
    FROM root_shards
    WHERE commit_status = '${COMMIT_STATUS.parked}'
      AND piece_cid IS NOT NULL
  `)

  const reconcileCommittedByPieceCidStmt = db.prepare(`
    UPDATE root_shards
    SET commit_status = '${COMMIT_STATUS.committed}',
        last_commit_error = NULL,
        tx_hash = COALESCE(tx_hash, ?),
        updated_at = ?
    WHERE piece_cid = ?
      AND commit_status IN ('${COMMIT_STATUS.failed}', '${COMMIT_STATUS.committing}')
  `)

  const markParkedByPieceCidStmt = db.prepare(`
    UPDATE root_shards
    SET commit_status = '${COMMIT_STATUS.parked}',
        last_commit_error = NULL,
        updated_at = ?
    WHERE piece_cid = ?
      AND commit_status = '${COMMIT_STATUS.pending}'
  `)

  const markPendingCommitPieceErrorStmt = db.prepare(`
    UPDATE root_shards
    SET last_commit_error = ?,
        updated_at = ?
    WHERE piece_cid = ?
      AND commit_status = '${COMMIT_STATUS.pending}'
  `)

  const pendingCommitPieceCidsStmt = db.prepare(`
    SELECT DISTINCT piece_cid
    FROM root_shards
    WHERE commit_status = '${COMMIT_STATUS.pending}'
      AND piece_cid IS NOT NULL
      AND piece_cid > ?
    ORDER BY piece_cid
    LIMIT ?
  `)

  const committedPieceCidsStmt = db.prepare(`
    SELECT DISTINCT piece_cid
    FROM root_shards
    WHERE commit_status = '${COMMIT_STATUS.committed}'
      AND piece_cid IS NOT NULL
      AND piece_cid > ?
    ORDER BY piece_cid
    LIMIT ?
  `)

  const unplannedAvailablePieceCidsStmt = db.prepare(`
    SELECT DISTINCT r.piece_cid
    FROM root_shards AS r
    LEFT JOIN aggregate_sub_pieces AS a
      ON a.sub_piece_cid = r.piece_cid
    WHERE r.commit_status != '${COMMIT_STATUS.pending}'
      AND r.piece_cid IS NOT NULL
      AND r.piece_cid > ?
      AND a.sub_piece_cid IS NULL
    ORDER BY r.piece_cid
    LIMIT ?
  `)

  const committedPieceCidCountStmt = db.prepare(`
    SELECT COUNT(DISTINCT piece_cid) AS n
    FROM root_shards
    WHERE commit_status = '${COMMIT_STATUS.committed}'
      AND piece_cid IS NOT NULL
  `)

  const aggregateSubPieceCountStmt = db.prepare(`
    SELECT COUNT(DISTINCT sub_piece_cid) AS n
    FROM aggregate_sub_pieces
  `)

  const unplannedAvailablePieceCidCountStmt = db.prepare(`
    SELECT COUNT(DISTINCT r.piece_cid) AS n
    FROM root_shards AS r
    LEFT JOIN aggregate_sub_pieces AS a
      ON a.sub_piece_cid = r.piece_cid
    WHERE r.commit_status != '${COMMIT_STATUS.pending}'
      AND r.piece_cid IS NOT NULL
      AND a.sub_piece_cid IS NULL
  `)

  const committedRootCidsStmt = db.prepare(`
    SELECT DISTINCT root_cid
    FROM root_shards
    WHERE commit_status = '${COMMIT_STATUS.committed}'
      AND root_cid > ?
    ORDER BY root_cid
    LIMIT ?
  `)

  const committedPiecesByRootStmt = db.prepare(`
    SELECT DISTINCT piece_cid, tx_hash
    FROM root_shards
    WHERE root_cid = ?
      AND commit_status = '${COMMIT_STATUS.committed}'
      AND piece_cid IS NOT NULL
    ORDER BY piece_cid
  `)

  const insertAggregatePieceStmt = db.prepare(`
    INSERT INTO aggregate_pieces (
      aggregate_piece_cid,
      aggregate_used_bytes,
      status,
      created_at,
      updated_at
    )
    VALUES (?, ?, '${AGGREGATE_STATUS.planned}', ?, ?)
  `)

  const insertAggregateSubPieceStmt = db.prepare(`
    INSERT INTO aggregate_sub_pieces (aggregate_id, position, sub_piece_cid)
    VALUES (?, ?, ?)
  `)

  const plannedAggregatePiecesStmt = db.prepare(`
    SELECT aggregate_id, aggregate_piece_cid
    FROM aggregate_pieces
    WHERE status = '${AGGREGATE_STATUS.planned}'
      AND aggregate_id > ?
    ORDER BY aggregate_id
    LIMIT ?
  `)

  const aggregateSubPiecesStmt = db.prepare(`
    SELECT position, sub_piece_cid
    FROM aggregate_sub_pieces
    WHERE aggregate_id = ?
      AND position > ?
    ORDER BY position
    LIMIT ?
  `)

  const claimCommitCandidatesStmt = db.prepare(`
    SELECT root_cid, shard_cid, piece_cid
    FROM root_shards
    WHERE commit_status = '${COMMIT_STATUS.parked}'
      AND piece_cid IS NOT NULL
    ORDER BY piece_cid, root_cid
    LIMIT ?
  `)

  const markClaimedCommitRowStmt = db.prepare(`
    UPDATE root_shards
    SET commit_status = '${COMMIT_STATUS.committing}',
        updated_at = ?
    WHERE root_cid = ?
      AND shard_cid = ?
      AND commit_status = '${COMMIT_STATUS.parked}'
  `)

  const markCommittedRowStmt = db.prepare(`
    UPDATE root_shards
    SET commit_status = '${COMMIT_STATUS.committed}',
        last_commit_error = NULL,
        tx_hash = ?,
        updated_at = ?
    WHERE root_cid = ?
      AND shard_cid = ?
  `)

  const markCommitFailedRowStmt = db.prepare(`
    UPDATE root_shards
    SET commit_status = '${COMMIT_STATUS.failed}',
        commit_attempts = commit_attempts + 1,
        last_commit_error = ?,
        tx_hash = NULL,
        updated_at = ?
    WHERE root_cid = ?
      AND shard_cid = ?
  `)

  const commitStatsStmt = db.prepare(`
    SELECT
      SUM(CASE WHEN commit_status = '${COMMIT_STATUS.pending}' THEN 1 ELSE 0 END) AS pending,
      SUM(CASE WHEN commit_status = '${COMMIT_STATUS.parked}' THEN 1 ELSE 0 END) AS parked,
      SUM(CASE WHEN commit_status = '${COMMIT_STATUS.committing}' THEN 1 ELSE 0 END) AS committing,
      SUM(CASE WHEN commit_status = '${COMMIT_STATUS.committed}' THEN 1 ELSE 0 END) AS committed,
      SUM(CASE WHEN commit_status = '${COMMIT_STATUS.failed}' THEN 1 ELSE 0 END) AS failed
    FROM root_shards
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
    MIGRATION_STATE,
    AGGREGATE_STATUS,

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

    /**
     * Count distinct shard_cids present in the input inventory db at
     * `inventoryPath` but NOT present in tracking.shards. Returns 0 when every
     * inventory shard is covered.
     *
     * Tracking can be a superset of any single inventory (`create` may be run
     * with multiple inventories targeting the same dir), so equality of counts
     * is not a valid check. This anti-join answers the right question:
     * "is the inventory a subset of tracking?".
     *
     * @param {string} inventoryPath
     */
    countInventoryShardsMissing(inventoryPath) {
      const escaped = inventoryPath.replace(/'/g, "''")
      db.exec(`ATTACH DATABASE '${escaped}' AS inv`)
      try {
        const row = db
          .prepare(
            `SELECT COUNT(DISTINCT inv_s.shard_cid) AS n
             FROM inv.shards AS inv_s
             LEFT JOIN main.shards AS t ON t.shard_cid = inv_s.shard_cid
             WHERE t.shard_cid IS NULL`,
          )
          .get()
        return Number(row.n)
      } finally {
        db.exec('DETACH DATABASE inv')
      }
    },

    /**
     * Count distinct root_cids present in the input inventory db at
     * `inventoryPath` but NOT present in tracking.root_shards. Returns 0 when
     * every inventory root is covered.
     *
     * @param {string} inventoryPath
     */
    countInventoryRootsMissing(inventoryPath) {
      const escaped = inventoryPath.replace(/'/g, "''")
      db.exec(`ATTACH DATABASE '${escaped}' AS inv`)
      try {
        const row = db
          .prepare(
            `SELECT COUNT(DISTINCT inv_s.root_cid) AS n
             FROM inv.shards AS inv_s
             LEFT JOIN main.root_shards AS t ON t.root_cid = inv_s.root_cid
             WHERE t.root_cid IS NULL`,
          )
          .get()
        return Number(row.n)
      } finally {
        db.exec('DETACH DATABASE inv')
      }
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
     * List one stable keyset-paginated batch of completed shards for prepare.
     *
     * @param {number} limit
     * @param {string} afterShardCid
     * @returns {PrepareCandidate[]}
     */
    listPrepareCandidates(limit, afterShardCid) {
      /** @type {PrepareCandidate[]} */
      const candidates = []
      for (const row of prepareCandidatesStmt.iterate(afterShardCid, limit)) {
        candidates.push({
          shardCid: row.shard_cid.toString(),
          pieceCid: row.piece_cid?.toString() || null,
        })
      }
      return candidates
    },

    /**
     * List one stable keyset-paginated batch of distinct committed piece CIDs.
     *
     * @param {number} limit
     * @param {string} afterPieceCid
     * @returns {string[]}
     */
    listCommittedPieceCids(limit, afterPieceCid) {
      return committedPieceCidsStmt.all(afterPieceCid, limit).map((row) => row.piece_cid.toString())
    },

    /**
     * List parked-or-beyond piece CIDs that are not already members of any aggregate plan.
     *
     * @param {number} limit
     * @param {string} afterPieceCid
     * @returns {string[]}
     */
    listUnplannedAvailablePieceCids(limit, afterPieceCid) {
      return unplannedAvailablePieceCidsStmt.all(afterPieceCid, limit).map((row) => row.piece_cid.toString())
    },

    /** Count distinct committed piece CIDs. */
    countCommittedPieceCids() {
      return Number(committedPieceCidCountStmt.get().n || 0)
    },

    /** Count distinct sub-piece CIDs already present in aggregate plans. */
    countAggregateSubPieceCids() {
      return Number(aggregateSubPieceCountStmt.get().n || 0)
    },

    /** Count parked-or-beyond piece CIDs not yet present in any aggregate plan. */
    countUnplannedAvailablePieceCids() {
      return Number(unplannedAvailablePieceCidCountStmt.get().n || 0)
    },

    /**
     * List one stable keyset-paginated batch of distinct committed root CIDs.
     *
     * @param {number} limit
     * @param {string} afterRootCid
     * @returns {string[]}
     */
    listCommittedRootCids(limit, afterRootCid) {
      return committedRootCidsStmt.all(afterRootCid, limit).map((row) => row.root_cid.toString())
    },

    /**
     * List all committed pieces for a single root CID.
     *
     * @param {string} rootCid
     * @returns {{ pieceCid: string, txHash: string | null }[]}
     */
    listCommittedPiecesByRootCid(rootCid) {
      return committedPiecesByRootStmt.all(rootCid).map((row) => ({
        pieceCid: row.piece_cid.toString(),
        txHash: row.tx_hash?.toString() ?? null,
      }))
    },

    /**
     * Insert one planned aggregate piece and its ordered sub-pieces.
     *
     * @param {AggregatePiecePlan} plan
     * @returns {number} aggregate_id
     */
    insertAggregatePiecePlan({ aggregatePieceCid, aggregateUsedBytes, subPieceCids }) {
      const usedBytes = Number(aggregateUsedBytes)
      if (!Number.isSafeInteger(usedBytes) || usedBytes < 1) {
        throw new Error(`aggregate used bytes must be a positive safe integer: ${aggregateUsedBytes}`)
      }

      const timestamp = now()
      db.exec('BEGIN IMMEDIATE')
      try {
        const result = insertAggregatePieceStmt.run(aggregatePieceCid, usedBytes, timestamp, timestamp)
        const aggregateId = Number(result.lastInsertRowid)
        for (const [position, subPieceCid] of subPieceCids.entries()) {
          insertAggregateSubPieceStmt.run(aggregateId, position, subPieceCid)
        }
        db.exec('COMMIT')
        return aggregateId
      } catch (err) {
        db.exec('ROLLBACK')
        throw err
      }
    },

    /**
     * List planned aggregate pieces with stable keyset pagination.
     *
     * @param {number} limit
     * @param {number} afterAggregateId
     * @returns {PlannedAggregatePiece[]}
     */
    listPlannedAggregatePieces(limit, afterAggregateId = 0) {
      return plannedAggregatePiecesStmt.all(afterAggregateId, limit).map((row) => ({
        aggregateId: Number(row.aggregate_id),
        aggregatePieceCid: row.aggregate_piece_cid.toString(),
      }))
    },

    /**
     * List ordered sub-pieces for an aggregate with stable keyset pagination.
     *
     * @param {number} aggregateId
     * @param {number} limit
     * @param {number} afterPosition
     * @returns {AggregateSubPieceRow[]}
     */
    listAggregateSubPieces(aggregateId, limit, afterPosition = -1) {
      return aggregateSubPiecesStmt.all(aggregateId, afterPosition, limit).map((row) => ({
        position: Number(row.position),
        subPieceCid: row.sub_piece_cid.toString(),
      }))
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
     * @param {string} shardCid
     * @param {string} pieceCid
     */
    setPieceCid(shardCid, pieceCid) {
      const timestamp = now()
      db.exec('BEGIN IMMEDIATE')
      try {
        setPieceCidStmt.run(pieceCid, timestamp, shardCid)
        setRootShardsPieceCidStmt.run(pieceCid, timestamp, shardCid)
        db.exec('COMMIT')
      } catch (err) {
        db.exec('ROLLBACK')
        throw err
      }
    },

    /**
     * @param {string} shardCid
     * @param {string} pieceCid
     */
    setRootShardsPieceCid(shardCid, pieceCid) {
      setRootShardsPieceCidStmt.run(pieceCid, now(), shardCid)
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

    /**
     * @param {object} failure
     * @param {string} failure.shardCid
     * @param {string} failure.error
     * @param {boolean} failure.retryable
     */
    markPrepareFailure({ shardCid, error, retryable }) {
      const timestamp = now()
      db.exec('BEGIN IMMEDIATE')
      try {
        insertFailureStmt.run(FAILURE_STAGE.prepare, shardCid, null, null, error, retryable ? 1 : 0, timestamp)
        db.exec('COMMIT')
      } catch (err) {
        db.exec('ROLLBACK')
        throw err
      }
    },

    /**
     * @param {string} shardCid
     */
    clearPrepareFailure(shardCid) {
      clearFailureStmt.run(FAILURE_STAGE.prepare, shardCid)
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

    /**
     * @param {object} metadata
     * @param {string} metadata.clientWallet
     * @param {string} metadata.serviceUrl
     * @param {string} metadata.providerAddress
     */
    initMigrationMetadata({ clientWallet, serviceUrl, providerAddress }) {
      const existing = getMigrationMetadataStmt.get()
      if (!existing) {
        insertMigrationMetadataStmt.run(clientWallet, serviceUrl, providerAddress, MIGRATION_STATE.pending, now())
        return
      }

      if (existing.client_wallet.toString() !== clientWallet) {
        throw new Error(
          `commit: customer wallet does not match existing migration metadata (${existing.client_wallet.toString()})`,
        )
      }

      const existingServiceUrl = existing.service_url.toString()
      if (existingServiceUrl !== serviceUrl) {
        throw new Error(`commit: service URL does not match existing migration metadata (${existingServiceUrl})`)
      }

      const existingProviderAddress = existing.provider_address.toString()
      if (existingProviderAddress !== providerAddress) {
        throw new Error(
          `commit: provider address does not match existing migration metadata (${existingProviderAddress})`,
        )
      }
    },

    /**
     * @returns {MigrationMetadata | null}
     */
    getMigrationMetadata() {
      const row = getMigrationMetadataStmt.get()
      if (!row) return null

      return {
        clientWallet: row.client_wallet.toString(),
        serviceUrl: row.service_url.toString(),
        providerAddress: row.provider_address.toString(),
        dataSetId: row.data_set_id != null ? Number(row.data_set_id) : null,
        clientDataSetId: row.client_data_set_id != null ? BigInt(row.client_data_set_id.toString()) : null,
        state: row.state.toString(),
        updatedAt: Number(row.updated_at),
      }
    },

    /**
     * @param {string} state
     */
    setMigrationState(state) {
      updateMigrationStateStmt.run(state, now())
    },

    /**
     * @param {bigint} clientDataSetId
     */
    setMigrationClientDataSetId(clientDataSetId) {
      updateMigrationClientDataSetIdStmt.run(clientDataSetId.toString(), now())
    },

    /**
     * @param {object} metadata
     * @param {number} metadata.dataSetId
     * @param {bigint} metadata.clientDataSetId
     */
    markMigrationDataSetCreated({ dataSetId, clientDataSetId }) {
      updateMigrationMetadataStmt.run(dataSetId, clientDataSetId.toString(), MIGRATION_STATE.migrating, now())
    },

    resetCommitRowsForRetry() {
      return Number(retryCommitRowsStmt.run(now()).changes || 0)
    },

    listUnresolvedCommitPieceCids() {
      return unresolvedCommitPieceCidsStmt.all().map((row) => row.piece_cid?.toString())
    },

    listParkedCommitPieceCids() {
      return parkedCommitPieceCidsStmt.all().map((row) => row.piece_cid?.toString())
    },

    /**
     * @param {string[]} pieceCids
     * @param {string} txHash
     */
    reconcileCommittedByPieceCids(pieceCids, txHash) {
      if (pieceCids.length === 0) return 0

      const timestamp = now()
      let changed = 0
      db.exec('BEGIN IMMEDIATE')
      try {
        for (const pieceCid of pieceCids) {
          changed += Number(reconcileCommittedByPieceCidStmt.run(txHash, timestamp, pieceCid).changes || 0)
        }
        db.exec('COMMIT')
      } catch (err) {
        db.exec('ROLLBACK')
        throw err
      }
      return changed
    },

    /**
     * @param {string[]} pieceCids
     */
    markParkedByPieceCids(pieceCids) {
      if (pieceCids.length === 0) return 0

      const timestamp = now()
      let changed = 0
      db.exec('BEGIN IMMEDIATE')
      try {
        for (const pieceCid of pieceCids) {
          changed += Number(markParkedByPieceCidStmt.run(timestamp, pieceCid).changes || 0)
        }
        db.exec('COMMIT')
      } catch (err) {
        db.exec('ROLLBACK')
        throw err
      }
      return changed
    },

    /**
     * @param {string} pieceCid
     * @param {string} error
     */
    markPendingCommitPieceError(pieceCid, error) {
      return Number(markPendingCommitPieceErrorStmt.run(error, now(), pieceCid).changes || 0)
    },

    /**
     * @param {number} limit
     * @param {string} afterPieceCid
     * @returns {string[]}
     */
    listPendingCommitPieceCids(limit, afterPieceCid = '') {
      return pendingCommitPieceCidsStmt.all(afterPieceCid, limit).map((row) => row.piece_cid?.toString())
    },

    /**
     * @param {number} limit
     * @returns {CommitCandidate[]}
     */
    claimCommitBatch(limit) {
      /** @type {CommitCandidate[]} */
      const claimed = []
      const timestamp = now()
      db.exec('BEGIN IMMEDIATE')
      try {
        const rows = claimCommitCandidatesStmt.all(limit)
        for (const row of rows) {
          const changes = Number(markClaimedCommitRowStmt.run(timestamp, row.root_cid, row.shard_cid).changes || 0)
          if (changes === 0) continue

          claimed.push({
            rootCid: row.root_cid.toString(),
            shardCid: row.shard_cid.toString(),
            pieceCid: row.piece_cid.toString(),
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
     * @param {CommitCandidate[]} rows
     * @param {string} txHash
     */
    markCommitBatchSucceeded(rows, txHash) {
      const timestamp = now()
      db.exec('BEGIN IMMEDIATE')
      try {
        for (const row of rows) {
          markCommittedRowStmt.run(txHash, timestamp, row.rootCid, row.shardCid)
        }
        db.exec('COMMIT')
      } catch (err) {
        db.exec('ROLLBACK')
        throw err
      }
    },

    /**
     * @param {CommitCandidate[]} rows
     * @param {string} error
     */
    markCommitBatchFailed(rows, error) {
      const timestamp = now()
      db.exec('BEGIN IMMEDIATE')
      try {
        for (const row of rows) {
          markCommitFailedRowStmt.run(error, timestamp, row.rootCid, row.shardCid)
        }
        db.exec('COMMIT')
      } catch (err) {
        db.exec('ROLLBACK')
        throw err
      }
    },

    /** Commit status counts across root_shards. */
    getCommitStats() {
      const row = commitStatsStmt.get()
      return {
        pending: Number(row.pending || 0),
        parked: Number(row.parked || 0),
        committing: Number(row.committing || 0),
        committed: Number(row.committed || 0),
        failed: Number(row.failed || 0),
      }
    },

    close() {
      db.close()
    },
  }
}
