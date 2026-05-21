import { DatabaseSync } from 'node:sqlite'

function toBigInt(value) {
  if (typeof value === 'bigint') return value
  if (typeof value === 'number') return BigInt(value)
  if (typeof value === 'string') return BigInt(value)
  throw new TypeError(`cannot convert ${typeof value} to bigint`)
}

function openStateDb(path) {
  return new DatabaseSync(path)
}

function buildMissingCandidateStatements(db) {
  return {
    countAllShards: db.prepare('SELECT COUNT(*) AS count FROM shards'),
    countWithPieceCID: db.prepare('SELECT COUNT(*) AS count FROM shards WHERE piece_cid IS NOT NULL'),
    iterateUniqueMissing: db.prepare(`
      SELECT s.space_did, s.root_cid, s.shard_cid, s.source_url, s.size_bytes
      FROM shards AS s
      JOIN (
        SELECT shard_cid, MIN(rowid) AS rowid
        FROM shards
        WHERE kind = 'store' AND piece_cid IS NULL AND source_url IS NOT NULL
        GROUP BY shard_cid
      ) AS missing
        ON missing.rowid = s.rowid
      ORDER BY s.size_bytes ASC, s.rowid ASC
    `),
    countStoreEntriesWithoutPiece: db.prepare(`
      SELECT COUNT(*) AS count
      FROM shards
      WHERE kind = 'store' AND piece_cid IS NULL AND source_url IS NOT NULL
    `),
  }
}

export const sqliteStateAdapter = {
  format: 'sqlite',

  async scan(inputPath, { includeMissing = false } = {}) {
    console.error(`Opening ${inputPath}...`)
    const db = openStateDb(inputPath)
    try {
      const stmts = buildMissingCandidateStatements(db)
      const totalShardEntries = stmts.countAllShards.get().count
      const withPieceCID = stmts.countWithPieceCID.get().count
      const missing = []
      let totalBytesToDownload = 0n

      for (const row of stmts.iterateUniqueMissing.iterate()) {
        const item = {
          spaceDID: row.space_did,
          root: row.root_cid,
          cid: row.shard_cid,
          sourceURL: row.source_url,
          sizeBytes: toBigInt(row.size_bytes),
        }
        totalBytesToDownload += item.sizeBytes
        if (includeMissing) missing.push(item)
      }

      const uniqueMissingCids = includeMissing
        ? missing.length
        : db
            .prepare(`
            SELECT COUNT(*) AS count
            FROM (
              SELECT shard_cid
              FROM shards
              WHERE kind = 'store' AND piece_cid IS NULL AND source_url IS NOT NULL
              GROUP BY shard_cid
            )
          `)
            .get().count

      console.error(`Total shard entries: ${totalShardEntries}`)
      console.error(`With pieceCID:       ${withPieceCID}`)
      console.error(`Missing pieceCID:    ${uniqueMissingCids}`)
      console.error(
        `Bytes to download:   ${Number(totalBytesToDownload)} (~${(Number(totalBytesToDownload) / 1024 ** 3).toFixed(2)} GiB)`,
      )

      return {
        totalShardEntries,
        withPieceCID,
        uniqueMissingCids,
        totalBytesToDownload,
        ...(includeMissing ? { missing } : {}),
      }
    } finally {
      db.close()
    }
  },

  async *iterateRepairCandidates(inputPath) {
    console.error(`Opening ${inputPath}...`)
    const db = openStateDb(inputPath)
    try {
      const { iterateUniqueMissing } = buildMissingCandidateStatements(db)
      for (const row of iterateUniqueMissing.iterate()) {
        yield {
          spaceDID: row.space_did,
          root: row.root_cid,
          cid: row.shard_cid,
          sourceURL: row.source_url,
          sizeBytes: toBigInt(row.size_bytes),
        }
      }
    } finally {
      db.close()
    }
  },

  async patch(inputPath, checkpoint, { out } = {}) {
    if (out) {
      throw new Error('SQLite patch operates in place; omit --out')
    }

    console.error(`Opening ${inputPath}...`)
    const db = openStateDb(inputPath)
    console.error('Ensuring patch index...')
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_shards_patch_lookup
      ON shards(shard_cid)
      WHERE kind = 'store' AND piece_cid IS NULL
    `)
    const updateShard = db.prepare(`
      UPDATE shards
      SET piece_cid = ?, kind = 'pull'
      WHERE shard_cid = ?
        AND kind = 'store'
        AND piece_cid IS NULL
    `)
    const countStillMissing = db.prepare(`
      SELECT COUNT(*) AS count
      FROM shards
      WHERE kind = 'store' AND piece_cid IS NULL AND source_url IS NOT NULL
    `)

    const totalCheckpointPieces = checkpoint.countPieces()
    let patched = 0
    let processed = 0
    console.error(`Applying ${totalCheckpointPieces} checkpoint rows...`)

    try {
      db.exec('BEGIN IMMEDIATE')
      for (const row of checkpoint.iteratePieces()) {
        const result = updateShard.run(row.piece_cid, row.cid)
        patched += Number(result.changes || 0)
        processed++
        if (processed % 100_000 === 0) {
          process.stderr.write(
            `Patch progress: processed ${processed}/${totalCheckpointPieces} checkpoint rows, updated ${patched} shard rows\n`,
          )
        }
      }
      db.exec('COMMIT')
    } catch (err) {
      try {
        db.exec('ROLLBACK')
      } catch {
        // best effort rollback
      }
      db.close()
      throw err
    }

    if (processed % 10_000 !== 0 || processed === 0) {
      process.stderr.write(
        `Patch progress: processed ${processed}/${totalCheckpointPieces} checkpoint rows, updated ${patched} shard rows\n`,
      )
    }

    const stillMissing = countStillMissing.get().count
    db.close()

    return {
      patched,
      stillMissing,
      movedToShards: patched,
      inPlace: true,
    }
  },

  async validate(inputPath) {
    console.error(`Opening ${inputPath}...`)
    const db = openStateDb(inputPath)
    try {
      const total = db.prepare('SELECT COUNT(*) AS count FROM shards').get().count
      const missingPiece = db.prepare('SELECT COUNT(*) AS count FROM shards WHERE piece_cid IS NULL').get().count
      const missingSourceURL = db
        .prepare("SELECT COUNT(*) AS count FROM shards WHERE source_url IS NULL OR source_url = ''")
        .get().count
      const badSize = db.prepare('SELECT COUNT(*) AS count FROM shards WHERE size_bytes <= 0').get().count
      const shardsCount = db.prepare("SELECT COUNT(*) AS count FROM shards WHERE kind = 'pull'").get().count
      const shardsToStoreRemaining = db.prepare("SELECT COUNT(*) AS count FROM shards WHERE kind = 'store'").get().count

      const sampleMissingPiece = db.prepare('SELECT shard_cid FROM shards WHERE piece_cid IS NULL LIMIT 3')
      const sampleMissingSource = db.prepare(
        "SELECT shard_cid FROM shards WHERE source_url IS NULL OR source_url = '' LIMIT 3",
      )
      const sampleBadSize = db.prepare('SELECT shard_cid, size_bytes FROM shards WHERE size_bytes <= 0 LIMIT 3')

      return {
        total,
        missingPiece,
        missingSourceURL,
        badSize,
        shardsCount,
        shardsToStoreRemaining,
        samples: {
          missingPiece: [...sampleMissingPiece.iterate()].map((row) => row.shard_cid),
          missingSourceURL: [...sampleMissingSource.iterate()].map((row) => row.shard_cid),
          badSize: [...sampleBadSize.iterate()].map((row) => ({
            cid: row.shard_cid,
            sizeBytes: Number(row.size_bytes),
          })),
        },
        migratable: missingPiece === 0 && missingSourceURL === 0 && badSize === 0 && shardsToStoreRemaining === 0,
      }
    } finally {
      db.close()
    }
  },

  async manual(inputPath, { thresholdBytes } = {}) {
    const threshold = thresholdBytes ?? 1024n ** 3n
    console.error(`Opening ${inputPath}...`)
    const db = openStateDb(inputPath)
    try {
      const skippedUploads = []
      const skippedStmt = db.prepare(`
        SELECT u.space_did, u.root_cid
        FROM uploads AS u
        WHERE u.skipped = 1
        ORDER BY u.rowid
      `)
      for (const row of skippedStmt.iterate()) {
        skippedUploads.push({
          space: row.space_did,
          root: row.root_cid,
          sizeBytes: null,
          gatewayURL: `https://trustless-gateway.link/ipfs/${row.root_cid}?format=car`,
        })
      }

      const rootSizes = new Map()
      const shardStmt = db.prepare(`
        SELECT space_did, root_cid, shard_cid, source_url, size_bytes
        FROM shards
        ORDER BY rowid
      `)
      for (const row of shardStmt.iterate()) {
        if (!rootSizes.has(row.root_cid)) {
          rootSizes.set(row.root_cid, {
            space: row.space_did,
            sizeBytes: 0n,
            shards: [],
          })
        }
        const acc = rootSizes.get(row.root_cid)
        acc.sizeBytes += toBigInt(row.size_bytes)
        acc.shards.push({
          cid: row.shard_cid,
          sourceURL: row.source_url,
          sizeBytes: Number(row.size_bytes),
        })
      }

      const largeRoots = []
      for (const [root, info] of rootSizes) {
        if (info.sizeBytes < threshold) continue
        largeRoots.push({
          root,
          space: info.space,
          sizeBytes: Number(info.sizeBytes),
          shardCount: info.shards.length,
          shards: info.shards,
        })
      }
      largeRoots.sort((a, b) => b.sizeBytes - a.sizeBytes)

      return {
        thresholdBytes: Number(threshold),
        skippedUploadsCount: skippedUploads.length,
        skippedUploadsBytes: 0,
        largeRootsCount: largeRoots.length,
        largeRootsBytes: largeRoots.reduce((sum, item) => sum + item.sizeBytes, 0),
        skippedUploads,
        largeRoots,
      }
    } finally {
      db.close()
    }
  },
}
