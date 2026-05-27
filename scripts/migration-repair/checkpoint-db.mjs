import { DatabaseSync } from 'node:sqlite'

export function openCheckpointDb(path) {
  const db = new DatabaseSync(path)
  db.exec(`
    CREATE TABLE IF NOT EXISTS pieces (
      cid TEXT PRIMARY KEY,
      piece_cid TEXT NOT NULL,
      size_bytes INTEGER NOT NULL,
      computed_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS failures (
      cid TEXT PRIMARY KEY,
      source_url TEXT NOT NULL,
      error TEXT NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 1,
      last_attempt INTEGER NOT NULL
    );
  `)

  const insertPiece = db.prepare(
    'INSERT OR REPLACE INTO pieces (cid, piece_cid, size_bytes, computed_at) VALUES (?, ?, ?, ?)',
  )
  const insertFailure = db.prepare(`
    INSERT INTO failures (cid, source_url, error, attempts, last_attempt) VALUES (?, ?, ?, 1, ?)
    ON CONFLICT(cid) DO UPDATE SET attempts = attempts + 1, error = excluded.error, last_attempt = excluded.last_attempt
  `)
  const hasPiece = db.prepare('SELECT 1 FROM pieces WHERE cid = ?')
  const lookupPiece = db.prepare('SELECT piece_cid FROM pieces WHERE cid = ?')
  const countPieces = db.prepare('SELECT COUNT(*) AS count FROM pieces')
  const iteratePieces = db.prepare('SELECT cid, piece_cid, size_bytes, computed_at FROM pieces ORDER BY rowid')

  return {
    db,
    insertPiece(cid, pieceCID, sizeBytes) {
      insertPiece.run(cid, pieceCID, Number(sizeBytes), Date.now())
    },
    insertFailure(cid, sourceURL, error) {
      insertFailure.run(cid, sourceURL, error, Date.now())
    },
    hasPiece(cid) {
      return Boolean(hasPiece.get(cid))
    },
    getPieceCID(cid) {
      const row = lookupPiece.get(cid)
      return row?.piece_cid
    },
    countPieces() {
      return countPieces.get().count
    },
    *iteratePieces() {
      for (const row of iteratePieces.iterate()) {
        yield row
      }
    },
    close() {
      db.close()
    },
  }
}
