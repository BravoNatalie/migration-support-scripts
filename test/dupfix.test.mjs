import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { afterEach, describe, expect, it } from 'vitest'

import { resolvePresumedFailure } from '../src/commands/commit.mjs'
import { openTrackingDb } from '../src/lib/tracking-db.mjs'

const cleanups = []
afterEach(() => {
  while (cleanups.length) cleanups.pop()()
})

function makeTrackingWithRows(rows) {
  const dir = mkdtempSync(path.join(tmpdir(), 'dupfix-'))
  cleanups.push(() => rmSync(dir, { recursive: true, force: true }))
  const tracking = openTrackingDb(dir)
  cleanups.push(() => tracking.close())

  const raw = new DatabaseSync(path.join(dir, 'tracking.db'))
  raw.exec('PRAGMA foreign_keys = OFF')
  const insert = raw.prepare(
    'INSERT INTO root_shards (root_cid, shard_cid, piece_cid, commit_status, commit_attempts, updated_at) VALUES (?, ?, ?, ?, 0, 0)',
  )
  for (const row of rows) insert.run(row.rootCid, row.shardCid, row.pieceCid, row.status)
  raw.close()
  return tracking
}

function statusOf(tracking) {
  const stats = tracking.getCommitStats()
  return stats
}

describe('reconcileCommittedByPieceCids', () => {
  it('marks failed and committing rows committed, leaves parked and pending alone', () => {
    const tracking = makeTrackingWithRows([
      { rootCid: 'r1', shardCid: 's1', pieceCid: 'cid-a', status: 'failed' },
      { rootCid: 'r2', shardCid: 's2', pieceCid: 'cid-b', status: 'committing' },
      { rootCid: 'r3', shardCid: 's3', pieceCid: 'cid-c', status: 'parked' },
      { rootCid: 'r4', shardCid: 's4', pieceCid: 'cid-d', status: 'pending' },
    ])

    const changed = tracking.reconcileCommittedByPieceCids(['cid-a', 'cid-b', 'cid-c', 'cid-d'], 'reconciled-on-chain')

    expect(changed).toBe(2)
    expect(statusOf(tracking)).toMatchObject({ committed: 2, parked: 1, pending: 1, failed: 0, committing: 0 })
  })

  it('reconcile-then-sweep never re-parks rows that already landed', () => {
    const tracking = makeTrackingWithRows([
      { rootCid: 'r1', shardCid: 's1', pieceCid: 'cid-landed', status: 'failed' },
      { rootCid: 'r2', shardCid: 's2', pieceCid: 'cid-lost', status: 'failed' },
      { rootCid: 'r3', shardCid: 's3', pieceCid: 'cid-inflight', status: 'committing' },
    ])

    const onChain = new Set(['cid-landed'])
    const unresolved = tracking.listUnresolvedCommitPieceCids()
    const landed = unresolved.filter((cid) => onChain.has(cid))
    tracking.reconcileCommittedByPieceCids(landed, 'reconciled-on-chain')
    const requeued = tracking.resetCommitRowsForRetry()

    expect(requeued).toBe(2)
    expect(statusOf(tracking)).toMatchObject({ committed: 1, parked: 2, failed: 0, committing: 0 })
  })
})

describe('resolvePresumedFailure', () => {
  function makeFakeTracking() {
    const calls = { succeeded: [], failed: [] }
    return {
      calls,
      markCommitBatchSucceeded: (rows, txHash) => calls.succeeded.push({ rows, txHash }),
      markCommitBatchFailed: (rows, error) => calls.failed.push({ rows, error }),
    }
  }

  const rows = [
    { rootCid: 'r1', shardCid: 's1', pieceCid: 'cid-x' },
    { rootCid: 'r2', shardCid: 's2', pieceCid: 'cid-y' },
  ]

  it('marks the batch succeeded when the pieces appear on-chain during the window', async () => {
    const tracking = makeFakeTracking()
    let refreshes = 0
    const onChainCids = new Set()
    const onChain = {
      has: (cid) => onChainCids.has(cid),
      add: () => {},
      refresh: async () => {
        refreshes += 1
        if (refreshes >= 2) {
          onChainCids.add('cid-x')
          onChainCids.add('cid-y')
        }
      },
    }

    const result = await resolvePresumedFailure({
      tracking: /** @type {any} */ (tracking),
      onChain: /** @type {any} */ (onChain),
      rows,
      error: 'Request timed out after 300000ms',
      polls: 4,
      intervalMs: 1,
    })

    expect(result).toEqual({ success: true, lateLanded: true })
    expect(tracking.calls.succeeded).toHaveLength(1)
    expect(tracking.calls.succeeded[0].txHash).toBe('late-landed')
    expect(tracking.calls.failed).toHaveLength(0)
  })

  it('marks the batch failed only after the full window passes with no inclusion', async () => {
    const tracking = makeFakeTracking()
    let refreshes = 0
    const onChain = {
      has: () => false,
      add: () => {},
      refresh: async () => {
        refreshes += 1
      },
    }

    const result = await resolvePresumedFailure({
      tracking: /** @type {any} */ (tracking),
      onChain: /** @type {any} */ (onChain),
      rows,
      error: 'boom',
      polls: 3,
      intervalMs: 1,
    })

    expect(result).toEqual({ success: false })
    expect(refreshes).toBe(3)
    expect(tracking.calls.failed).toHaveLength(1)
    expect(tracking.calls.failed[0].error).toBe('boom')
    expect(tracking.calls.succeeded).toHaveLength(0)
  })

  it('does not mark succeeded when only part of the batch appears', async () => {
    const tracking = makeFakeTracking()
    const onChainCids = new Set(['cid-x'])
    const onChain = {
      has: (cid) => onChainCids.has(cid),
      add: () => {},
      refresh: async () => {},
    }

    const result = await resolvePresumedFailure({
      tracking: /** @type {any} */ (tracking),
      onChain: /** @type {any} */ (onChain),
      rows,
      error: 'partial',
      polls: 2,
      intervalMs: 1,
    })

    expect(result).toEqual({ success: false })
    expect(tracking.calls.failed).toHaveLength(1)
  })
})
