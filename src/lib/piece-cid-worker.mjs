/**
 * Worker thread for `backup-helper prepare`: computes a piece CID from a local
 * CAR file using the same `@filoz/synapse-core/piece` hash as the main thread,
 * so the CPU-bound (pure-JS) CommP hashing can run in parallel across cores.
 *
 * Local addition (not upstream) — parallelises prepare's hashing. The hash
 * itself is unchanged, so output piece CIDs are identical to the single-thread
 * path; only the throughput differs.
 */
import { createReadStream } from 'node:fs'
import { parentPort } from 'node:worker_threads'

import { calculateFromIterable } from '@filoz/synapse-core/piece'

if (!parentPort) {
  throw new Error('piece-cid-worker.mjs must be run as a worker thread')
}

parentPort.on('message', async ({ carPath }) => {
  const stream = createReadStream(carPath)
  try {
    const pieceCid = await calculateFromIterable(stream)
    parentPort.postMessage({ pieceCid: pieceCid.toString() })
  } catch (err) {
    parentPort.postMessage({ error: String(err?.message || err) })
  } finally {
    stream.destroy()
  }
})
