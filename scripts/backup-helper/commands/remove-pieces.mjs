import { readFileSync, appendFileSync } from 'node:fs'
import path from 'node:path'

import { fromSecp256k1 } from '@filoz/synapse-core/session-key'
import { signSchedulePieceRemovals } from '@filoz/synapse-core/typed-data'
import { getAddress } from 'viem/utils'

import { openTrackingDb } from '../lib/tracking-db.mjs'

const DEFAULT_BATCH_SIZE = 1
const DEFAULT_DELAY_MS = 2_000

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function scheduleDeletions({ serviceUrl, dataSetId, pieceIds, extraData }) {
  const baseUrl = serviceUrl.endsWith('/') ? serviceUrl : `${serviceUrl}/`
  const url = new URL(`pdp/data-sets/${dataSetId}/pieces/${pieceIds[0]}`, baseUrl)
  const response = await fetch(url, {
    method: 'DELETE',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ extraData, pieceIds }),
  })
  if (!response.ok) {
    throw new Error(`DELETE ${url.pathname} returned ${response.status}: ${await response.text()}`)
  }
  const result = await response.json()
  return result.txHash
}

/**
 * @param {object} args
 * @param {string} args.dir
 * @param {string} args.serviceUrl
 * @param {`0x${string}`} args.customerWallet
 * @param {`0x${string}`} args.sessionKey
 * @param {import('viem').Chain} args.chain
 * @param {string} args.idsFile
 * @param {number} [args.limit]
 * @param {number} [args.batchSize]
 * @param {number} [args.delayMs]
 * @param {boolean} [args.skipDone]
 */
export async function runRemovePieces({
  dir,
  serviceUrl,
  customerWallet,
  sessionKey,
  chain,
  idsFile,
  limit,
  batchSize = DEFAULT_BATCH_SIZE,
  delayMs = DEFAULT_DELAY_MS,
  skipDone = true,
}) {
  const tracking = openTrackingDb(dir)
  let metadata
  try {
    metadata = tracking.getMigrationMetadata()
  } finally {
    tracking.close()
  }
  if (metadata?.dataSetId == null || metadata?.clientDataSetId == null) {
    throw new Error('remove-pieces: tracking DB has no data set id / client data set id')
  }
  const { dataSetId, clientDataSetId } = metadata

  const logFile = path.join(dir, `piece-removals-${dataSetId}.ndjson`)
  const done = new Set()
  if (skipDone) {
    try {
      for (const line of readFileSync(logFile, 'utf8').split('\n')) {
        if (!line.trim()) continue
        const entry = JSON.parse(line)
        if (entry.ok) done.add(entry.pieceId)
      }
    } catch {}
  }

  let ids = readFileSync(idsFile, 'utf8')
    .split('\n')
    .map((s) => s.trim())
    .filter(Boolean)
    .map(Number)
    .filter((id) => !done.has(id))
  if (limit != null) ids = ids.slice(0, limit)

  console.log(
    `remove-pieces: data set ${dataSetId}: ${ids.length} piece ids in batches of ${batchSize} (${done.size} already done)`,
  )

  const session = fromSecp256k1({
    privateKey: sessionKey,
    root: getAddress(customerWallet),
    chain,
  })
  await session.syncExpirations()

  let scheduled = 0
  for (let i = 0; i < ids.length; i += batchSize) {
    const batch = ids.slice(i, i + batchSize)
    const extraData = await signSchedulePieceRemovals(session.client, {
      clientDataSetId,
      pieceIds: batch.map(BigInt),
    })
    try {
      const txHash = await scheduleDeletions({ serviceUrl, dataSetId, pieceIds: batch, extraData })
      scheduled += batch.length
      for (const pieceId of batch) {
        appendFileSync(logFile, JSON.stringify({ pieceId, ok: true, txHash, at: Date.now() }) + '\n')
      }
      console.log(`remove-pieces: batch of ${batch.length} scheduled, tx ${txHash} (${scheduled}/${ids.length})`)
    } catch (err) {
      const error = String(err?.message || err)
      for (const pieceId of batch) {
        appendFileSync(logFile, JSON.stringify({ pieceId, ok: false, error, at: Date.now() }) + '\n')
      }
      if (error.includes('Too many removals')) {
        console.log(
          `remove-pieces: proving-period removal cap reached after ${scheduled} this run - rerun after the next proving period to continue`,
        )
        return
      }
      throw new Error(`remove-pieces: batch starting at piece ${batch[0]} failed: ${error}`)
    }
    await sleep(delayMs)
  }

  console.log(`remove-pieces: done. scheduled=${scheduled} log=${logFile}`)
}
