import { writeFileSync } from 'node:fs'
import path from 'node:path'

import { createPublicClient, http } from 'viem'

import { openTrackingDb } from '../lib/tracking-db.mjs'

const PDP_VERIFIER = {
  314: '0xBADd0B92C1c71d02E7d520f64c0876538fa2557F',
  314159: '0x85e366Cf9DD2c0aE37E963d9556F5f4718d6417C',
}

const nextPieceIdAbi = [
  {
    type: 'function',
    name: 'getNextPieceId',
    inputs: [{ name: 'setId', type: 'uint256' }],
    outputs: [{ name: '', type: 'uint256' }],
    stateMutability: 'view',
  },
]

/**
 * @param {object} args
 * @param {string} args.dir
 * @param {string} args.serviceUrl
 * @param {import('viem').Chain} [args.chain]
 * @param {string} [args.outFile]
 */
export async function runReportDuplicates({ dir, serviceUrl, chain, outFile }) {
  const tracking = openTrackingDb(dir)
  let dataSetId
  try {
    dataSetId = tracking.getMigrationMetadata()?.dataSetId
  } finally {
    tracking.close()
  }
  if (dataSetId == null) {
    throw new Error('report-duplicates: tracking DB has no data set id')
  }

  const baseUrl = serviceUrl.endsWith('/') ? serviceUrl : `${serviceUrl}/`
  const url = new URL(`pdp/data-sets/${dataSetId}`, baseUrl)
  const response = await fetch(url)
  if (!response.ok) {
    throw new Error(`report-duplicates: GET ${url.toString()} returned ${response.status}`)
  }
  const data = await response.json()

  const idsByCid = new Map()
  for (const piece of data.pieces ?? []) {
    if (!piece?.pieceCid) continue
    const cid = String(piece.pieceCid).toLowerCase()
    let ids = idsByCid.get(cid)
    if (!ids) {
      ids = new Set()
      idsByCid.set(cid, ids)
    }
    ids.add(Number(piece.pieceId))
  }

  let knownIdCount = 0
  for (const idSet of idsByCid.values()) knownIdCount += idSet.size

  let chainNextPieceId = null
  let complete = null
  const verifierAddress = chain ? PDP_VERIFIER[chain.id] : null
  if (chain && verifierAddress) {
    const client = createPublicClient({ chain, transport: http() })
    chainNextPieceId = Number(
      await client.readContract(
        /** @type {any} */ ({
          address: verifierAddress,
          abi: nextPieceIdAbi,
          functionName: 'getNextPieceId',
          args: [BigInt(dataSetId)],
        }),
      ),
    )
    complete = chainNextPieceId === knownIdCount
    if (!complete) {
      console.error(
        `report-duplicates: WARNING: SP listing has ${knownIdCount} piece ids but the contract has assigned ${chainNextPieceId} - the listing is incomplete and this report understates duplicates`,
      )
    }
  }

  const duplicates = []
  let removableCount = 0
  for (const [pieceCid, idSet] of idsByCid) {
    if (idSet.size < 2) continue
    const ids = [...idSet].sort((a, b) => a - b)
    duplicates.push({ pieceCid, keepPieceId: ids[0], removePieceIds: ids.slice(1) })
    removableCount += ids.length - 1
  }
  duplicates.sort((a, b) => a.keepPieceId - b.keepPieceId)

  const target = outFile ?? path.join(dir, `duplicate-pieces-${dataSetId}.json`)
  writeFileSync(
    target,
    JSON.stringify(
      {
        dataSetId,
        generatedAt: new Date().toISOString(),
        pieceEntries: (data.pieces ?? []).length,
        distinctPieceCids: idsByCid.size,
        knownPieceIds: knownIdCount,
        chainNextPieceId,
        listingComplete: complete,
        duplicatePieceCids: duplicates.length,
        removablePieceIds: removableCount,
        duplicates,
      },
      null,
      2,
    ),
  )
  const idsTarget = target.replace(/\.json$/, '.txt')
  writeFileSync(
    idsTarget,
    `${duplicates
      .flatMap((d) => d.removePieceIds)
      .sort((a, b) => a - b)
      .join('\n')}\n`,
  )
  console.log(
    `report-duplicates: data set ${dataSetId}: ${duplicates.length} piece CIDs duplicated, ${removableCount} removable piece ids -> ${target} + ${idsTarget}` +
      (complete === false ? ' (INCOMPLETE - see warning)' : ''),
  )
}
