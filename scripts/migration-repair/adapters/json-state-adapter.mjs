import { readFile, rename, writeFile } from 'node:fs/promises'

function toBigInt(value) {
  if (typeof value === 'bigint') return value
  if (typeof value === 'number') return BigInt(value)
  if (typeof value === 'string') return BigInt(value)
  throw new TypeError(`cannot convert ${typeof value} to bigint`)
}

async function loadJson(path) {
  const buf = await readFile(path)
  return JSON.parse(buf.toString('utf8'))
}

function* walkShards(node) {
  if (node === null || typeof node !== 'object') return
  if (Array.isArray(node)) {
    for (const v of node) yield* walkShards(v)
    return
  }
  if (typeof node.cid === 'string' && (typeof node.sizeBytes === 'string' || typeof node.sizeBytes === 'number')) {
    yield node
  }
  for (const value of Object.values(node)) {
    if (value && typeof value === 'object') yield* walkShards(value)
  }
}

function collectUniqueMissing(json) {
  const unique = new Map()
  let totalShardEntries = 0
  let withPieceCID = 0
  let totalBytesToDownload = 0n

  for (const entry of walkShards(json)) {
    totalShardEntries++
    if (entry.pieceCID) {
      withPieceCID++
      continue
    }
    if (!entry.sourceURL) continue
    if (unique.has(entry.cid)) continue
    const sizeBytes = toBigInt(entry.sizeBytes)
    unique.set(entry.cid, {
      cid: entry.cid,
      root: entry.root,
      sourceURL: entry.sourceURL,
      sizeBytes,
    })
    totalBytesToDownload += sizeBytes
  }

  const missing = [...unique.values()].sort((a, b) =>
    a.sizeBytes < b.sizeBytes ? -1 : a.sizeBytes > b.sizeBytes ? 1 : 0,
  )

  return {
    totalShardEntries,
    withPieceCID,
    missing,
    uniqueMissingCids: missing.length,
    totalBytesToDownload,
  }
}

export const jsonStateAdapter = {
  format: 'json',

  async scan(inputPath, { includeMissing = false } = {}) {
    console.error(`Loading ${inputPath}...`)
    const json = await loadJson(inputPath)
    const result = collectUniqueMissing(json)
    console.error(`Total shard entries: ${result.totalShardEntries}`)
    console.error(`With pieceCID:       ${result.withPieceCID}`)
    console.error(`Missing pieceCID:    ${result.uniqueMissingCids}`)
    console.error(
      `Bytes to download:   ${Number(result.totalBytesToDownload)} (~${(Number(result.totalBytesToDownload) / 1024 ** 3).toFixed(2)} GiB)`,
    )
    return {
      ...result,
      ...(includeMissing ? { missing: result.missing } : {}),
    }
  },

  async *iterateRepairCandidates(inputPath) {
    console.error(`Loading ${inputPath}...`)
    const json = await loadJson(inputPath)
    for (const item of collectUniqueMissing(json).missing) {
      yield item
    }
  },

  async patch(inputPath, checkpoint, { out } = {}) {
    if (!out) throw new Error('JSON patch requires --out')
    console.error(`Loading ${inputPath}...`)
    const json = await loadJson(inputPath)

    let patched = 0
    let stillMissing = 0
    let movedToShards = 0
    const inventories = json?.spacesInventories

    if (inventories && typeof inventories === 'object') {
      for (const inv of Object.values(inventories)) {
        if (!inv || !Array.isArray(inv.shardsToStore)) continue
        if (!Array.isArray(inv.shards)) inv.shards = []
        const keep = []
        for (const entry of inv.shardsToStore) {
          const pieceCID = checkpoint.getPieceCID(entry.cid)
          if (!pieceCID) {
            stillMissing++
            keep.push(entry)
            continue
          }
          entry.pieceCID = pieceCID
          inv.shards.push(entry)
          patched++
          movedToShards++
        }
        inv.shardsToStore = keep
      }
    } else {
      console.error('Warning: no spacesInventories found, falling back to generic walk')
      for (const entry of walkShards(json)) {
        if (entry.pieceCID) continue
        const pieceCID = checkpoint.getPieceCID(entry.cid)
        if (!pieceCID) {
          stillMissing++
          continue
        }
        entry.pieceCID = pieceCID
        patched++
      }
    }

    const tmp = `${out}.tmp`
    await writeFile(tmp, JSON.stringify(json, null, 2))
    await rename(tmp, out)

    return {
      patched,
      stillMissing,
      movedToShards,
      outputPath: out,
      inPlace: false,
    }
  },

  async validate(inputPath) {
    console.error(`Loading ${inputPath}...`)
    const json = await loadJson(inputPath)
    let total = 0
    let missingPiece = 0
    let missingSourceURL = 0
    let badSize = 0
    const samples = { missingPiece: [], missingSourceURL: [], badSize: [] }

    for (const entry of walkShards(json)) {
      total++
      if (!entry.pieceCID) {
        missingPiece++
        if (samples.missingPiece.length < 3) samples.missingPiece.push(entry.cid)
      }
      const url = entry.sourceURL || ''
      if (!url) {
        missingSourceURL++
        if (samples.missingSourceURL.length < 3) {
          samples.missingSourceURL.push(entry.cid)
        }
      }
      const size = Number(entry.sizeBytes)
      if (!Number.isFinite(size) || size <= 0) {
        badSize++
        if (samples.badSize.length < 3) {
          samples.badSize.push({ cid: entry.cid, sizeBytes: entry.sizeBytes })
        }
      }
    }

    let shardsToStoreRemaining = 0
    let shardsCount = 0
    const inventories = json?.spacesInventories
    if (inventories && typeof inventories === 'object') {
      for (const inv of Object.values(inventories)) {
        if (Array.isArray(inv?.shardsToStore)) {
          shardsToStoreRemaining += inv.shardsToStore.length
        }
        if (Array.isArray(inv?.shards)) shardsCount += inv.shards.length
      }
    }

    return {
      total,
      missingPiece,
      missingSourceURL,
      badSize,
      shardsCount,
      shardsToStoreRemaining,
      samples,
      migratable: missingPiece === 0 && missingSourceURL === 0 && badSize === 0 && shardsToStoreRemaining === 0,
    }
  },

  async manual(inputPath, { thresholdBytes } = {}) {
    const threshold = thresholdBytes ?? 1024n ** 3n
    console.error(`Loading ${inputPath}...`)
    const json = await loadJson(inputPath)
    const inventories = json?.spacesInventories
    if (!inventories) throw new Error('no spacesInventories in JSON')

    const skippedUploads = []
    const rootSizes = new Map()

    for (const [space, inv] of Object.entries(inventories)) {
      for (const root of inv.skippedUploads ?? []) {
        const cid = typeof root === 'string' ? root : root.root
        skippedUploads.push({
          space,
          root: cid,
          sizeBytes: typeof root === 'object' && root?.sizeBytes ? Number(root.sizeBytes) : null,
          gatewayURL: `https://trustless-gateway.link/ipfs/${cid}?format=car`,
        })
      }

      for (const arr of [inv.shards ?? [], inv.shardsToStore ?? []]) {
        for (const shard of arr) {
          if (!shard.root) continue
          if (!rootSizes.has(shard.root)) {
            rootSizes.set(shard.root, { space, sizeBytes: 0n, shards: [] })
          }
          const acc = rootSizes.get(shard.root)
          acc.sizeBytes += toBigInt(shard.sizeBytes)
          acc.shards.push({
            cid: shard.cid,
            sourceURL: shard.sourceURL,
            sizeBytes: Number(shard.sizeBytes) || 0,
          })
        }
      }
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
      skippedUploadsBytes: skippedUploads.reduce((sum, item) => sum + (item.sizeBytes || 0), 0),
      largeRootsCount: largeRoots.length,
      largeRootsBytes: largeRoots.reduce((sum, item) => sum + item.sizeBytes, 0),
      skippedUploads,
      largeRoots,
    }
  },
}
