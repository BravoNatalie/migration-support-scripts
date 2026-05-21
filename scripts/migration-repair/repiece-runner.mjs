import { calculateFromIterable } from '@filoz/synapse-core/piece'

async function streamPieceCid(url, signal) {
  const res = await fetch(url, { signal, redirect: 'follow' })
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`)
  if (!res.body) throw new Error('no response body')
  async function* iter() {
    for await (const chunk of res.body) yield chunk
  }
  return calculateFromIterable(iter())
}

export async function runRepiece({
  checkpoint,
  candidates,
  concurrency,
  limit,
  summary,
}) {
  const totalCandidates = Number.isFinite(limit)
    ? Math.min(summary.totalCandidates, limit)
    : summary.totalCandidates
  const totalBytes = summary.totalBytes

  console.error(
    `Queue: up to ${totalCandidates} shards to compute (concurrency=${concurrency})`,
  )
  if (totalCandidates === 0) {
    console.error('Nothing to do.')
    return
  }

  let ok = 0
  let failed = 0
  let skipped = 0
  let seen = 0
  let bytesDone = 0n
  const startedAt = Date.now()
  const pending = new Set()

  const tick = () => {
    const elapsed = (Date.now() - startedAt) / 1000
    const mbps = Number(bytesDone) / 1024 / 1024 / Math.max(elapsed, 0.001)
    process.stderr.write(
      `\r[${seen}/${totalCandidates}] ok=${ok} fail=${failed} skip=${skipped} ${(Number(bytesDone) / 1024 ** 3).toFixed(2)}/${(Number(totalBytes) / 1024 ** 3).toFixed(2)} GiB @ ${mbps.toFixed(1)} MiB/s   `,
    )
  }

  const schedule = async (item) => {
    if (checkpoint.hasPiece(item.cid)) {
      skipped++
      seen++
      if (seen % 10 === 0) tick()
      return
    }

    const job = (async () => {
      try {
        const pieceCid = await streamPieceCid(item.sourceURL)
        checkpoint.insertPiece(item.cid, pieceCid.toString(), item.sizeBytes)
        ok++
        bytesDone += item.sizeBytes
      } catch (err) {
        checkpoint.insertFailure(
          item.cid,
          item.sourceURL,
          String(err?.message || err),
        )
        failed++
      } finally {
        seen++
        if (seen % 10 === 0) tick()
      }
    })()

    pending.add(job)
    job.finally(() => pending.delete(job))
    if (pending.size >= concurrency) {
      await Promise.race(pending)
    }
  }

  let emitted = 0
  for await (const item of candidates) {
    if (emitted >= totalCandidates) break
    emitted++
    await schedule(item)
  }
  await Promise.all(pending)
  tick()
  process.stderr.write('\n')
  console.error(`Done. ok=${ok} fail=${failed} skip=${skipped}`)
}
