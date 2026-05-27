/**
 * `backup-helper create` — populate tracking.db from space-inventory.db and
 * write the deduplicated aria2 manifest.
 *
 * Flow:
 *   1. Stream every row from the input DB in PK order.
 *   2. Bulk-UPSERT into tracking.db's `shards` table (PK on shard_cid does the
 *      dedup; COALESCE keeps non-NULL piece_cid across conflicting rows).
 *   3. Read the deduplicated shards back from tracking.db in shard_cid order
 *      and write manifest.aria2 atomically via a streaming tmp-rename pattern.
 *
 * Re-runs are idempotent: step 2's UPSERT is a no-op for unchanged input;
 * step 3 produces byte-identical manifest content for the same input.
 */

import { once } from 'node:events'
import fs from 'node:fs'
import fsPromises from 'node:fs/promises'
import { finished } from 'node:stream/promises'

import { openInventoryDb } from '../lib/inventory-db.mjs'
import { manifestPath, shardsDir } from '../lib/layout.mjs'
import { openTrackingDb } from '../lib/tracking-db.mjs'

/**
 * Write to a stream, respecting backpressure: if `stream.write()` returns
 * false we wait for the 'drain' event before resolving.
 *
 * @param {import('node:fs').WriteStream} stream
 * @param {string} chunk
 */
async function writeChunk(stream, chunk) {
  if (!stream.write(chunk)) {
    await once(stream, 'drain')
  }
}

/**
 * @param {object} opts
 * @param {string} opts.db   Absolute path to space-inventory.db
 * @param {string} opts.dir  Absolute path to the output directory
 */
export async function runCreate({ db, dir }) {
  await fsPromises.mkdir(dir, { recursive: true })
  await fsPromises.mkdir(shardsDir(dir), { recursive: true })

  const inventory = openInventoryDb(db)
  const tracking = openTrackingDb(dir)

  try {
    const rowsIngested = tracking.populate(inventory.iterateAllShards())
    const uniqueShards = tracking.countShards()
    console.error(
      `create: ingested ${rowsIngested.toLocaleString()} input rows → ` +
        `${uniqueShards.toLocaleString()} unique shards in tracking.db`,
    )

    /**
     * Write manifest.aria2 as a transitional artifact only.
     * The RPC download flow uses tracking.db as the source of truth, not this file.
     * Keep it for now as an operator/debug artifact, but it can be removed once
     * nothing downstream depends on its presence anymore.
     */

    const dest = manifestPath(dir)
    const tmp = `${dest}.tmp`
    const dst = shardsDir(dir)

    await fsPromises.rm(tmp, { force: true })

    const stream = fs.createWriteStream(tmp)

    let count = 0
    try {
      for (const { shardCid, sourceUrl } of tracking.iterateForManifest()) {
        // Format matches serializeAria2Input from prepare-cars-download:
        //   <sourceURL>
        //    dir=<absolute shards dir>
        //    out=<shardCID>.car
        await writeChunk(stream, `${sourceUrl}\n dir=${dst}\n out=${shardCid}.car\n`)
        count++
      }

      stream.end()
      await finished(stream)

      await fsPromises.rename(tmp, dest)
    } catch (err) {
      stream.destroy()
      try {
        await finished(stream)
      } catch {}
      await fsPromises.rm(tmp, { force: true })
      throw err
    }

    console.error(`create: wrote ${count.toLocaleString()} entries to ${dest}`)
  } finally {
    inventory.close()
    tracking.close()
  }
}
