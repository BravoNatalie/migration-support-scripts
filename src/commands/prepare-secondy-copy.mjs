/**
 * `backup-helper prepare-secondy-copy` — aggregate non-pending pieces via
 * `curio toolbox aggregate-pieces` for secondary copy.
 *
 * Writes all non-pending piece CIDs from tracking.db to an input text file
 * (one PieceCIDv2 per line), then invokes the Curio binary which reads
 * source subpieces, packs them into aggregate pieces up to 1 GiB, and writes
 * a result JSON to --result. The command validates that file and logs a
 * summary. It does not modify tracking.db.
 *
 * The Curio binary is retryable: it stores deterministic resume state under
 * <target>/storacha-aggregate-work/<input-sha256> and picks up where it left
 * off on re-run. Re-run this command until it exits cleanly. On each exit
 * (even non-zero) read the result file — it may contain aggregates completed
 * before the failure.
 */

import fs from 'node:fs/promises'

import { execa } from 'execa'
import { z } from 'zod'

import { secondaryCopyInputPath, secondaryCopyResultPath } from '../lib/layout.mjs'
import { openTrackingDb } from '../lib/tracking-db.mjs'

const PIECE_CID_BATCH_SIZE = 1_000

const secondaryCopyResultSchema = z.object({
  aggregates: z
    .array(
      z.object({
        piece_cid: z.string(),
        sub_pieces: z.array(z.string()),
        count: z.number().int().nonnegative(),
      }),
    )
    .nullable()
    .transform((aggregates) => aggregates ?? []),
  error: z
    .string()
    .nullable()
    .optional()
    .transform((error) => error ?? null),
})

/**
 * @param {string} dir
 * @returns {Promise<number>} number of piece CIDs written
 */
async function writeInputFile(dir) {
  const tracking = openTrackingDb(dir)
  const inputPath = secondaryCopyInputPath(dir)
  const fh = await fs.open(inputPath, 'w')
  let count = 0
  try {
    let afterPieceCid = ''
    while (true) {
      const batch = tracking.listNonPendingPieceCids(PIECE_CID_BATCH_SIZE, afterPieceCid)
      if (batch.length === 0) break
      await fh.write(batch.join('\n') + '\n')
      count += batch.length
      afterPieceCid = batch[batch.length - 1]
    }
  } finally {
    await fh.close()
    tracking.close()
  }
  return count
}

/**
 * @param {object} args
 * @param {string} args.dir
 * @param {string} args.source
 * @param {string} args.target
 */
export async function runPrepareSecondaryCopy({ dir, source, target }) {
  const inputPath = secondaryCopyInputPath(dir)
  const resultPath = secondaryCopyResultPath(dir)

  const count = await writeInputFile(dir)
  if (count === 0) {
    console.error('prepare-secondy-copy: nothing to do — no non-pending piece CIDs found')
    return
  }
  console.error(`prepare-secondy-copy: wrote ${count} piece CIDs to ${inputPath}`)

  let commandError = null
  try {
    await execa({
      env: {
        LANG: 'en_US.UTF-8',
        GOLOG_LOG_LEVEL: 'error',
      },
    })('curio', [
      'toolbox',
      'aggregate-pieces',
      '--input',
      inputPath,
      '--source',
      source,
      '--target',
      target,
      '--result',
      resultPath,
    ])
  } catch (err) {
    commandError = err?.stderr || err?.stdout || err?.message || String(err)
  }

  // Always read the result file — completed aggregates are written even on failure.
  let parsed
  try {
    const content = await fs.readFile(resultPath, 'utf8')
    parsed = JSON.parse(content)
  } catch (err) {
    throw new Error(`prepare-secondy-copy: binary returned invalid result file: ${err?.message || err}`)
  }

  const result = secondaryCopyResultSchema.parse(parsed)

  console.error(
    `prepare-secondy-copy: aggregates=${result.aggregates.length} total-sub-pieces=${result.aggregates.reduce((n, a) => n + a.count, 0)}`,
  )

  if (result.error) {
    console.error(`prepare-secondy-copy: binary reported error: ${result.error}`)
  } else if (commandError) {
    console.error(`prepare-secondy-copy: binary stderr: ${commandError}`)
  }

  if (result.error || commandError) {
    console.error(`prepare-secondy-copy: run did not complete cleanly — re-run to resume from Curio work directory`)
    process.exit(1)
  }

  console.error(`prepare-secondy-copy: done. result written to ${resultPath}`)
}
