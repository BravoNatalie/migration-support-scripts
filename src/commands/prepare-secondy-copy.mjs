/**
 * `backup-helper prepare-secondy-copy` — export all non-pending piece CIDs
 * to the SP via an external curio toolbox binary.
 *
 * Collects every piece CID from root_shards where commit_status is not
 * 'pending' and passes them to the curio binary. The binary writes a result
 * JSON to secondary-copy-pieces.json in the --dir directory. The command
 * validates that file and logs a summary; it does not modify tracking.db.
 */

import fs from 'node:fs/promises'

import { execa } from 'execa'
import { z } from 'zod'

import { secondaryCopyResultPath } from '../lib/layout.mjs'
import { openTrackingDb } from '../lib/tracking-db.mjs'

// TODO: replace with the actual curio toolbox subcommand name
const CURIO_SUBCOMMAND = 'REPLACE_WITH_ACTUAL_SUBCOMMAND'

const PIECE_CID_BATCH_SIZE = 1_000

const secondaryCopyResultSchema = z
  .object({
    count: z.number().int().nonnegative(),
    pieces: z
      .array(z.string())
      .nullable()
      .transform((pieces) => pieces ?? []),
    error: z
      .string()
      .nullable()
      .optional()
      .transform((error) => error ?? null),
  })
  .refine((value) => value.count === value.pieces.length, {
    message: 'count must match pieces length',
  })

/**
 * @param {string} dir
 * @returns {Promise<string[]>}
 */
async function collectNonPendingPieceCids(dir) {
  const tracking = openTrackingDb(dir)
  try {
    const all = []
    let afterPieceCid = ''
    while (true) {
      const batch = tracking.listNonPendingPieceCids(PIECE_CID_BATCH_SIZE, afterPieceCid)
      if (batch.length === 0) break
      all.push(...batch)
      afterPieceCid = batch[batch.length - 1]
    }
    return all
  } finally {
    tracking.close()
  }
}

/**
 * @param {object} args
 * @param {string} args.dir
 * @param {string} args.target
 */
export async function runPrepareSecondaryCopy({ dir, target }) {
  const pieceCids = await collectNonPendingPieceCids(dir)

  if (pieceCids.length === 0) {
    console.error('prepare-secondy-copy: nothing to do — no non-pending piece CIDs found')
    return
  }

  console.error(`prepare-secondy-copy: collected ${pieceCids.length} piece CIDs`)

  const resultPath = secondaryCopyResultPath(dir)

  // TODO: update flags when the curio subcommand interface is confirmed
  let commandError = null
  try {
    await execa({
      env: {
        LANG: 'en_US.UTF-8',
        GOLOG_LOG_LEVEL: 'error',
      },
    })('curio', [
      'toolbox',
      CURIO_SUBCOMMAND,
      '--target',
      target,
      '--result',
      resultPath,
      '--pieces',
      pieceCids.join(','),
    ])
  } catch (err) {
    commandError = err?.stderr || err?.stdout || err?.message || String(err)
  }

  let parsed
  try {
    const content = await fs.readFile(resultPath, 'utf8')
    parsed = JSON.parse(content)
  } catch (err) {
    throw new Error(`prepare-secondy-copy: binary returned invalid result file: ${err?.message || err}`)
  }

  const result = secondaryCopyResultSchema.parse(parsed)

  if (commandError && !result.error) {
    console.error(`prepare-secondy-copy: binary stderr: ${commandError}`)
  }
  if (result.error) {
    console.error(`prepare-secondy-copy: binary reported error: ${result.error}`)
  }

  console.error(`prepare-secondy-copy: done. count=${result.count} pieces=${result.pieces.length}`)
}
