import fs from 'node:fs'
import path from 'node:path'
import process from 'node:process'
import { z } from 'zod'

// ---------------------------------------------------------------------------
// Atomic file write
// ---------------------------------------------------------------------------

/**
 * Write `content` to `destPath` atomically via a tmp-rename pattern.
 * Callers are responsible for ensuring `destPath` does not already exist
 * (refuse-overwrite semantics live at the call site, not here).
 *
 * @param {string} destPath
 * @param {string} content
 */
export async function writeFileAtomic(destPath, content) {
  const tmpPath = `${destPath}.tmp`
  await fs.promises.mkdir(path.dirname(destPath), { recursive: true })
  await fs.promises.rm(tmpPath, { force: true })
  try {
    await fs.promises.writeFile(tmpPath, content)
    await fs.promises.rename(tmpPath, destPath)
  } finally {
    await fs.promises.rm(tmpPath, { force: true })
  }
}

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

/**
 * @param {string} filePath
 */
export async function pathExists(filePath) {
  try {
    await fs.promises.access(filePath)
    return true
  } catch (error) {
    if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') {
      return false
    }

    throw error
  }
}

// ---------------------------------------------------------------------------
// Run-dir lock
// ---------------------------------------------------------------------------

/**
 * @template T
 * @param {string} runDir
 * @param {string} operation
 * @param {() => Promise<T>} action
 * @returns {Promise<T>}
 */
export async function withRunDirLock(runDir, operation, action) {
  await fs.promises.mkdir(runDir, { recursive: true })
  const lockPath = path.join(runDir, '.lock')
  let handle

  try {
    handle = await fs.promises.open(lockPath, 'wx')
  } catch (error) {
    if (error && typeof error === 'object' && 'code' in error && error.code === 'EEXIST') {
      throw new Error(
        `${operation}: run directory is locked by another process or a stale lock file: ${runDir}. If the previous run crashed and no process is still using this run directory, remove ${lockPath} and retry.`,
      )
    }

    throw error
  }

  try {
    await handle.writeFile(
      `${JSON.stringify(
        {
          operation,
          pid: process.pid,
          createdAt: new Date().toISOString(),
        },
        null,
        2,
      )}\n`,
    )
    return await action()
  } finally {
    await handle.close()
    await fs.promises.rm(lockPath, { force: true })
  }
}

// ---------------------------------------------------------------------------
// Zod schemas
// ---------------------------------------------------------------------------

/**
 * A decimal-string representation of a positive non-zero bigint (no leading
 * zeros).  Stored on disk as a string because JSON cannot round-trip bigints.
 */
export const DecimalBigIntString = z
  .string()
  .regex(/^[1-9]\d*$/, 'must be a positive decimal integer string (no leading zeros)')

/**
 * Metadata sidecar written by export-upload-roots.
 *
 * @example { spaceDID: "did:key:z...", rootCount: 3, rootDigest: "abc..." }
 */
export const MetaSchema = z.object({
  spaceDID: z.string().min(1),
  rootCount: z
    .number()
    .int()
    .min(0)
    .refine((v) => Number.isSafeInteger(v), 'must be a safe integer'),
  rootDigest: z.string().regex(/^[a-f0-9]{64}$/, 'must be a 64-char hex sha256'),
})

/**
 * Queue descriptor written into `queue/pending/<batch-id>.json` by
 * split-upload-roots.
 */
export const QueueDescriptorSchema = z.object({
  version: z.literal(1),
  batchId: z.string().min(1),
  spaceDID: z.string().min(1),
  selectedRootsFile: z
    .string()
    .min(1)
    .refine((v) => !path.isAbsolute(v), 'must be a relative path'),
  stateFile: z
    .string()
    .min(1)
    .refine((v) => !path.isAbsolute(v), 'must be a relative path'),
})

const BindingsCopySchema = z.object({
  copyIndex: z.number().int(),
  providerId: DecimalBigIntString,
  serviceProvider: z.string().regex(/^0x[a-fA-F0-9]{40}$/, 'must be a 0x-prefixed 40-char hex address'),
  providerURL: z.string().nullable(),
  dataSetId: DecimalBigIntString,
})

/**
 * Bindings file written by extract-bindings, consumed by prebuild-batch-states.
 *
 * Invariants enforced here:
 * - exactly 2 copies
 * - copy indexes are [0, 1]
 * - providerIds are distinct
 */
export const BindingsFileSchema = z
  .object({
    version: z.literal(1),
    spaceDID: z.string().min(1),
    copies: z.array(BindingsCopySchema).length(2),
  })
  .superRefine((val, ctx) => {
    const sorted = [...val.copies].sort((a, b) => a.copyIndex - b.copyIndex)
    if (sorted[0].copyIndex !== 0 || sorted[1].copyIndex !== 1) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'copies must have copyIndex values [0, 1]',
        path: ['copies'],
      })
    }
    if (sorted[0].providerId === sorted[1].providerId) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'copies must have distinct providerId values',
        path: ['copies'],
      })
    }
  })

// ---------------------------------------------------------------------------
// Zod error formatting
// ---------------------------------------------------------------------------

/**
 * Format a ZodError into a single operator-friendly string.
 * Lists each failing field path and its message.
 *
 * @param {import('zod').ZodError} error
 * @param {string} filePath  - included in the prefix so operators know which file
 * @param {string} context   - short label like "bindings file" or "queue descriptor"
 */
export function formatZodError(error, filePath, context) {
  const issues = error.issues
    .map((issue) => {
      const fieldPath = issue.path.length > 0 ? issue.path.join('.') : '(root)'
      return `  ${fieldPath}: ${issue.message}`
    })
    .join('\n')
  return `invalid ${context}: ${filePath}\n${issues}`
}

// ---------------------------------------------------------------------------
// Queue descriptor helpers
// ---------------------------------------------------------------------------

/**
 * @typedef {object} QueueDescriptor
 * @property {1} version
 * @property {string} batchId
 * @property {import('@storacha/filecoin-pin-migration/types').SpaceDID} spaceDID
 * @property {string} selectedRootsFile
 * @property {string} stateFile
 */

/**
 * Read, parse, and validate a queue descriptor file.
 *
 * @param {string} descriptorPath
 * @param {string} operation  - caller name used in error messages (e.g. 'queueWorker')
 * @returns {Promise<QueueDescriptor>}
 */
export async function loadQueueDescriptor(descriptorPath, operation) {
  /** @type {unknown} */
  let parsed
  try {
    parsed = JSON.parse(await fs.promises.readFile(descriptorPath, 'utf8'))
  } catch (error) {
    throw new Error(
      `${operation}: failed to read queue descriptor ${descriptorPath}: ${
        error instanceof Error ? error.message : String(error)
      }`,
    )
  }

  const result = QueueDescriptorSchema.safeParse(parsed)
  if (!result.success) {
    throw new Error(formatZodError(result.error, descriptorPath, 'queue descriptor'))
  }

  return {
    version: 1,
    batchId: result.data.batchId,
    spaceDID: /** @type {import('@storacha/filecoin-pin-migration/types').SpaceDID} */ (result.data.spaceDID),
    selectedRootsFile: result.data.selectedRootsFile,
    stateFile: result.data.stateFile,
  }
}

/**
 * Resolve `relativePath` against `runDir` and assert it stays inside it.
 *
 * @param {string} runDir
 * @param {string} relativePath
 * @param {string} label      - field description for the error message
 * @param {string} operation  - caller name used in error messages
 */
export function resolveRunDirPath(runDir, relativePath, label, operation) {
  const resolved = path.resolve(runDir, relativePath)
  const relative = path.relative(runDir, resolved)

  if (relative === '' || relative.startsWith('..') || path.isAbsolute(relative)) {
    throw new Error(`${operation}: ${label} must stay within run directory: ${relativePath}`)
  }

  return resolved
}

/**
 * Rename `source` to `dest`, silently ignoring ENOENT on the source.
 *
 * @param {string} source
 * @param {string} dest
 */
export async function moveIfPresent(source, dest) {
  try {
    await fs.promises.rename(source, dest)
  } catch (error) {
    if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') {
      return
    }

    throw error
  }
}

// ---------------------------------------------------------------------------
// Terminal progress
// ---------------------------------------------------------------------------

/**
 * Render a single-line terminal progress update without adding a new line.
 * Subsequent calls replace the same line.
 *
 * Does nothing when stdout is not a TTY.
 *
 * @param {string} text
 */
export function renderProgressLine(text) {
  if (!process.stdout.isTTY) return

  process.stdout.clearLine(0)
  process.stdout.cursorTo(0)
  process.stdout.write(text)
}
