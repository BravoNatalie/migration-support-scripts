/**
 * `backup-helper download` — DB-driven aria2 RPC scheduler.
 *
 * Uses tracking.db as the source of truth for shard status, retries, and
 * resumability across reruns. aria2 remains responsible for transfer-level
 * resume via its session and per-file control files.
 */

import fs from 'node:fs/promises'
import net from 'node:net'
import path from 'node:path'
import { execa } from 'execa'

import { Aria2RPC } from '../lib/aria2-rpc.mjs'
import { shardCarPath, shardsDir } from '../lib/layout.mjs'
import { openTrackingDb } from '../lib/tracking-db.mjs'

export const MAX_DOWNLOAD_ATTEMPTS = 3
const TARGET_RPC_QUEUE_SIZE = 1000
const RPC_SUBMISSION_BATCH_SIZE = 200
const POLL_INTERVAL_MS = 1000
const RPC_BOOT_TIMEOUT_MS = 15_000
const POST_STARTUP_GRACE_MS = 1_000
const DEFAULT_ARIA_CONCURRENT_FILES = 30

/**
 * @typedef {[string, ...unknown[]]} Aria2RPCCall
 */

/**
 * @typedef {object} Aria2NotificationEvent
 * @property {string} gid
 */

/**
 * @typedef {object} Aria2FileUri
 * @property {string} [status]
 * @property {string} [uri]
 */

/**
 * @typedef {object} Aria2FileEntry
 * @property {string} [path]
 * @property {Aria2FileUri[]} [uris]
 */

/**
 * @typedef {object} Aria2Status
 * @property {string} [gid]
 * @property {string} [errorCode]
 * @property {string} [errorMessage]
 * @property {Aria2FileEntry[]} [files]
 */

/**
 * @typedef {object} DownloadQueueItem
 * @property {string} shardCid
 * @property {string} sourceUrl
 * @property {string | null} effectiveUrl
 * @property {number} sizeBytes
 * @property {string} url
 */

/**
 * @typedef {object} ShardContext
 * @property {string} sourceUrl
 * @property {string | null} effectiveUrl
 */

/**
 * @param {number} ms
 */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * Probe the aria2 HTTP JSON-RPC endpoint with a real request timeout.
 *
 * @param {number} port
 * @param {number} timeoutMs
 */
async function probeAriaHttpRPC(port, timeoutMs) {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)

  try {
    const response = await fetch(`http://127.0.0.1:${port}/jsonrpc`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 'startup-probe',
        method: 'aria2.getGlobalStat',
        params: [],
      }),
      signal: controller.signal,
    })

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`)
    }

    const payload = await response.json()
    if (payload.error) {
      throw new Error(payload.error.message || 'aria2 RPC error')
    }

    return payload.result
  } finally {
    clearTimeout(timer)
  }
}

/**
 * Return per-download aria2 transfer options for the given shard size.
 * Most shard CARs stay single-connection; larger CARs can use more splits.
 *
 * ARCHITECTURAL DECISIONS FOR DOWNLOAD OPTIMIZATION
 *
 * Context:
 * Storacha splits uploaded content into maximum 134 MB shards,
 * which are hosted in a Cloudflare R2 bucket.
 *
 * Optimization Strategy:
 * 1. Disable Intra-file Multi-threading (--split=1, --max-connection-per-server=1)
 *    Standard tools optimize speed by slicing a single file via HTTP Range headers.
 *    Because these shards are small-to-medium sized, we force a 1:1 ratio.
 *
 * 2. Horizontal Bandwidth Saturation (--max-concurrent-downloads)
 *    Instead of downloading one file with multiple threads, maximize bandwidth
 *    by downloading multiple completely distinct shards simultaneously.
 *
 * @param {number} sizeBytes
 */
function transferOptionsForSize(sizeBytes) {
  const SMALL_CAR_BYTES = 96 * 1024 * 1024
  const MEDIUM_CAR_BYTES = 256 * 1024 * 1024
  const LARGE_CAR_BYTES = 512 * 1024 * 1024
  const XLARGE_CAR_BYTES = 1024 * 1024 * 1024

  if (sizeBytes > XLARGE_CAR_BYTES) {
    return {
      continue: 'true',
      split: '8',
      'max-connection-per-server': '8',
    }
  }

  if (sizeBytes > LARGE_CAR_BYTES) {
    return {
      continue: 'true',
      split: '4',
      'max-connection-per-server': '4',
    }
  }

  if (sizeBytes > MEDIUM_CAR_BYTES) {
    return {
      continue: 'true',
      split: '3',
      'max-connection-per-server': '3',
    }
  }

  if (sizeBytes > SMALL_CAR_BYTES) {
    return {
      continue: 'true',
      split: '2',
      'max-connection-per-server': '2',
    }
  }

  return {
    continue: 'true',
    split: '1',
    'max-connection-per-server': '1',
  }
}

/**
 * Ask the OS for a free localhost TCP port for a private aria2 RPC daemon.
 *
 * @returns {Promise<number>}
 */
function getFreePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer()
    server.once('error', reject)
    server.listen(0, () => {
      const address = server.address()
      if (!address || typeof address === 'string') {
        server.close(() => reject(new Error('download: failed to allocate RPC port')))
        return
      }
      const { port } = address
      server.close((err) => (err ? reject(err) : resolve(port)))
    })
  })
}

/**
 * @param {object} opts
 * @param {string} opts.dir
 * @param {number} opts.rpcPort
 * @param {number | undefined} opts.concurrency
 */
function aria2Args({ dir, rpcPort, concurrency }) {
  const maxConcurrentFiles = concurrency ?? DEFAULT_ARIA_CONCURRENT_FILES
  const sessionFile = path.join(dir, 'aria2.session')

  return [
    '--enable-rpc=true',
    '--rpc-listen-all=false',
    `--rpc-listen-port=${rpcPort}`,
    `--dir=${shardsDir(dir)}`,
    '--allow-overwrite=false',
    '--auto-file-renaming=false',
    '--follow-metalink=false',
    '--no-want-digest-header=true',
    `--save-session=${sessionFile}`,
    '--save-session-interval=60',
    `--max-concurrent-downloads=${maxConcurrentFiles}`,
    '--enable-http-pipelining=true',
    `--stop-with-process=${process.pid}`,
    `--console-log-level=warn`,
  ]
}

/**
 * Return whether a local shard download is complete enough to skip requeueing.
 * The shard file must exist, its `.aria2` control file must be absent, and its size must not be smaller than expected.
 *
 * @param {string} outputPath
 * @param {number} expectedSize
 * @returns {Promise<boolean>}
 */
async function isCompletedDownload(outputPath, expectedSize) {
  let stat
  try {
    stat = await fs.stat(outputPath)
  } catch {
    return false
  }

  try {
    await fs.access(`${outputPath}.aria2`)
    return false
  } catch {}

  return stat.size >= expectedSize
}

/**
 * @param {string} sourceUrl
 */
function shouldRewriteSignedRedirect(sourceUrl) {
  return sourceUrl.includes('roundabout.web3.storage/piece/')
}

/**
 * Resolve the current redirect target once and normalize the signed R2 target
 * into its public `carpark-prod-*.r2.w3s.link` form.
 *
 * @param {string} sourceUrl
 * @returns {Promise<string | null>}
 */
async function resolveUnsignedRedirectTarget(sourceUrl) {
  if (!shouldRewriteSignedRedirect(sourceUrl)) return null

  const response = await fetch(sourceUrl, { method: 'HEAD', redirect: 'manual' })
  if (response.status < 300 || response.status >= 400) {
    return null
  }

  const location = response.headers.get('location')
  if (!location) return null

  const resolved = new URL(location)
  const hostMatch = resolved.hostname.match(/^(carpark-prod-\d+)\..+$/)
  if (!hostMatch) return null

  resolved.hostname = `${hostMatch[1]}.r2.w3s.link`
  resolved.search = ''
  return resolved.toString()
}

/**
 * @param {Aria2NotificationEvent[] | Aria2NotificationEvent | unknown} params
 */
function extractGidFromNotification(params) {
  const first = Array.isArray(params) ? params[0] : params
  if (first && typeof first === 'object' && 'gid' in first) {
    return typeof first.gid === 'string' ? first.gid : String(first.gid)
  }
  return null
}

/**
 * Normalize one `aria2.multicall()` result entry and extract the `addUri` GID.
 * The helper accepts the wrapped result shapes returned by `system.multicall`.
 *
 * @param {unknown} result
 */
function extractGidFromMulticallResult(result) {
  if (typeof result === 'string') return result
  if (Array.isArray(result)) {
    if (typeof result[0] === 'string') return result[0]
    if (Array.isArray(result[0]) && typeof result[0][0] === 'string') return result[0][0]
  }
  return null
}

/**
 * @param {unknown} value
 */
function getString(value) {
  return typeof value === 'string' ? value : value == null ? null : String(value)
}

/**
 * Recover the shard CID from aria2 file metadata when the GID-to-shard mapping is unavailable.
 * Downloads are written as `<shardCid>.car`, so the output basename maps back to the shard row.
 *
 * @param {Aria2Status} status
 */
function shardCidFromStatus(status) {
  const outputPath = getString(status?.files?.[0]?.path)
  if (!outputPath) return null
  if (!outputPath.endsWith('.car')) return null
  return path.basename(outputPath, '.car')
}

/**
 * Read aria2's status payload to determine which URL this download actually used.
 * Prefer the URI marked as `used`, and fall back to the first available URI for the first file.
 *
 * @param {Aria2Status} status
 */
function uriFromStatus(status) {
  const file = status?.files?.[0]
  if (!file || !Array.isArray(file.uris)) return null
  for (const uri of file.uris) {
    if (uri && typeof uri === 'object' && uri.status === 'used' && typeof uri.uri === 'string') {
      return uri.uri
    }
  }
  for (const uri of file.uris) {
    if (uri && typeof uri === 'object' && typeof uri.uri === 'string') {
      return uri.uri
    }
  }
  return null
}

/**
 * @param {Aria2Status} status
 */
function classifyFailure(status) {
  const errorMessage = getString(status?.errorMessage) || 'unknown aria2 error'
  const httpStatusMatch = errorMessage.match(/\bstatus=(\d{3})\b/)
  const statusCode = httpStatusMatch ? Number(httpStatusMatch[1]) : null
  const lower = errorMessage.toLowerCase()
  const isPermissionFailure =
    statusCode === 401 ||
    statusCode === 403 ||
    lower.includes('forbidden') ||
    lower.includes('access denied') ||
    lower.includes('permission denied')

  if (statusCode === 429 || isPermissionFailure || (statusCode != null && statusCode >= 500)) {
    return { retryable: true, statusCode, error: errorMessage }
  }

  const networkHints = [
    'timed out',
    'timeout',
    'temporar',
    'connection reset',
    'connection refused',
    'connection aborted',
    'network is unreachable',
    'name resolution',
    'could not resolve',
    'tls',
    'ssl',
    'eof',
    'broken pipe',
  ]

  if (networkHints.some((hint) => lower.includes(hint))) {
    return { retryable: true, statusCode, error: errorMessage }
  }

  return { retryable: false, statusCode, error: errorMessage }
}

/**
 * @param {number} port
 * @param {Aria2RPC} aria2
 * @param {number} timeoutMs
 */
async function waitForRPC(port, aria2, timeoutMs) {
  const deadline = Date.now() + timeoutMs
  let lastError
  while (Date.now() < deadline) {
    try {
      await probeAriaHttpRPC(port, 1_000)
      await aria2.open()
      return
    } catch (err) {
      lastError = err
      try {
        await aria2.close()
      } catch {}
      await sleep(250)
    }
  }
  // If we ever need to support environments where WebSocket is unavailable,
  // this can grow an HTTP polling fallback instead of requiring aria2.open().
  throw new Error(`download: aria2 RPC did not become ready: ${lastError?.message || lastError || 'timeout'}`)
}

/**
 * @param {import('execa').Subprocess} child
 * @param {Aria2RPC} aria2
 * @param {Promise<{code: number | null, signal: NodeJS.Signals | null}>} exitPromise
 * @param {boolean} interrupted
 */
async function shutdownAria(child, aria2, exitPromise, interrupted) {
  try {
    await aria2.shutdown()
  } catch {}

  let finalState = await Promise.race([exitPromise, sleep(3_000).then(() => null)])
  if (finalState == null) {
    if (interrupted) {
      try {
        await aria2.forceShutdown()
      } catch {}
      finalState = await Promise.race([exitPromise, sleep(1_000).then(() => null)])
    }
  }

  try {
    await aria2.close()
  } catch {}

  if (finalState == null && !child.killed) {
    child.kill()
  }
}

/**
 * @param {object} opts
 * @param {string} opts.dir
 * @param {number | undefined} opts.concurrency
 * @param {number | undefined} opts.port
 */
export async function runDownload({ dir, concurrency, port }) {
  await fs.mkdir(shardsDir(dir), { recursive: true })

  const tracking = openTrackingDb(dir)
  const reset = tracking.resetStaleDownloads()
  if (reset > 0) {
    console.error(`download: reset ${reset.toLocaleString()} stale queued/active shards to pending`)
  }

  const rpcPort = port ?? (await getFreePort())
  console.error(`download: starting aria2 RPC on localhost:${rpcPort}`)

  const child = execa('aria2c', aria2Args({ dir, rpcPort, concurrency }), {
    stdio: 'inherit',
  })
  const exitPromise = child.then(
    (result) => ({ code: result.exitCode, signal: result.signal ?? null }),
    (error) => ({ code: error.exitCode ?? null, signal: error.signal ?? null }),
  )

  const aria2 = new Aria2RPC({ port: rpcPort })
  /** @type {Map<string, string>} */
  const gidToShard = new Map()
  /** @type {Map<string, ShardContext>} */
  const shardContext = new Map()
  /** @type {Set<Promise<void>>} */
  const notificationTasks = new Set()
  let interrupted = false
  let signalMessageShown = false

  /**
   * @param {NodeJS.Signals} signal
   */
  const onInterruptSignal = (signal) => {
    interrupted = true
    if (!signalMessageShown) {
      signalMessageShown = true
      console.error(`\ndownload: ${signal} received, shutting down aria2...`)
    }
  }

  process.on('SIGINT', onInterruptSignal)
  process.on('SIGTERM', onInterruptSignal)
  try {
    const startupExit = await Promise.race([
      waitForRPC(rpcPort, aria2, RPC_BOOT_TIMEOUT_MS).then(() => null),
      exitPromise,
    ])
    if (startupExit) {
      const reason =
        startupExit.signal != null
          ? `terminated by signal ${startupExit.signal}`
          : `exited with code ${startupExit.code ?? 1}`
      throw new Error(`download: aria2 ${reason}`)
    }
    await sleep(POST_STARTUP_GRACE_MS)

    /**
     * Track async notification work so the scheduler sees persisted DB state
     * before it decides whether the run is drained.
     *
     * @param {Promise<void>} task
     */
    const trackNotificationTask = (task) => {
      notificationTasks.add(task)
      void task.finally(() => {
        notificationTasks.delete(task)
      })
    }

    const waitForNotificationTasks = async () => {
      while (notificationTasks.size > 0) {
        await Promise.allSettled([...notificationTasks])
      }
    }

    /**
     * @param {string} shardCid
     * @param {string} sourceUrl
     * @param {string | null} effectiveUrl
     */
    const rememberShard = (shardCid, sourceUrl, effectiveUrl) => {
      shardContext.set(shardCid, { sourceUrl, effectiveUrl })
    }

    /**
     * @param {string} gid
     * @returns {Promise<void>}
     */
    const completeFromGid = async (gid) => {
      /** @type {Aria2Status} */
      const status = await aria2.tellStatus(gid, ['gid', 'files'])
      const shardCid = gidToShard.get(gid) || shardCidFromStatus(status)
      if (!shardCid) return
      gidToShard.delete(gid)
      shardContext.delete(shardCid)
      tracking.markComplete(shardCid)
    }

    /**
     * @param {string} gid
     * @returns {Promise<void>}
     */
    const failFromGid = async (gid) => {
      /** @type {Aria2Status} */
      const status = await aria2.tellStatus(gid, ['gid', 'errorCode', 'errorMessage', 'files'])
      const shardCid = gidToShard.get(gid) || shardCidFromStatus(status)
      if (!shardCid) return
      gidToShard.delete(gid)
      const context = shardContext.get(shardCid)
      shardContext.delete(shardCid)
      const usedUri = uriFromStatus(status)
      const { retryable, statusCode, error } = classifyFailure(status)
      tracking.markFailure({
        shardCid,
        url: usedUri || context?.effectiveUrl || context?.sourceUrl || null,
        statusCode,
        error,
        retryable,
      })
    }

    aria2.addEventListener('onDownloadComplete', (event) => {
      const gid = extractGidFromNotification(event.params)
      if (gid) {
        trackNotificationTask(completeFromGid(gid))
      }
    })

    aria2.addEventListener('onDownloadError', (event) => {
      const gid = extractGidFromNotification(event.params)
      if (gid) {
        trackNotificationTask(failFromGid(gid))
      }
    })

    let submitting = false

    const reconcileUnsignedFallbacks = async () => {
      const candidates = tracking.listUnsignedFallbackCandidates(RPC_SUBMISSION_BATCH_SIZE, MAX_DOWNLOAD_ATTEMPTS)
      for (const candidate of candidates) {
        if (!shouldRewriteSignedRedirect(candidate.sourceUrl)) {
          continue
        }
        try {
          const rewritten = await resolveUnsignedRedirectTarget(candidate.sourceUrl)
          if (rewritten && rewritten !== candidate.sourceUrl) {
            tracking.setEffectiveUrl(candidate.shardCid, rewritten)
            tracking.requeuePending(candidate.shardCid)
          }
        } catch (err) {
          console.error(`download: failed to resolve redirect for ${candidate.shardCid}: ${err.message || err}`)
        }
      }
    }

    const fillQueue = async () => {
      if (submitting) return
      submitting = true
      try {
        while (gidToShard.size < TARGET_RPC_QUEUE_SIZE) {
          const remainingCapacity = TARGET_RPC_QUEUE_SIZE - gidToShard.size
          const limit = Math.min(RPC_SUBMISSION_BATCH_SIZE, remainingCapacity)
          const candidates = tracking.claimDownloadBatch(limit)
          if (candidates.length === 0) {
            break
          }

          /** @type {Aria2RPCCall[]} */
          const submissions = []
          /** @type {DownloadQueueItem[]} */
          const queued = []

          for (const candidate of candidates) {
            const outputPath = shardCarPath(dir, candidate.shardCid)
            if (await isCompletedDownload(outputPath, candidate.sizeBytes)) {
              tracking.markComplete(candidate.shardCid)
              continue
            }

            const url = candidate.effectiveUrl || candidate.sourceUrl
            const transferOptions = transferOptionsForSize(candidate.sizeBytes)
            submissions.push([
              'addUri',
              [url],
              {
                dir: shardsDir(dir),
                out: `${candidate.shardCid}.car`,
                ...transferOptions,
              },
            ])
            queued.push({ ...candidate, url })
            rememberShard(candidate.shardCid, candidate.sourceUrl, candidate.effectiveUrl)
          }

          if (submissions.length === 0) {
            continue
          }

          /** @type {unknown[]} */
          let results
          try {
            results = await aria2.multicall(submissions)
          } catch (err) {
            for (const candidate of queued) {
              shardContext.delete(candidate.shardCid)
              tracking.requeuePending(candidate.shardCid)
            }
            throw err
          }

          for (const [index, result] of results.entries()) {
            const candidate = queued[index]
            const gid = extractGidFromMulticallResult(result)
            if (!gid) {
              shardContext.delete(candidate.shardCid)
              tracking.markFailure({
                shardCid: candidate.shardCid,
                url: candidate.url !== candidate.sourceUrl ? candidate.url : null,
                statusCode: null,
                error: `download: failed to enqueue shard ${candidate.shardCid} (rpc-addUri)`,
                retryable: true,
              })
              continue
            }
            gidToShard.set(gid, candidate.shardCid)
            tracking.markActive(candidate.shardCid, gid)
          }
        }
      } finally {
        submitting = false
      }
    }

    while (true) {
      if (interrupted) {
        break
      }

      // has the aria2 worker process already exited unexpectedly?
      const workerState = await Promise.race([exitPromise, sleep(0).then(() => null)])
      if (workerState && workerState.code !== 0) {
        throw new Error(`download: aria2 exited with code ${workerState.code ?? 1}`)
      }
      if (workerState && workerState.signal != null) {
        throw new Error(`download: aria2 terminated by signal ${workerState.signal}`)
      }

      await reconcileUnsignedFallbacks()
      if (interrupted) {
        break
      }
      await fillQueue()
      await waitForNotificationTasks()
      if (interrupted) {
        break
      }

      const stats = tracking.getDownloadStats()
      console.error(
        `\ndownload: complete=${stats.complete.toString()} pending=${stats.pending.toString()} queued=${stats.queued.toString()} active=${stats.active.toString()} error=${stats.error.toString()}`,
      )

      if (stats.pending === 0 && stats.queued === 0 && stats.active === 0) {
        break
      }

      if (interrupted) {
        break
      }
      await sleep(POLL_INTERVAL_MS)
    }
  } finally {
    try {
      process.off('SIGINT', onInterruptSignal)
      process.off('SIGTERM', onInterruptSignal)
      await shutdownAria(child, aria2, exitPromise, interrupted)
    } finally {
      tracking.close()
    }
  }
}
