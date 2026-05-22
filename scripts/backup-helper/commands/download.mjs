/**
 * `backup-helper download` — thin Node wrapper around `run-backup-download.sh`.
 *
 * Spawns the shell launcher with `stdio: 'inherit'` so aria2's progress output
 * streams directly to the user's terminal. On failure, rejects with enough
 * detail for the CLI entrypoint to surface the error and choose the process
 * exit behavior. We don't wrap aria2's stderr — it's already informative on
 * its own.
 */

import { spawn } from 'node:child_process'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const LAUNCHER_PATH = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', 'run-backup-download.sh')

/**
 * @param {object} opts
 * @param {string} opts.manifest            Absolute path to the aria2 manifest.
 * @param {number} [opts.concurrency]       Max concurrent file downloads. Defaults to the launcher's own default (16).
 */
export async function runDownload({ manifest, concurrency }) {
  const args = [LAUNCHER_PATH, manifest]
  if (concurrency != null) args.push(String(concurrency))

  await new Promise((resolve, reject) => {
    const child = spawn('bash', args, { stdio: 'inherit' })

    child.on('error', reject)

    child.on('close', (code, signal) => {
      if (signal != null) {
        reject(new Error(`download: launcher terminated by signal ${signal}`))
        return
      }
      if (code !== 0) {
        reject(new Error(`download: launcher exited with code ${code ?? 1}`))
        return
      }
      resolve(undefined)
    })
  })
}
