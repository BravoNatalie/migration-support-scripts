import fs from 'node:fs'
import path from 'node:path'

/**
 * Write `content` to `destPath` atomically via a tmp-rename pattern.
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
