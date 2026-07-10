import process from 'node:process'

/**
 * Render a single-line terminal progress update without adding a new line.
 * Subsequent calls replace the same line.
 *
 * @param {string} text
 */
export function renderProgressLine(text) {
  if (!process.stdout.isTTY) return

  process.stdout.clearLine(0)
  process.stdout.cursorTo(0)
  process.stdout.write(text)
}
