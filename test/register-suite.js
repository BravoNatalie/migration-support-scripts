import { describe, test } from 'vitest'

/**
 * Register a flat object of test cases as Vitest tests.
 *
 * @param {string} suiteName
 * @param {Record<string, () => unknown | Promise<unknown>>} suite
 */
export function registerSuite(suiteName, suite) {
  describe(suiteName, () => {
    for (const [testName, testFn] of Object.entries(suite)) {
      test(testName, testFn)
    }
  })
}
