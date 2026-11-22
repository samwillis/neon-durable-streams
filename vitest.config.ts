import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    projects: [
      { test: { name: 'unit', include: ['test/unit/**/*.test.ts'] } },
      { test: { name: 'integration', include: ['test/integration/**/*.test.ts'],
        fileParallelism: false, testTimeout: 60_000, hookTimeout: 60_000 } },
      { test: { name: 'conformance', include: ['test/conformance/**/*.test.ts'],
        // Upstream property tests perform hundreds of sequential transactions.
        // Keep every sample/assertion across CI runner regions. Full Neon qualification
        // is intentionally sequential; network round trips dominate these properties.
        fileParallelism: false, testTimeout: 600_000, hookTimeout: 60_000 } },
      { test: { name: 'neon', include: ['test/neon/**/*.test.ts'],
        fileParallelism: false, testTimeout: 60_000, hookTimeout: 60_000 } },
      { test: { name: 'function', include: ['test/function/**/*.test.ts'],
        fileParallelism: false, testTimeout: 60_000, hookTimeout: 60_000 } },
      { test: { name: 'recovery', include: ['test/recovery/**/*.test.ts'],
        fileParallelism: false, testTimeout: 180_000, hookTimeout: 180_000 } },
    ],
  },
})
