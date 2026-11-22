import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    projects: [
      { test: { name: 'unit', include: ['test/unit/**/*.test.ts'] } },
      { test: { name: 'integration', include: ['test/integration/**/*.test.ts'],
        fileParallelism: false, testTimeout: 60_000, hookTimeout: 60_000 } },
      { test: { name: 'conformance', include: ['test/conformance/**/*.test.ts'],
        fileParallelism: false, testTimeout: 30_000, hookTimeout: 30_000 } },
      { test: { name: 'neon', include: ['test/neon/**/*.test.ts'],
        fileParallelism: false, testTimeout: 60_000, hookTimeout: 60_000 } },
    ],
  },
})
