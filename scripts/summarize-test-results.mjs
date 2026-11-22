import { readFile, writeFile } from 'node:fs/promises'

const [input, output] = process.argv.slice(2)
if (!input || !output) throw new Error('Usage: summarize-test-results.mjs input.json output.json')
const report = JSON.parse(await readFile(input, 'utf8'))
const secrets = Object.entries(process.env).filter(([key, value]) => /SECRET|TOKEN|KEY|PASSWORD|DATABASE_URL/.test(key) && value?.length >= 8).map(([, value]) => value)
if (process.env.DURABLE_STREAMS_KEYRING) secrets.push(...Object.values(JSON.parse(process.env.DURABLE_STREAMS_KEYRING).keys))
const redact = (message) => {
  let text = String(message)
  for (const secret of secrets) text = text.replaceAll(secret, '[redacted]')
  return text.replace(/postgres(?:ql)?:\/\/[^\s'"<>]+/g, '[database URL]')
    .replace(/dsw\.2\.[A-Za-z0-9_-]{1,128}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}|dsw\.[0-9a-f-]{36}\.[A-Za-z0-9_-]{43}/gi, '[wake token]').slice(0, 8000)
}
await writeFile(output, JSON.stringify({ total: report.numTotalTests, passed: report.numPassedTests,
  failed: report.numFailedTests, pending: report.numPendingTests,
  suites: report.testResults.map((suite) => ({ file: suite.name.split(/[\\/]/).at(-1), status: suite.status,
    message: redact(suite.message ?? ''), tests: suite.assertionResults.map((test) => ({ name: test.fullName, status: test.status,
      durationMs: test.duration, failures: test.failureMessages.map(redact) })) })),
}, null, 2) + '\n')
