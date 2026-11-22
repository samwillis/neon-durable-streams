// Opt-in qualification fixture, never imported by shipped code. It delays SQL
// sends to reproduce a distant coordinator while retaining real Neon services.
import pg from 'pg'
const delay = Number(process.env.DS_TEST_SQL_DELAY_MS)
if (!Number.isSafeInteger(delay) || delay < 1 || delay > 1000) throw new RangeError('Set DS_TEST_SQL_DELAY_MS between 1 and 1000')
const original = pg.Client.prototype.query
pg.Client.prototype.query = function (...args) {
  const callback = typeof args.at(-1) === 'function' ? args.at(-1) : undefined
  if (callback) {
    setTimeout(() => original.apply(this, args), delay)
    return
  }
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      try { Promise.resolve(original.apply(this, args)).then(resolve, reject) }
      catch (error) { reject(error) }
    }, delay)
  })
}
