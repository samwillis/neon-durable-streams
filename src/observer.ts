/** Observers cannot change committed outcomes or create unhandled rejections. */
export function observe<T>(callback: ((value: T) => unknown) | undefined, value: T,
  onError?: (error: unknown) => void): void {
  try { void Promise.resolve(callback?.(value)).catch((error: unknown) => { observe(onError, error) }) }
  catch (error) { observe(onError, error) }
}
