/** A protocol failure whose public message must not contain infrastructure secrets. */
export class StreamError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly headers: Readonly<Record<string, string>> = {},
  ) {
    super(message)
    this.name = 'StreamError'
  }
}

export class StorageCompatibilityError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'StorageCompatibilityError'
  }
}
