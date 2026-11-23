/** Emits one verified data event followed by its durable control checkpoint. */
export function* encodeSsePage(bytes: Buffer, hasData: boolean, binary: boolean, control: Readonly<Record<string, unknown>>): Generator<Uint8Array> {
  if (hasData) {
    yield Buffer.from('event: data\ndata:')
    if (binary) {
      // Every intermediate base64 fragment ends at a complete three-byte unit.
      for (let position = 0; position < bytes.length; position += 49152) yield Buffer.from(bytes.subarray(position, position + 49152).toString('base64'))
    } else {
      const decoder = new TextDecoder('utf-8', { ignoreBOM: true })
      let previousCR = false
      const fragment = (text: string) => {
        if (!text.length) return ''
        if (previousCR && text.startsWith('\n')) text = text.slice(1)
        previousCR = text.endsWith('\r')
        return text.replace(/\r\n|\r/g, '\n').replace(/\n/g, '\ndata:')
      }
      for (let position = 0; position < bytes.length; position += 8192) {
        const text = fragment(decoder.decode(bytes.subarray(position, position + 8192), { stream: true }))
        if (text) yield Buffer.from(text)
      }
      const tail = fragment(decoder.decode())
      if (tail) yield Buffer.from(tail)
    }
    yield Buffer.from('\n\n')
  }
  yield Buffer.from(`event: control\ndata:${JSON.stringify(control)}\n\n`)
}
