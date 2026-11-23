/** Availability is independent of append closure and retained fork ancestry. */
export function streamPresence(stream: { readonly deleted_at: Date | null; readonly expired: boolean }): 'available' | 'deleted' | 'expired' {
  return stream.deleted_at !== null ? 'deleted' : stream.expired ? 'expired' : 'available'
}
export function unavailableGenerationStatus(references: number): 404 | 410 { return references > 0 ? 410 : 404 }
