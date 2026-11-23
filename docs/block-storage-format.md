# Verifiable block object format

The development object format is version 2. It uses one immutable object per sealed chunk, a packed block directory and independently verified blocks. Upload verification checks the whole object before publication stores the directory and deletes staging rows atomically. Fresh development schemas include the directory in their sealed manifests.

All fixed-width integers use unsigned big endian encoding. Offsets use ASCII and the library's offset grammar. Frame numbers in the directory are zero based within the object.

| Header field | Bytes |
| --- | --- |
| Magic `NDSBLK02` | 8 |
| Installation, stream and chunk UUIDs | 16 each |
| Frame count, block count and packed directory length | 4 each |

The 68-byte header is followed by the packed directory and then contiguous encoded blocks. The first block starts immediately after the directory. The final block ends at the exact object length. Counts, byte ranges and logical ranges must cover the complete object in order.

Each directory entry has five 4-byte integers, a 32-byte SHA-256 digest, two 2-byte offset lengths and the two offset strings. The integers give the first frame, frame count, total payload bytes, absolute object byte start and encoded byte length. The strings give the logical start and end. The fixed part is 56 bytes. Each offset is at most 255 bytes. The SQL manifest stores these same packed directory bytes, bound to the uploaded object's identity, full checksum and exact size.

Each block contains consecutive complete frames. A frame encodes its payload length and JSON message count as unsigned base-128 variable-length integers, followed by any message-end deltas and the payload. The high bit marks continuation. Integers use their shortest encoding and fit unsigned 32 bits. A zero message count identifies binary content. JSON frames have at least one message, and each message-end delta advances through the stored comma-terminated JSON payload. The decoder validates UTF-8 JSON and exact message coverage before returning views.

The block directory supplies the offset at the beginning of a block. The decoder derives subsequent offsets from the selected offset scheme and frame lengths. The current byte-offset control advances by payload length plus five. The selected public offset design will determine that rule for the final baseline. Logical frame boundaries and JSON message boundaries remain distinct.

Block targets are soft limits checked after adding a complete frame. Two half-target payloads can occupy one block even when their framing takes the encoded size slightly over target. A large valid append can occupy a block larger than the target. The object has a hard 128 MiB encoded limit and the codec permits at most 131,072 frames. Storage policy has additional payload, frame, work and age bounds.

For a partial read, the Function validates the trusted directory and requests the selected contiguous physical interval. The S3 adapter requires an exact 206 response and `Content-Range`, including the total object size. Each selected block must match its directory digest, lengths and logical range before serving. Complete-object reads additionally verify the whole-object checksum and embedded identities. Cached blocks own their allocations so a small retained block cannot keep an evicted larger download alive outside cache accounting.

The codec, physical range operation and normal replay integration are under qualification. [Implementation plan](replay-performance-plan.md) · [Independent one-frame fixture](../test/fixtures/chunks/v2.hex) · [Codec properties and corruption cases](../test/unit/block-codec.test.ts)
