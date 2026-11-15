# up-wire-protobuf

A separate payload wire crate using the same `up-rust-core` revision as the LoLa,
iceoryx2, and native C/C++ wire candidates. The crate does not define another
transport, runtime, metadata format, or core trait family.

```text
Application generated protobuf message
                 │
       ProtobufWire : Wire<T>
                 │
       core PayloadWriter / StartupSlot
                 │
       configured transport loan/storage
```

`protobuf = 3.7.2` is exact. This is the current rust-protobuf engine; migration
to Google's newer Protobuf runtime is separate, deferred work. The engine stays
in this external wire crate: the shared core gains no Protobuf dependency,
generated message types or code generator.

The core Git dependency pins
[`02c6340`](https://github.com/PLeVasseur/up-rust/commit/02c6340115f089dd13552f3a03ba30de1d98d8b4) on the
experimental `codex/allocator-mcu-spike` branch, the same revision used by both
uP-L1 transports, the native wire and both allocator providers. Changes to that
shared contract require compatibility checks across all six consumers.
This crate contains `#![forbid(unsafe_code)]`. It is experimental and has no
crates.io release enabled.

On macOS and Linux this SDK revision selects its host execution model
automatically. For `aarch64-apple-darwin`, `aarch64-unknown-linux-gnu` and
`x86_64-unknown-linux-gnu` it also builds a small deadline C shim with the native
`clang` and `ar` (cross-compiling the shim is not supported), and a Linux x86-64
build must target a CX16 minimum ISA, for example `-C target-feature=+cmpxchg16b`.
See the [SDK core README](https://github.com/PLeVasseur/up-rust/blob/02c6340115f089dd13552f3a03ba30de1d98d8b4/core/README.md).

## Codec behavior

- Encodes the ordinary Protobuf payload format, not `google.protobuf.Any` and not
  a whole uProtocol frame. Identity is the core's existing Protobuf wire/family ID.
- Computes size, validates the format/address-space limit and destination capacity,
  then uses `CodedOutputStream::bytes` over the core writer's initialized output.
  There is no intermediate whole-message `Vec`. `PayloadWriter::zeroed` initializes
  the destination before the serializer writes it; this is not an uninitialized
  buffer optimization or a claim that physical bytes are written only once.
- Refuses short output before modifying the writer. A later serialization failure
  can leave initialized partial output, which must be discarded like every failed
  `Wire::encode`. An inconsistent successful byte count is an error, not an assert.
- Parses a complete slice with stock `Message::parse_from_bytes`, then replaces
  the destination. Parse refusal does not assign a new value into the destination.
  Stock unknown-field, duplicate-field, and malformed-input semantics are retained.
- Initializes decode slots through the existing `StartupSlot::write(T::new())`.

Both encode and decode conservatively report `RuntimeHeap::Allocates`. A successful
slice encode for a particular generated scalar/string may avoid allocation, but
generic `Message` implementations and dependency error paths can allocate. Decode
owns strings/vectors/unknown fields; successful replacement frees the previous
value's owned fields, and parse failure drops any partial new message. Setup
`T::new()` and custom `Drop` behavior are schema/implementation dependent.

This crate does **not** claim panic/OOM freedom or ASIL qualification. The safe
`Message` trait is open to caller implementations, and stock generated runtime
code has infallible allocations. Native heap restrictions are unchanged.

## Integration status

The pinned core admits declared allocating codecs on serialized routes and owns
decoded-value initialization, replacement, cleanup and slot reuse. The retained
host controls cover Protobuf publisher/subscriber and RPC composition, capacity
preflight, reuse and refusal behavior, including destructor reentry and unwind.
These are host controls using test transport implementations, not middleware
or target acceptance. Native and mixed routes retain the native restrictions.

Generic messages have no schema-wide maximum, so route setup must provide its
ordinary explicit maximum payload. The wire declares exact byte sizes for
successfully encodable values. Values outside the Protobuf format/address-space
limit return unknown size and are refused before publication. An inconsistent
caller-provided `Message` implementation is not trusted for memory safety.

At this pin, locked library checks passed with stable 1.98.1, Rust 1.88.0 and
Rust 1.100.0, and the test targets compiled with Rust 1.88.0, on an aarch64 macOS
host. The compile-only commands are:

```sh
cargo +1.88.0 check --locked --tests
cargo +1.100.0 check --locked --tests
```

These commands do not execute the tests. Rust 1.88 remains the declared minimum;
the 1.100 check is an additional compatibility lane. The retained tests use the
runtime's generated well-known messages and golden peer bytes. This crate has no
schema/codegen workflow yet; applications provide compatible generated messages.
No protoc build, target/native execution or middleware rebuild is implied.

The crate is licensed under [Apache-2.0](LICENSE); see [NOTICE.md](NOTICE.md).
