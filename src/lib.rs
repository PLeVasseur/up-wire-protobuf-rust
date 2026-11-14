// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0
//! Protocol Buffers application payloads using the shared core wire contracts.
//!
//! This crate encodes payload bytes only. The core and transport own uProtocol
//! frame metadata, routes, and loan/sample storage. No complete-message staging
//! vector is used during encoding.
//!
//! Both directions conservatively declare [`RuntimeHeap::Allocates`]: generated
//! decode owns fields and unknown fields, including initializer/reinitialization,
//! replacement and final owned-value destruction; encode error paths and
//! caller-supplied `Message` implementations can allocate/free. This is not a no-panic/OOM guarantee
//! or a qualified native/safety runtime. Use the core's separate native wire for
//! agreed C/C++ layout borrowing.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use protobuf::{CodedOutputStream, Message};
use up_rust_core::uattributes::PayloadEncoding;
use up_rust_core::wire::{
    EncodedSize, PayloadWriter, Ready, RuntimeHeap, StartupSlot, Wire, WireError, WireInfo,
    WirePayloadIdentity, PROTOBUF_PAYLOAD_FAMILY_ID, PROTOBUF_WIRE_ID,
};

// rust-protobuf 3.7.2 wire_format::MAX_MESSAGE_SIZE follows Protobuf's signed
// 32-bit message-size limit. This is a format bound, not a caller/concurrency cap.
const MAX_MESSAGE_SIZE: u64 = i32::MAX as u64;

/// A stateless wire for `protobuf` 3.7.2 generated messages, including lite types.
///
/// Applications use normal core roles with `.wire(ProtobufWire)`. Generic message
/// size is unbounded at schema level, so route setup still needs an explicit
/// `max_payload` bound. The current core must admit allocating serialized codecs
/// and correctly retire owned decode values before such a route can run.
///
/// `protobuf::Message` is a safe, open trait. Its implementations can panic,
/// allocate, or return inconsistent sizes. The adapter does not use their
/// semantic correctness as a Rust memory-safety premise. Production resource and
/// panic behavior requires qualification of the concrete generated schema and
/// runtime, including `new`, parse, encode, replacement/Drop, and OOM handling.
/// The generic implementation is outside strict/native no-panic acceptance;
/// admitting a strict deployment requires the selected generated-message/runtime
/// profile's source and resource evidence. Merely implementing this trait or
/// declaring `Allocates` is not that qualification.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProtobufWire;

impl<T: Message> Wire<T> for ProtobufWire {
    fn describe(&self) -> WireInfo {
        WireInfo::new(
            "protobuf",
            PROTOBUF_WIRE_ID,
            PROTOBUF_PAYLOAD_FAMILY_ID,
            WirePayloadIdentity::serialized(PayloadEncoding::PROTOBUF),
            RuntimeHeap::Allocates("protobuf encode implementation or error path"),
            RuntimeHeap::Allocates(
                "protobuf initialization, decode, replacement and owned cleanup",
            ),
        )
        // Every successfully encodable value has its exact byte count. Values
        // outside the format/address-space limit are refused before publication.
        .exact()
    }

    fn encoded_size(&self, value: &T) -> EncodedSize {
        match checked_size(value.compute_size()) {
            Ok(size) => EncodedSize::Exact(size),
            Err(_) => EncodedSize::Unknown,
        }
    }

    /// # Safety-usable invariant
    /// Uses only the core writer's checked initialized byte slice. The writer is
    /// never replaced and its earlier initialized bytes remain initialized on
    /// success, error, or unwind. A refused encode may leave a partial initialized
    /// payload; the SDK must discard it as with any other failed wire encode.
    fn encode(&self, value: &T, out: &mut PayloadWriter<'_>) -> Result<(), WireError> {
        if !value.is_initialized() {
            return Err(WireError::Unencodable("protobuf required field missing"));
        }
        let size = checked_size(value.compute_size())?;
        if size > out.remaining() {
            return Err(WireError::BufferTooSmall {
                needed: size,
                available: out.remaining(),
            });
        }
        // `zeroed` returns ordinary initialized bytes in the actual SDK output.
        // The selected dependency's safe slice writer bounds every output write;
        // inaccurate caller-provided Message sizes cannot escape that slice.
        let bytes = out.zeroed(size)?;
        let mut stream = CodedOutputStream::bytes(bytes);
        value
            .write_to(&mut stream)
            .map_err(|_| WireError::Unencodable("protobuf serialization failed"))?;
        stream
            .flush()
            .map_err(|_| WireError::Unencodable("protobuf output flush failed"))?;
        // Do not call check_eof(): it asserts on inconsistent custom Message
        // implementations. Treat a successful short write as a normal refusal.
        if stream.total_bytes_written() != size as u64 {
            return Err(WireError::Unencodable(
                "protobuf size changed while encoding",
            ));
        }
        Ok(())
    }

    /// # Safety-usable invariant
    /// Consumes and initializes the exact startup slot with a valid T and returns
    /// only that slot's Ready. A panic from caller-provided T::new cannot produce
    /// Ready for uninitialized storage; the slot has not been written in that case.
    fn init_slot<'s>(&self, slot: StartupSlot<'s, T>) -> Ready<'s, T> {
        slot.write(T::new())
    }

    /// # Safety-usable invariant
    /// The destination is unchanged on parse refusal. Success assigns a fully
    /// constructed T after parsing the whole slice, dropping the prior owned T.
    /// If the old value's Drop unwinds during assignment, Rust's drop-and-replace
    /// cleanup installs the new valid value before unwinding reaches the caller;
    /// the result is not a successful decode. No no-panic premise is used here.
    /// No borrowed input view escapes through the selected dependency's owned
    /// parse-from-bytes API. Arbitrary safe Message implementations cannot cause
    /// this adapter to create raw references or assume uninitialized values.
    fn decode(
        &self,
        encoding: PayloadEncoding,
        bytes: &[u8],
        into: &mut T,
    ) -> Result<(), WireError> {
        if encoding != PayloadEncoding::PROTOBUF {
            return Err(WireError::UnknownEncoding(encoding));
        }
        let decoded = T::parse_from_bytes(bytes)
            .map_err(|_| WireError::Malformed("protobuf parsing failed"))?;
        *into = decoded;
        Ok(())
    }
}

fn checked_size(size: u64) -> Result<usize, WireError> {
    if size > MAX_MESSAGE_SIZE {
        return Err(WireError::Unencodable(
            "protobuf message exceeds format size limit",
        ));
    }
    usize::try_from(size)
        .map_err(|_| WireError::Unencodable("protobuf message exceeds address space"))
}
