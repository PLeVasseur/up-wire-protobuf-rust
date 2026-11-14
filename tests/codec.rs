// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0
#![forbid(unsafe_code)]
use core::mem::MaybeUninit;
use protobuf::{
    well_known_types::wrappers::{StringValue, UInt32Value},
    Message,
};
use up_rust_core::uattributes::PayloadEncoding;
use up_rust_core::wire::{EncodedSize, PayloadWriter, RuntimeHeap, Wire, WireError};
use up_wire_protobuf::ProtobufWire;

fn string(value: &str) -> StringValue {
    StringValue {
        value: value.into(),
        ..Default::default()
    }
}

#[test]
fn generated_string_bytes_match_stock_runtime_and_golden_wire() {
    let value = string("hello");
    let mut storage = [MaybeUninit::uninit(); 7];
    let mut writer = PayloadWriter::new(&mut storage);
    assert_eq!(ProtobufWire.encoded_size(&value), EncodedSize::Exact(7));
    ProtobufWire.encode(&value, &mut writer).unwrap();
    assert_eq!(writer.as_written(), b"\x0a\x05hello");
    assert_eq!(writer.as_written(), value.write_to_bytes().unwrap());
    assert_eq!(
        StringValue::parse_from_bytes(writer.as_written()).unwrap(),
        value
    );
}

#[test]
fn zero_length_message_uses_empty_buffer() {
    let value = UInt32Value::new();
    let mut storage = [];
    let mut writer = PayloadWriter::new(&mut storage);
    ProtobufWire.encode(&value, &mut writer).unwrap();
    assert_eq!(writer.as_written(), &[]);
    let mut into = UInt32Value {
        value: 99,
        ..Default::default()
    };
    ProtobufWire
        .decode(PayloadEncoding::PROTOBUF, &[], &mut into)
        .unwrap();
    assert_eq!(into.value, 0);
}

#[test]
fn insufficient_buffer_preserves_existing_writer_prefix() {
    let value = string("hello");
    let mut storage = [MaybeUninit::uninit(); 7];
    let mut writer = PayloadWriter::new(&mut storage);
    writer.put(&[0x42]).unwrap();
    assert!(matches!(
        ProtobufWire.encode(&value, &mut writer),
        Err(WireError::BufferTooSmall {
            needed: 7,
            available: 6
        })
    ));
    assert_eq!(writer.as_written(), &[0x42]);
}

#[test]
fn malformed_input_keeps_prior_owned_value() {
    let mut value = string("preserved");
    assert!(ProtobufWire
        .decode(PayloadEncoding::PROTOBUF, b"\x0a\x05x", &mut value)
        .is_err());
    assert_eq!(value.value, "preserved");
}

#[test]
fn wrong_encoding_keeps_prior_owned_value() {
    let mut value = string("preserved");
    assert!(matches!(
        ProtobufWire.decode(PayloadEncoding::RAW, &[], &mut value),
        Err(WireError::UnknownEncoding(PayloadEncoding::RAW))
    ));
    assert_eq!(value.value, "preserved");
}

#[test]
fn stock_unknown_and_duplicate_field_semantics_are_preserved() {
    let input = b"\x0a\x01a\x10\x07\x0a\x01b";
    let mut value = StringValue::new();
    ProtobufWire
        .decode(PayloadEncoding::PROTOBUF, input, &mut value)
        .unwrap();
    assert_eq!(value, StringValue::parse_from_bytes(input).unwrap());
    assert_eq!(value.value, "b");
    assert_eq!(value.unknown_fields().iter().count(), 1);
}

#[test]
fn reports_serialized_identity_and_conservative_heap_use() {
    let info = <ProtobufWire as Wire<StringValue>>::describe(&ProtobufWire);
    assert_eq!(info.identity.encoding, PayloadEncoding::PROTOBUF);
    assert_eq!(info.identity.native_type_token, None);
    assert!(matches!(info.encode_heap(), RuntimeHeap::Allocates(_)));
    assert!(matches!(info.decode_heap(), RuntimeHeap::Allocates(_)));
    assert_eq!(info.max_encoded_len, None);
    assert!(info.exact_size);
}
