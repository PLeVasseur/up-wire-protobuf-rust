// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0
//! Public role controls, not middleware or no-heap qualification. The small
//! lending fixture allocates explicitly and deliberately refuses truncation.

use protobuf::{well_known_types::wrappers::StringValue, Message};
use std::{
    mem::MaybeUninit,
    sync::{Arc, Mutex},
};
use up_rust_core::{
    communication::{two_mode::{Publisher, RpcClient, RpcServer, Subscriber}, LocalUriProvider},
    frame::header::WrittenHeader,
    memory::StartupRegion,
    transport::{
        BindingRefusal, BindingRoute, BindingSetup, Capabilities, Envelope, Frame, InboundRoute,
        InitializedTx, LendingRuntime, LoanError, LoanedPayload, PayloadLoanProvenance, PeerRef,
        PreparedRoute, PreparedTxStorage, QosSupport, ReceiveBudget, RouteDeclaration, RouteKind,
        RuntimePort, SendError, SendReport, TransportSetup, TxFrameLayout,
    },
    uattributes::{PayloadEncoding, UMessageType},
    uri::UUri,
    wire::{
        EncodedSize, PayloadWriter, Ready, RuntimeHeap, StartupSlot, Wire, WireError, WireIdentity,
        WireInfo, WirePayloadIdentity,
    },
};
use up_wire_protobuf::ProtobufWire;

const ENVELOPES: &[Envelope] = &[Envelope::UFrameFields];

struct Uri(u16);
impl LocalUriProvider for Uri {
    fn get_resource_uri(&self, resource: u16) -> UUri {
        UUri::try_from_wire_parts("local", (1 << 16) | u32::from(self.0), 1, resource).unwrap()
    }
    fn get_source_uri(&self) -> UUri {
        self.get_resource_uri(0)
    }
}

#[derive(Default)]
struct State {
    port: Option<RuntimePort>,
    routes: Vec<(Option<UMessageType>, Option<InboundRoute>)>,
    publishes: usize,
    last_payload: Vec<u8>,
    corrupt_next: bool,
}
#[derive(Clone, Default)]
struct Loopback(Arc<Mutex<State>>);

impl BindingSetup for Loopback {
    type Running = Self;
    fn name(&self) -> &'static str {
        "serialized-no-truncate-fixture"
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::new(Envelope::UFrameFields, ENVELOPES, 4096)
            .with_per_message_encoding(true)
            .with_truncate_payload(false)
    }
    fn qos_support(&self, _: RouteKind) -> QosSupport {
        QosSupport::new()
            .with_reliable(true)
            .with_drop_oldest(true)
            .with_drop_newest(true)
            .with_wait(true)
            .with_max_depth(64)
    }
    fn prepare_route(
        &mut self,
        route: &RouteDeclaration<'_>,
    ) -> Result<PreparedRoute, BindingRefusal> {
        let mut state = self.0.lock().unwrap();
        let id = BindingRoute(state.routes.len() as u32);
        state.routes.push((route.kind, route.inbound));
        Ok(PreparedRoute::new(
            id,
            RuntimeHeap::Allocates("explicit test storage"),
        ))
    }
    fn into_running(self, port: RuntimePort) -> Result<Self, BindingRefusal> {
        self.0.lock().unwrap().port = Some(port);
        Ok(self)
    }
}

struct Loan {
    metadata: Box<[MaybeUninit<u8>]>,
    payload: Box<[MaybeUninit<u8>]>,
}

// SAFETY: each Box uniquely owns a distinct initialized-MaybeUninit byte slice.
// Accessors reborrow those fixed allocations, never resize/replace/drop them,
// and metadata/payload cannot overlap. record_header only checks length. Moving
// Loan leaves both allocations stable; consumption alone releases ownership.
// Empty slices are valid, aligned byte slices and never dereferenced out of bounds.
unsafe impl PreparedTxStorage for Loan {
    fn metadata_uninit_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        &mut self.metadata
    }
    fn payload_uninit_mut(&mut self) -> Option<&mut [MaybeUninit<u8>]> {
        Some(&mut self.payload)
    }
    fn record_header(&mut self, header: WrittenHeader) -> Result<(), SendError> {
        if header.len == self.metadata.len() {
            Ok(())
        } else {
            Err(SendError::PayloadStorage)
        }
    }
    // Deliberately retain the default truncate_payload refusal.
}

struct OwnedFrame {
    metadata: Vec<u8>,
    payload: Vec<u8>,
}
impl Frame for OwnedFrame {
    fn encoded_metadata(&self) -> &[u8] {
        &self.metadata
    }
    fn payload(&self) -> Option<LoanedPayload<'_>> {
        Some(LoanedPayload::new(
            &self.payload,
            PayloadLoanProvenance::OpaqueTransportLoan,
        ))
    }
    fn peer(&self) -> PeerRef {
        PeerRef::UNKNOWN
    }
}

impl LendingRuntime for Loopback {
    type Tx<'a> = Loan;
    type Rx = OwnedFrame;
    fn loan(&self, _: BindingRoute, layout: TxFrameLayout) -> Result<Loan, LoanError> {
        if layout.payload_alignment != 1 {
            return Err(LoanError::InvalidRoute);
        }
        Ok(Loan {
            metadata: vec![MaybeUninit::uninit(); layout.metadata_len].into_boxed_slice(),
            payload: vec![MaybeUninit::uninit(); layout.payload_len.unwrap_or(0)]
                .into_boxed_slice(),
        })
    }
    fn publish<'a>(
        &'a self,
        route: BindingRoute,
        frame: InitializedTx<Loan>,
    ) -> Result<SendReport, SendError> {
        let loan = frame.into_storage();
        // SAFETY: InitializedTx certifies every byte of both exact logical
        // extents. Loan's unsafe-trait invariant preserved these fixed allocations
        // and initialized bytes since the core filled them. Per-byte reads stay
        // within each allocation; the owning Boxes remain live throughout.
        let metadata = loan
            .metadata
            .iter()
            .map(|b| unsafe { b.assume_init() })
            .collect();
        // SAFETY: the same certificate and ownership argument applies to payload.
        let mut payload: Vec<u8> = loan
            .payload
            .iter()
            .map(|b| unsafe { b.assume_init() })
            .collect();
        let (port, inbound) = {
            let mut state = self.0.lock().unwrap();
            state.publishes += 1;
            state.last_payload = payload.clone();
            if state.corrupt_next {
                state.corrupt_next = false;
                payload = b"\x0a\x05x".to_vec();
            }
            let kind = state.routes[route.0 as usize].0;
            let inbound = state
                .routes
                .iter()
                .find_map(|(k, r)| (*k == kind).then_some(*r).flatten());
            (state.port.unwrap(), inbound)
        };
        if let Some(inbound) = inbound {
            port.deliver_frame(inbound, OwnedFrame { metadata, payload })
                .map_err(|_| SendError::Delivery)?;
        }
        Ok(SendReport::exact(1, 1))
    }
    fn receive(&self, _: BindingRoute) -> Option<OwnedFrame> {
        None
    }
    fn receive_bounded(&self, _: BindingRoute, _: &ReceiveBudget) -> Option<OwnedFrame> {
        None
    }
}

fn string(value: &str) -> StringValue {
    StringValue {
        value: value.into(),
        ..Default::default()
    }
}

#[test]
fn protobuf_public_pubsub_uses_exact_no_truncate_loans_and_reuses_owned_slots() {
    static REGION: StartupRegion<{ 1 << 19 }> = StartupRegion::new();
    let memory = REGION.claim().unwrap();
    let binding = Loopback::default();
    let setup = TransportSetup::lending_in(&memory, binding.clone()).unwrap();
    let publisher = Publisher::<StringValue>::on(&setup, &Uri(1), 0x8001_u16)
        .wire(ProtobufWire)
        .max_payload(256)
        .build()
        .unwrap();
    let subscriber = Subscriber::<StringValue>::on(&setup, Uri(1).get_resource_uri(0x8001))
        .wire(ProtobufWire)
        .max_payload(256)
        .max_held(1)
        .build()
        .unwrap();
    let runtime = setup.enter_runtime().unwrap();
    let publisher = publisher.activate(&runtime).unwrap();
    let subscriber = subscriber.activate(&runtime).unwrap();
    for text in [
        "first owned value",
        "second",
        "",
        "longer final owned value",
    ] {
        let message = string(text);
        publisher.publish(&message).unwrap();
        assert_eq!(
            binding.0.lock().unwrap().last_payload,
            message.write_to_bytes().unwrap()
        );
        let received = subscriber.try_recv().unwrap().unwrap();
        assert_eq!(received.value, text);
        drop(received);
    }
    assert_eq!(binding.0.lock().unwrap().publishes, 4);
    // A malformed frame must not publish a partial or previous decoded value.
    binding.0.lock().unwrap().corrupt_next = true;
    assert!(publisher.publish(&string("malformed on ingress")).is_err());
    assert!(subscriber.try_recv().unwrap().is_none());
    publisher.publish(&string("after refusal")).unwrap();
    assert_eq!(
        subscriber.try_recv().unwrap().unwrap().value,
        "after refusal"
    );
}

#[test]
fn protobuf_public_rpc_reuses_owned_response_and_request_slots() {
    static REGION: StartupRegion<{ 1 << 19 }> = StartupRegion::new();
    let memory = REGION.claim().unwrap();
    let setup = TransportSetup::lending_in(&memory, Loopback::default()).unwrap();
    let client =
        RpcClient::<StringValue, StringValue>::on(&setup, &Uri(1), Uri(2).get_resource_uri(1))
            .wire(ProtobufWire)
            .max_payload(256)
            .max_response(256)
            .max_in_flight(1)
            .build()
            .unwrap();
    let server = RpcServer::<StringValue, StringValue>::on(&setup, &Uri(2), 1_u16)
        .wire(ProtobufWire)
        .max_payload(256)
        .max_response(256)
        .build()
        .unwrap();
    let runtime = setup.enter_runtime().unwrap();
    let client = client.activate(&runtime).unwrap();
    let server = server.activate(&runtime).unwrap();
    for text in ["one", "two", "three"] {
        let mut call = client.invoke(&string(text)).unwrap();
        let request = server.try_recv().unwrap().unwrap();
        assert_eq!(request.value, text);
        request.respond(&string(text)).unwrap();
        let reply = call.try_response().unwrap().unwrap();
        assert_eq!(reply.value, text);
        drop(reply);
        assert_eq!(call.__taken_value().unwrap().value, text);
        drop(call);
    }
}

#[derive(Clone, Copy)]
struct ScalarWire;
impl Wire<u32> for ScalarWire {
    fn describe(&self) -> WireInfo {
        WireInfo::new(
            "partial-scalar",
            WireIdentity::new("partial.scalar", 0),
            WireIdentity::new("partial.scalar.family", 0),
            WirePayloadIdentity::serialized(PayloadEncoding::RAW),
            RuntimeHeap::None,
            RuntimeHeap::None,
        )
        .max_encoded_len(4)
        .exact()
    }
    fn encoded_size(&self, _: &u32) -> EncodedSize {
        EncodedSize::Exact(4)
    }
    fn encode(&self, value: &u32, out: &mut PayloadWriter<'_>) -> Result<(), WireError> {
        out.put(&value.to_le_bytes())
    }
    fn init_slot<'s>(&self, slot: StartupSlot<'s, u32>) -> Ready<'s, u32> {
        slot.write(0)
    }
    fn decode(&self, _: PayloadEncoding, bytes: &[u8], into: &mut u32) -> Result<(), WireError> {
        let n = u32::from_le_bytes(bytes.try_into().map_err(|_| WireError::InvalidBits)?);
        *into = if n == 99 { 7 } else { n };
        if n == 99 {
            Err(WireError::Malformed("changed value before refusal"))
        } else {
            Ok(())
        }
    }
}

#[test]
fn failed_scalar_rpc_decode_never_exposes_the_written_value() {
    static REGION: StartupRegion<{ 1 << 19 }> = StartupRegion::new();
    let memory = REGION.claim().unwrap();
    let setup = TransportSetup::lending_in(&memory, Loopback::default()).unwrap();
    let client = RpcClient::<u32, u32>::on(&setup, &Uri(1), Uri(2).get_resource_uri(1))
        .wire(ScalarWire)
        .max_in_flight(1)
        .build()
        .unwrap();
    let server = RpcServer::<u32, u32>::on(&setup, &Uri(2), 1_u16)
        .wire(ScalarWire)
        .build()
        .unwrap();
    let runtime = setup.enter_runtime().unwrap();
    let client = client.activate(&runtime).unwrap();
    let server = server.activate(&runtime).unwrap();
    let mut failed = client.invoke(&5).unwrap();
    server.try_recv().unwrap().unwrap().respond(&99).unwrap();
    assert!(failed.try_response().is_err());
    assert!(failed.__taken_value().is_none());
    drop(failed);
    let mut next = client.invoke(&6).unwrap();
    server.try_recv().unwrap().unwrap().respond(&8).unwrap();
    assert_eq!(*next.try_response().unwrap().unwrap(), 8);
    assert_eq!(next.__taken_value(), Some(&8));
}

// A safe open Message implementation reporting a format-invalid size. The
// production wire must refuse it before publish, rather than clamp/saturate.
#[derive(Clone, Default, PartialEq)]
struct Oversized {
    fields: protobuf::SpecialFields,
}
impl Message for Oversized {
    const NAME: &'static str = "Oversized";
    fn is_initialized(&self) -> bool {
        true
    }
    fn merge_from(&mut self, _: &mut protobuf::CodedInputStream<'_>) -> protobuf::Result<()> {
        Ok(())
    }
    fn write_to_with_cached_sizes(
        &self,
        _: &mut protobuf::CodedOutputStream<'_>,
    ) -> protobuf::Result<()> {
        panic!("invalid size must be refused before the serializer")
    }
    fn compute_size(&self) -> u64 {
        i32::MAX as u64 + 1
    }
    fn special_fields(&self) -> &protobuf::SpecialFields {
        &self.fields
    }
    fn mut_special_fields(&mut self) -> &mut protobuf::SpecialFields {
        &mut self.fields
    }
    fn new() -> Self {
        Self::default()
    }
    fn default_instance() -> &'static Self {
        static VALUE: std::sync::OnceLock<Oversized> = std::sync::OnceLock::new();
        VALUE.get_or_init(Self::default)
    }
}

#[test]
fn protobuf_invalid_size_is_refused_without_publication_on_no_truncate_binding() {
    static REGION: StartupRegion<{ 1 << 18 }> = StartupRegion::new();
    let memory = REGION.claim().unwrap();
    let binding = Loopback::default();
    let setup = TransportSetup::lending_in(&memory, binding.clone()).unwrap();
    let role = Publisher::<Oversized>::on(&setup, &Uri(1), 0x8001_u16)
        .wire(ProtobufWire)
        .max_payload(64)
        .build()
        .unwrap();
    let runtime = setup.enter_runtime().unwrap();
    let role = role.activate(&runtime).unwrap();
    let value = Oversized::new();
    assert_eq!(ProtobufWire.encoded_size(&value), EncodedSize::Unknown);
    assert!(matches!(
        role.publish(&value),
        Err(SendError::Wire(WireError::Unencodable(_)))
    ));
    assert_eq!(binding.0.lock().unwrap().publishes, 0);
}

#[derive(Default)]
struct DropHook {
    action: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    calls: std::sync::atomic::AtomicUsize,
}
struct DropProbe {
    id: u8,
    armed: bool,
    hook: Arc<DropHook>,
}
impl Drop for DropProbe {
    fn drop(&mut self) {
        if self.armed {
            self.hook
                .calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let action = self.hook.action.lock().unwrap().clone();
            if let Some(action) = action {
                action();
            }
        }
    }
}
#[derive(Clone)]
struct DropWire(Arc<DropHook>);
impl Wire<DropProbe> for DropWire {
    fn describe(&self) -> WireInfo {
        WireInfo::new(
            "owned-drop",
            WireIdentity::new("owned.drop", 0),
            WireIdentity::new("owned.drop.family", 0),
            WirePayloadIdentity::serialized(PayloadEncoding::RAW),
            RuntimeHeap::Allocates("test owned drop"),
            RuntimeHeap::Allocates("test owned drop"),
        )
        .max_encoded_len(1)
        .exact()
    }
    fn encoded_size(&self, _: &DropProbe) -> EncodedSize {
        EncodedSize::Exact(1)
    }
    fn encode(&self, value: &DropProbe, out: &mut PayloadWriter<'_>) -> Result<(), WireError> {
        out.put(&[value.id])
    }
    fn init_slot<'s>(&self, slot: StartupSlot<'s, DropProbe>) -> Ready<'s, DropProbe> {
        slot.write(DropProbe {
            id: 0,
            armed: false,
            hook: self.0.clone(),
        })
    }
    fn decode(
        &self,
        _: PayloadEncoding,
        bytes: &[u8],
        into: &mut DropProbe,
    ) -> Result<(), WireError> {
        let [id] = bytes else {
            return Err(WireError::InvalidBits);
        };
        into.id = *id;
        into.armed = *id == 1;
        Ok(())
    }
}

fn drop_probe_roles(
    memory: &up_rust_core::memory::StartupMemory,
    hook: &Arc<DropHook>,
) -> (
    RpcClient<DropProbe, DropProbe>,
    RpcServer<DropProbe, DropProbe>,
) {
    let setup = TransportSetup::lending_in(memory, Loopback::default()).unwrap();
    let client = RpcClient::<DropProbe, DropProbe>::on(&setup, &Uri(1), Uri(2).get_resource_uri(1))
        .wire(DropWire(hook.clone()))
        .max_in_flight(1)
        .build()
        .unwrap();
    let server = RpcServer::<DropProbe, DropProbe>::on(&setup, &Uri(2), 1_u16)
        .wire(DropWire(hook.clone()))
        .build()
        .unwrap();
    let runtime = setup.enter_runtime().unwrap();
    (
        client.activate(&runtime).unwrap(),
        server.activate(&runtime).unwrap(),
    )
}

fn unarmed(hook: &Arc<DropHook>, id: u8) -> DropProbe {
    DropProbe {
        id,
        armed: false,
        hook: hook.clone(),
    }
}

#[test]
fn rpc_response_destructor_reentry_cannot_reacquire_its_releasing_slot() {
    static REGION: StartupRegion<{ 1 << 19 }> = StartupRegion::new();
    let memory = REGION.claim().unwrap();
    let hook = Arc::new(DropHook::default());
    let (client, server) = drop_probe_roles(&memory, &hook);
    let nested_hook = hook.clone();
    *hook.action.lock().unwrap() = Some(Arc::new(move || {
        assert!(matches!(
            client.invoke(&unarmed(&nested_hook, 0)),
            Err(up_rust_core::transport::CallError::SlotsExhausted)
        ));
    }));
    let mut call = client.invoke(&unarmed(&hook, 0)).unwrap();
    server
        .try_recv()
        .unwrap()
        .unwrap()
        .respond(&unarmed(&hook, 1))
        .unwrap();
    assert_eq!(call.try_response().unwrap().unwrap().id, 1);
    drop(call); // Executes arbitrary safe Drop while the only index is withheld.
    assert_eq!(hook.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    // Release completed normally, so a new call may now claim the slot.
    let next = client.invoke(&unarmed(&hook, 0)).unwrap();
    drop(next);
    *hook.action.lock().unwrap() = None;
}

#[test]
fn rpc_response_destructor_unwind_withholds_slot_and_never_redrops_value() {
    static REGION: StartupRegion<{ 1 << 19 }> = StartupRegion::new();
    let memory = REGION.claim().unwrap();
    let hook = Arc::new(DropHook::default());
    let (client, server) = drop_probe_roles(&memory, &hook);
    *hook.action.lock().unwrap() =
        Some(Arc::new(|| panic!("intentional safe response destructor")));
    let mut call = client.invoke(&unarmed(&hook, 0)).unwrap();
    server
        .try_recv()
        .unwrap()
        .unwrap()
        .respond(&unarmed(&hook, 1))
        .unwrap();
    assert_eq!(call.try_response().unwrap().unwrap().id, 1);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(call))).is_err());
    assert!(matches!(
        client.invoke(&unarmed(&hook, 0)),
        Err(up_rust_core::transport::CallError::SlotsExhausted)
    ));
    assert_eq!(hook.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    *hook.action.lock().unwrap() = None;
}

#[test]
fn forgotten_owned_response_call_withholds_its_slot_and_value() {
    static REGION: StartupRegion<{ 1 << 19 }> = StartupRegion::new();
    let memory = REGION.claim().unwrap();
    let hook = Arc::new(DropHook::default());
    let (client, server) = drop_probe_roles(&memory, &hook);
    let mut call = client.invoke(&unarmed(&hook, 0)).unwrap();
    server
        .try_recv()
        .unwrap()
        .unwrap()
        .respond(&unarmed(&hook, 1))
        .unwrap();
    assert_eq!(call.try_response().unwrap().unwrap().id, 1);
    std::mem::forget(call);
    assert!(matches!(
        client.invoke(&unarmed(&hook, 0)),
        Err(up_rust_core::transport::CallError::SlotsExhausted)
    ));
    assert_eq!(hook.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[derive(Default)]
struct ReplacementCounts {
    panic_once: std::sync::atomic::AtomicBool,
    seed_drops: std::sync::atomic::AtomicUsize,
    replacement_drops: std::sync::atomic::AtomicUsize,
}
enum Origin {
    Input,
    Seed,
    Replacement,
}
struct ReplacementValue {
    origin: Origin,
    value: u8,
    counts: Arc<ReplacementCounts>,
}
impl Drop for ReplacementValue {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering::SeqCst;
        match self.origin {
            Origin::Input => {}
            Origin::Seed => {
                self.counts.seed_drops.fetch_add(1, SeqCst);
                assert!(
                    !self.counts.panic_once.swap(false, SeqCst),
                    "intentional old-value Drop"
                );
            }
            Origin::Replacement => {
                self.counts.replacement_drops.fetch_add(1, SeqCst);
            }
        }
    }
}
#[derive(Clone)]
struct ReplacementWire(Arc<ReplacementCounts>);
impl Wire<ReplacementValue> for ReplacementWire {
    fn describe(&self) -> WireInfo {
        WireInfo::new(
            "assignment-replacement",
            WireIdentity::new("assignment.replace", 0),
            WireIdentity::new("assignment.replace.family", 0),
            WirePayloadIdentity::serialized(PayloadEncoding::RAW),
            RuntimeHeap::Allocates("test owned replacement"),
            RuntimeHeap::Allocates("test owned replacement"),
        )
        .max_encoded_len(1)
        .exact()
    }
    fn encoded_size(&self, _: &ReplacementValue) -> EncodedSize {
        EncodedSize::Exact(1)
    }
    fn encode(
        &self,
        value: &ReplacementValue,
        out: &mut PayloadWriter<'_>,
    ) -> Result<(), WireError> {
        out.put(&[value.value])
    }
    fn init_slot<'s>(
        &self,
        slot: StartupSlot<'s, ReplacementValue>,
    ) -> Ready<'s, ReplacementValue> {
        slot.write(ReplacementValue {
            origin: Origin::Seed,
            value: 0,
            counts: self.0.clone(),
        })
    }
    fn decode(
        &self,
        _: PayloadEncoding,
        bytes: &[u8],
        into: &mut ReplacementValue,
    ) -> Result<(), WireError> {
        let [value] = bytes else {
            return Err(WireError::InvalidBits);
        };
        // Ordinary, entirely safe assignment. Rust installs this replacement on
        // both the normal and old-value-Drop unwind paths. No raw destruction,
        // uninitialized capability, or hidden unsafe caller contract is involved.
        *into = ReplacementValue {
            origin: Origin::Replacement,
            value: *value,
            counts: self.0.clone(),
        };
        Ok(())
    }
}
impl Wire<u32> for ReplacementWire {
    fn describe(&self) -> WireInfo {
        ScalarWire.describe()
    }
    fn encoded_size(&self, value: &u32) -> EncodedSize {
        ScalarWire.encoded_size(value)
    }
    fn encode(&self, value: &u32, out: &mut PayloadWriter<'_>) -> Result<(), WireError> {
        ScalarWire.encode(value, out)
    }
    fn init_slot<'s>(&self, slot: StartupSlot<'s, u32>) -> Ready<'s, u32> {
        slot.write(0)
    }
    fn decode(
        &self,
        encoding: PayloadEncoding,
        bytes: &[u8],
        into: &mut u32,
    ) -> Result<(), WireError> {
        ScalarWire.decode(encoding, bytes, into)
    }
}

fn replacement_input(counts: &Arc<ReplacementCounts>) -> ReplacementValue {
    ReplacementValue {
        origin: Origin::Input,
        value: 42,
        counts: counts.clone(),
    }
}

#[test]
fn rx_safe_assignment_old_drop_unwind_cleans_the_installed_replacement_once() {
    use std::sync::atomic::Ordering::SeqCst;
    static REGION: StartupRegion<{ 1 << 19 }> = StartupRegion::new();
    let memory = REGION.claim().unwrap();
    let counts = Arc::new(ReplacementCounts::default());
    let setup = TransportSetup::lending_in(&memory, Loopback::default()).unwrap();
    let publisher = Publisher::<ReplacementValue>::on(&setup, &Uri(1), 0x8001_u16)
        .wire(ReplacementWire(counts.clone()))
        .build()
        .unwrap();
    let subscriber = Subscriber::<ReplacementValue>::on(&setup, Uri(1).get_resource_uri(0x8001))
        .wire(ReplacementWire(counts.clone()))
        .build()
        .unwrap();
    let runtime = setup.enter_runtime().unwrap();
    let publisher = publisher.activate(&runtime).unwrap();
    let subscriber = subscriber.activate(&runtime).unwrap();
    counts.panic_once.store(true, SeqCst);
    let input = replacement_input(&counts);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = publisher.publish(&input);
    }))
    .is_err());
    // DecodeGuard destroys the valid replacement left by safe assignment;
    // it does not destroy the old seed a second time or publish a partial value.
    assert_eq!(counts.seed_drops.load(SeqCst), 1);
    assert_eq!(counts.replacement_drops.load(SeqCst), 1);
    assert!(subscriber.try_recv().unwrap().is_none());
    // Reuse is safe only after that complete cleanup, not because a panic passed.
    publisher.publish(&input).unwrap();
    let received = subscriber.try_recv().unwrap().unwrap();
    assert_eq!(received.value, 42);
    drop(received);
    assert_eq!(counts.seed_drops.load(SeqCst), 2);
    assert_eq!(counts.replacement_drops.load(SeqCst), 2);
}

#[test]
fn rpc_safe_assignment_old_drop_unwind_keeps_valid_unpublished_replacement() {
    use std::sync::atomic::Ordering::SeqCst;
    static REGION: StartupRegion<{ 1 << 19 }> = StartupRegion::new();
    let memory = REGION.claim().unwrap();
    let counts = Arc::new(ReplacementCounts::default());
    let setup = TransportSetup::lending_in(&memory, Loopback::default()).unwrap();
    let client =
        RpcClient::<u32, ReplacementValue>::on(&setup, &Uri(1), Uri(2).get_resource_uri(1))
            .wire(ReplacementWire(counts.clone()))
            .max_in_flight(1)
            .build()
            .unwrap();
    let server = RpcServer::<u32, ReplacementValue>::on(&setup, &Uri(2), 1_u16)
        .wire(ReplacementWire(counts.clone()))
        .build()
        .unwrap();
    let runtime = setup.enter_runtime().unwrap();
    let client = client.activate(&runtime).unwrap();
    let server = server.activate(&runtime).unwrap();
    let mut call = client.invoke(&5).unwrap();
    server
        .try_recv()
        .unwrap()
        .unwrap()
        .respond(&replacement_input(&counts))
        .unwrap();
    counts.panic_once.store(true, SeqCst);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = call.try_response();
    }))
    .is_err());
    assert!(call.__taken_value().is_none());
    assert_eq!(counts.seed_drops.load(SeqCst), 1);
    assert_eq!(counts.replacement_drops.load(SeqCst), 0);
    drop(call); // The Call still owns the valid replacement until this cleanup.
    assert_eq!(counts.seed_drops.load(SeqCst), 1);
    assert_eq!(counts.replacement_drops.load(SeqCst), 1);
    let mut next = client.invoke(&6).unwrap();
    server
        .try_recv()
        .unwrap()
        .unwrap()
        .respond(&replacement_input(&counts))
        .unwrap();
    assert_eq!(next.try_response().unwrap().unwrap().value, 42);
    drop(next);
    assert_eq!(counts.seed_drops.load(SeqCst), 2);
    assert_eq!(counts.replacement_drops.load(SeqCst), 2);
}
