import gleam/erlang/process
import mist

/// OTP primitives only; service decisions live in Gleam.
@external(erlang, "highway_runtime_ffi", "now")
pub fn now() -> Int

@external(erlang, "highway_runtime_ffi", "monotonic_ms")
pub fn monotonic_ms() -> Int

@external(erlang, "highway_runtime_ffi", "incarnation")
pub fn incarnation() -> String

pub type Slots

@external(erlang, "highway_runtime_ffi", "slots")
pub fn slots() -> Slots

@external(erlang, "highway_runtime_ffi", "acquire")
pub fn acquire(slots: Slots, limit: Int) -> Bool

@external(erlang, "highway_runtime_ffi", "release")
pub fn release(slots: Slots, pid: process.Pid) -> Nil

@external(erlang, "highway_runtime_ffi", "socket_limits")
pub fn socket_limits(
  connection: mist.WebsocketConnection,
) -> Result(Nil, String)

@external(erlang, "highway_runtime_ffi", "close")
pub fn close(
  connection: mist.WebsocketConnection,
  code: Int,
) -> Result(Nil, String)

pub type Permit

/// A permit is released by the receiver only after dequeuing the request.
/// Timeouts and dead callers cannot open new mailbox capacity prematurely.
@external(erlang, "highway_runtime_ffi", "permit")
pub fn permit(gate: Slots) -> Result(Permit, Nil)

@external(erlang, "highway_runtime_ffi", "release_permit")
pub fn release_permit(gate: Slots, slot: Permit) -> Nil

@external(erlang, "highway_runtime_ffi", "reset")
pub fn reset(gate: Slots) -> Nil

import gleam/option.{type Option}
import highway/domain.{type Cursor}

@external(erlang, "highway_runtime_ffi", "set_account")
pub fn set_account(
  gate: Slots,
  bytes: Int,
  healthy: Bool,
  persisted: Option(Cursor),
) -> Nil

@external(erlang, "highway_runtime_ffi", "account")
pub fn account(gate: Slots) -> #(Int, Bool, Option(Cursor))

@external(erlang, "highway_runtime_ffi", "set_missing")
pub fn set_missing(gate: Slots, missing: Bool) -> Nil

@external(erlang, "highway_runtime_ffi", "is_missing")
pub fn is_missing(gate: Slots) -> Bool

/// Reserve append bytes before enqueueing. The owner releases the reservation
/// after updating its account, including when a caller has already timed out.
@external(erlang, "highway_runtime_ffi", "reserve_bytes")
pub fn reserve_bytes(gate: Slots, bytes: Int) -> Result(Nil, Nil)

@external(erlang, "highway_runtime_ffi", "release_bytes")
pub fn release_bytes(gate: Slots, bytes: Int) -> Nil

/// Bounded synchronous call with an OTP reply alias. Late replies are dropped
/// and monitors are always removed, including timeout/callee failure paths.
@external(erlang, "highway_runtime_ffi", "call")
pub fn call(
  subject: process.Subject(message),
  timeout_ms: Int,
  make: fn(process.Subject(reply)) -> message,
) -> Result(reply, String)

/// Terminate a temporary supervised worker without linking its exit to caller.
@external(erlang, "highway_runtime_ffi", "shutdown")
pub fn shutdown(pid: process.Pid) -> Nil

@external(erlang, "highway_runtime_ffi", "set_drained")
pub fn set_drained(gate: Slots, drained: Bool) -> Nil

@external(erlang, "highway_runtime_ffi", "is_drained")
pub fn is_drained(gate: Slots) -> Bool

/// BEAM allocated memory, not OS/container RSS. Used as a fixed node gauge.
@external(erlang, "highway_runtime_ffi", "memory_bytes")
pub fn memory_bytes() -> Int
