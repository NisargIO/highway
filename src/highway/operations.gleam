/// Fixed-cardinality operational state. No tenant, stream, token or payload is
/// stored here. This table survives registry restarts and its drain latch sticks.
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string
import highway/runtime
import highway/storage

pub opaque type Context {
  Context(table: Table)
}

pub type Table

pub type Metric {
  AcceptedEvents
  AcceptedBytes
  PersistedEvents
  PersistedBytes
  PublishRejected
  StorageRequests
  StorageErrors
  StorageDurationMs
  StorageReadBytes
  ReplayItems
  ReplayErrors
  ConnectionsTotal
  AuthFailures
  SlowConsumers
  DrainCompleted
  DrainTimeout
}

pub type Activity {
  Connection
  Replay
}

@external(erlang, "highway_operations_ffi", "new")
fn new_table() -> Table

pub fn new() -> Context {
  Context(new_table())
}

@external(erlang, "highway_operations_ffi", "add")
fn counter_add(t: Table, metric: Metric, n: Int) -> Nil

pub fn add(c: Context, metric: Metric, n: Int) -> Nil {
  counter_add(c.table, metric, n)
}

pub fn record(c: Option(Context), metric: Metric, n: Int) -> Nil {
  case c {
    None -> Nil
    Some(c) -> add(c, metric, n)
  }
}

@external(erlang, "highway_operations_ffi", "counter")
fn counter(t: Table, metric: Metric) -> Int

@external(erlang, "highway_operations_ffi", "set_phase")
fn set_phase(t: Table, phase: Int) -> Nil

@external(erlang, "highway_operations_ffi", "phase")
fn phase(t: Table) -> Int

pub fn begin_drain(c: Context) -> Nil {
  set_phase(c.table, 1)
}

pub fn finish_drain(c: Context) -> Nil {
  set_phase(c.table, 2)
}

pub fn draining(c: Context) -> Bool {
  phase(c.table) >= 1
}

pub fn closing(c: Context) -> Bool {
  phase(c.table) >= 2
}

@external(erlang, "highway_operations_ffi", "health_set")
fn health_set(t: Table, healthy: Bool, now: Int) -> Nil

@external(erlang, "highway_operations_ffi", "health")
fn health_get(t: Table) -> #(Bool, Int)

pub fn probe(c: Context, healthy: Bool, now: Int) -> Nil {
  health_set(c.table, healthy, now)
}

pub fn storage_ready(c: Context, now: Int, stale_ms: Int) -> Bool {
  let #(healthy, checked) = health_get(c.table)
  healthy && now - checked >= 0 && now - checked < stale_ms
}

@external(erlang, "highway_operations_ffi", "track")
fn track(t: Table, kind: Activity, active: Bool) -> Nil

@external(erlang, "highway_operations_ffi", "active")
fn count(t: Table, kind: Activity) -> Int

pub fn activity(c: Context, kind: Activity, active: Bool) -> Nil {
  track(c.table, kind, active)
}

pub fn active(c: Context, kind: Activity) -> Int {
  count(c.table, kind)
}

pub fn measured(c: Context, store: storage.Store) -> storage.Store {
  storage.Store(
    fn(key) {
      let start = runtime.monotonic_ms()
      let outcome = store.get(key)
      observe(c, start, outcome)
      case outcome {
        Ok(o) -> add(c, StorageReadBytes, string.byte_size(o.body))
        _ -> Nil
      }
      outcome
    },
    fn(key, body, condition) {
      let start = runtime.monotonic_ms()
      let outcome = store.put(key, body, condition)
      observe(c, start, outcome)
      outcome
    },
  )
}

fn observe(c: Context, start: Int, outcome: Result(a, storage.Error)) -> Nil {
  add(c, StorageRequests, 1)
  add(c, StorageDurationMs, runtime.monotonic_ms() - start)
  case outcome {
    Error(_) -> add(c, StorageErrors, 1)
    Ok(_) -> Nil
  }
}

fn name(m: Metric) -> String {
  case m {
    AcceptedEvents -> "accepted_events"
    AcceptedBytes -> "accepted_bytes"
    PersistedEvents -> "persisted_events"
    PersistedBytes -> "persisted_bytes"
    PublishRejected -> "publish_rejected"
    StorageRequests -> "storage_requests"
    StorageErrors -> "storage_errors"
    StorageDurationMs -> "storage_duration_milliseconds"
    StorageReadBytes -> "storage_read_bytes"
    ReplayItems -> "replay_items"
    ReplayErrors -> "replay_errors"
    ConnectionsTotal -> "connections"
    AuthFailures -> "auth_failures"
    SlowConsumers -> "slow_consumers"
    DrainCompleted -> "drain_completed"
    DrainTimeout -> "drain_timeout"
  }
}

pub fn gauge(name: String, value: Int) -> String {
  "# TYPE highway_"
  <> name
  <> " gauge\nhighway_"
  <> name
  <> " "
  <> int.to_string(value)
  <> "\n"
}

pub fn render(c: Context) -> String {
  let counters =
    list.map(
      [
        AcceptedEvents, AcceptedBytes, PersistedEvents, PersistedBytes,
        PublishRejected, StorageRequests, StorageErrors, StorageDurationMs,
        StorageReadBytes, ReplayItems, ReplayErrors, ConnectionsTotal,
        AuthFailures, SlowConsumers, DrainCompleted, DrainTimeout,
      ],
      fn(m) {
        let key = "highway_" <> name(m) <> "_total"
        "# TYPE "
        <> key
        <> " counter\n"
        <> key
        <> " "
        <> int.to_string(counter(c.table, m))
        <> "\n"
      },
    )
    |> string.join("")
  counters
  <> gauge("connections_active", active(c, Connection))
  <> gauge("replay_active", active(c, Replay))
  <> gauge("draining", case draining(c) {
    True -> 1
    False -> 0
  })
}
