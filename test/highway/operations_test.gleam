import gleam/erlang/process
import gleam/json
import gleam/option.{None, Some}
import gleam/string
import highway/config
import highway/domain
import highway/lifecycle
import highway/operations
import highway/owner
import highway/registry
import highway/runtime
import highway/storage
import highway/storage/archive
import highway/storage/format
import highway/storage_fake as fake

fn wait_ready(r: registry.Registry, attempts: Int) -> Nil {
  case registry.ready(r), attempts {
    True, _ -> Nil
    _, 0 -> panic as "probe not healthy"
    _, _ -> {
      process.sleep(5)
      wait_ready(r, attempts - 1)
    }
  }
}

fn wait_owner(r: registry.Registry, attempts: Int) -> owner.Owner {
  case
    registry.find(r, domain.Tenant("tenant"), domain.Stream("events")),
    attempts
  {
    Ok(#(o, _)), _ -> o
    _, 0 -> panic as "owner did not recover"
    _, _ -> {
      process.sleep(5)
      wait_owner(r, attempts - 1)
    }
  }
}

fn start(store: storage.Store) -> registry.Registry {
  let assert Ok(s) =
    registry.start_storage(
      registry.registration(),
      config.defaults(),
      Some(store),
      "test",
    )
  wait_ready(s.data, 200)
  s.data
}

fn scope() -> String {
  archive.scope("test", domain.Tenant("tenant"), domain.Stream("events"))
}

pub fn drain_waits_for_final_commit_and_ambiguous_close_confirmation_test() {
  let f = fake.new()
  let assert Ok(_) = archive.open(f.store, scope(), True)
  let blocked = process.new_subject()
  let store =
    storage.Store(f.store.get, fn(key, body, condition) {
      case format.decode_head(body) {
        Ok(h) if h.closed -> {
          let release = process.new_subject()
          process.send(blocked, release)
          let assert Ok(_) = process.receive(release, 2000)
          fake.fail(
            f,
            key,
            fake.After(storage.Ambiguous("lost close response")),
          )
          f.store.put(key, body, condition)
        }
        _ -> f.store.put(key, body, condition)
      }
    })
  let r = start(store)
  let o = wait_owner(r, 200)
  let assert Ok(event) =
    registry.publish(
      r,
      domain.Tenant("tenant"),
      domain.Stream("events"),
      "m",
      json.null(),
    )
  let outcomes = process.new_subject()
  let _ =
    process.spawn(fn() { process.send(outcomes, lifecycle.drain(r, 1500)) })
  let assert Ok(release) = process.receive(blocked, 2000)
  assert registry.draining(r)
  assert !registry.ready(r)
  assert registry.publish(
      r,
      domain.Tenant("tenant"),
      domain.Stream("events"),
      "rejected",
      json.null(),
    )
    == Error(domain.Unavailable)
  assert owner.append(o, "rejected", json.null(), runtime.monotonic_ms())
    == Error(domain.Unavailable)
  let assert Ok(snapshot) = registry.inspect(r)
  assert snapshot.pending == 0 && !snapshot.drain_complete
  assert owner.stats(o).2 == Some(event.cursor)
  let assert Ok(object) = f.store.get(scope() <> "head.json")
  let assert Ok(head) = format.decode_head(object.body)
  assert !head.closed
  process.send(release, Nil)
  let assert Ok(lifecycle.Drained) = process.receive(outcomes, 2000)
  let assert Ok(object) = f.store.get(scope() <> "head.json")
  let assert Ok(head) = format.decode_head(object.body)
  assert head.closed && head.archived == event.cursor
  let text = registry.metrics(r)
  assert string.contains(text, "highway_accepted_events_total 1\n")
  assert string.contains(text, "highway_persisted_events_total 1\n")
  assert string.contains(text, "highway_drain_completed_total 1\n")
  assert !string.contains(text, "tenant") && !string.contains(text, "events\"")
  registry.stop(r)
  fake.stop(f)
}

pub fn drain_timeout_never_claims_or_closes_an_unconfirmed_tail_test() {
  let f = fake.new()
  let assert Ok(_) = archive.open(f.store, scope(), True)
  let blocked = process.new_subject()
  let store =
    storage.Store(f.store.get, fn(key, body, condition) {
      case format.decode_head(body) {
        Ok(h) if h.archived.offset > 0 -> {
          let release = process.new_subject()
          process.send(blocked, release)
          let assert Ok(_) = process.receive(release, 2000)
          f.store.put(key, body, condition)
        }
        _ -> f.store.put(key, body, condition)
      }
    })
  let r = start(store)
  let o = wait_owner(r, 200)
  let assert Ok(_) =
    registry.publish(
      r,
      domain.Tenant("tenant"),
      domain.Stream("events"),
      "m",
      json.null(),
    )
  let start = runtime.monotonic_ms()
  assert lifecycle.drain(r, 100) == lifecycle.TimedOut
  assert runtime.monotonic_ms() - start < 500
  let assert Ok(_) = process.receive(blocked, 1000)
  let assert Ok(s) = registry.inspect(r)
  assert !s.drain_complete && s.pending > 0
  assert owner.stats(o).2 == None
  let assert Ok(object) = f.store.get(scope() <> "head.json")
  let assert Ok(head) = format.decode_head(object.body)
  assert !head.closed && head.archived.offset == 0
  assert string.contains(registry.metrics(r), "highway_drain_timeout_total 1\n")
  let assert Ok(_) = owner.stop(o)
  registry.stop(r)
  fake.stop(f)
}

pub fn readiness_probes_empty_service_and_expires_during_blocked_io_test() {
  let f = fake.new()
  let reads = process.new_subject()
  let store =
    storage.Store(f.store.get, fn(key, body, condition) {
      case string.contains(key, "/health/") {
        True -> {
          let release = process.new_subject()
          process.send(reads, release)
          let assert Ok(_) = process.receive(release, 2000)
          f.store.put(key, body, condition)
        }
        False -> f.store.put(key, body, condition)
      }
    })
  let limits = config.Limits(..config.defaults(), max_connections: 1)
  let assert Ok(s) =
    registry.start_operational(
      registry.registration(),
      limits,
      Some(store),
      "test",
      config.Operational(500, 100, 200),
    )
  let r = s.data
  let assert Ok(release) = process.receive(reads, 1000)
  assert !registry.ready(r)
  process.send(release, Nil)
  wait_ready(r, 200)
  let assert Ok(snapshot) = registry.inspect(r)
  assert snapshot.owners == 0
  operations.activity(registry.context(r), operations.Connection, True)
  assert !registry.ready(r)
  operations.activity(registry.context(r), operations.Connection, False)
  assert registry.ready(r)
  let assert Ok(release) = process.receive(reads, 1000)
  process.sleep(210)
  assert !registry.ready(r)
  process.send(release, Nil)
  wait_ready(r, 200)
  let assert Ok(_) = registry.begin_drain(r)
  assert !registry.ready(r)
  registry.stop(r)
  fake.stop(f)
}

pub fn closing_metadata_is_versioned_cas_fenced_and_v1_readable_test() {
  let f = fake.new()
  let assert Ok(base) = archive.open(f.store, scope(), True)
  let legacy =
    string.replace(format.head(base.head), "\"version\":2", "\"version\":1")
  let assert Ok(old) = format.decode_head(legacy)
  assert !old.closed
  let plan = archive.closure(base)
  let assert Ok(closed) = archive.close(f.store, plan)
  assert closed.head.closed
  assert archive.close(f.store, plan) == Ok(closed)
  let invalid =
    string.replace(format.head(closed.head), "\"version\":2", "\"version\":1")
  let assert Error(_) = format.decode_head(invalid)
  let assert Ok(next) = archive.open(f.store, scope(), False)
  assert !next.head.closed && next.head.epoch == 2
  assert archive.close(f.store, plan) == Error(storage.Conflict)
  fake.stop(f)
}

pub fn operational_phase_cannot_regress_and_dead_workers_are_reaped_test() {
  let c = operations.new()
  operations.probe(c, True, -1000)
  assert operations.storage_ready(c, -999, 100)
  assert !operations.storage_ready(c, -900, 100)
  operations.finish_drain(c)
  operations.begin_drain(c)
  assert operations.closing(c)
  let ready = process.new_subject()
  let p =
    process.spawn_unlinked(fn() {
      operations.activity(c, operations.Replay, True)
      process.send(ready, Nil)
      process.sleep_forever()
    })
  let assert Ok(_) = process.receive(ready, 1000)
  assert operations.active(c, operations.Replay) == 1
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(process.monitor(p), fn(_) { Nil })
  process.kill(p)
  let assert Ok(_) = process.selector_receive(selector, 1000)
  assert operations.active(c, operations.Replay) == 0
}

@external(erlang, "highway_test_crypto_ffi", "redacted_report")
fn redacted_report(secret: String) -> String

pub fn runtime_logs_never_serialize_session_or_report_payloads_test() {
  let text = redacted_report("private-token-and-message")
  assert text == "{\"event\":\"runtime_report\",\"level\":\"error\"}\n"
}

import envoy
import highway/secrets
import simplifile

pub fn mounted_secrets_conflicts_and_configuration_errors_are_redacted_test() {
  let name = "HIGHWAY_TEST_SECRET_" <> runtime.incarnation()
  let path = "/tmp/highway-secret-" <> runtime.incarnation()
  let assert Ok(_) = simplifile.write(path, "private-fixture-value\n")
  envoy.set(name <> "_FILE", path)
  assert secrets.required(name) == Ok("private-fixture-value")
  envoy.set(name, "other-private-value")
  let assert Error(message) = secrets.required(name)
  assert !string.contains(message, "private-value")
  envoy.unset(name)
  let assert Ok(_) = simplifile.delete_file(path)
  assert secrets.required(name) == Error("cannot read " <> name <> "_FILE")
  envoy.unset(name <> "_FILE")
  assert secrets.optional(name) == Ok(None)
  let assert Error(_) =
    config.validate_operational(config.Operational(0, 100, 200))
  let assert Error(_) =
    config.validate_operational(config.Operational(100, 200, 200))
}

@external(erlang, "highway_test_crypto_ffi", "suspend")
fn suspend(pid: process.Pid) -> Nil

@external(erlang, "highway_test_crypto_ffi", "resume")
fn resume(pid: process.Pid) -> Nil

pub fn drain_retries_saturated_owner_barriers_without_skipping_pending_work_test() {
  let assert Ok(s) = registry.start(registry.registration(), config.defaults())
  let r = s.data
  let assert Ok(seed) =
    registry.publish(
      r,
      domain.Tenant("tenant"),
      domain.Stream("events"),
      "seed",
      json.null(),
    )
  let o = wait_owner(r, 200)
  let replies = process.new_subject()
  suspend(owner.pid(o))
  list.each(int.range(130, 0, [], list.prepend), fn(_) {
    process.spawn(fn() { process.send(replies, owner.read(o, seed.cursor)) })
  })
  // Exactly 128 permits remain held while suspended; the other two cannot
  // enqueue. The drain barrier initially fails admission and must retry.
  let assert Ok(Error(domain.RateLimited)) = process.receive(replies, 1000)
  let assert Ok(Error(domain.RateLimited)) = process.receive(replies, 1000)
  let outcomes = process.new_subject()
  process.spawn(fn() { process.send(outcomes, lifecycle.drain(r, 1000)) })
  process.sleep(20)
  assert registry.draining(r)
  resume(owner.pid(o))
  list.each(int.range(128, 0, [], list.prepend), fn(_) {
    let assert Ok(Ok(None)) = process.receive(replies, 1000)
  })
  let assert Ok(lifecycle.Drained) = process.receive(outcomes, 1000)
  registry.stop(r)
}

import gleam/int
import gleam/list

import highway/health

pub fn idle_probe_checks_write_permissions_and_exact_ambiguous_readback_test() {
  let f = fake.new()
  let key = "test/health/probe.json"
  assert health.probe(f.store, key) == Ok(Nil)
  let readonly =
    storage.Store(f.store.get, fn(_, _, _) { Error(storage.Http(403)) })
  assert health.probe(readonly, key) == Error(storage.Http(403))
  fake.fail(f, key, fake.After(storage.Ambiguous("lost probe response")))
  assert health.probe(f.store, key) == Ok(Nil)
  let assert Ok(object) = f.store.get(key)
  let assert Ok(_) =
    f.store.put(key, "{\"version\":99}", storage.Match(object.etag))
  assert health.probe(f.store, key) == Error(storage.Corrupt("probe format"))
  fake.stop(f)
}
