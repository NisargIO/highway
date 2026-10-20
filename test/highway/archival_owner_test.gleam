import gleam/dict
import gleam/erlang/process
import gleam/json
import gleam/option.{None, Some}
import gleam/string
import highway/auth
import highway/config
import highway/domain
import highway/owner
import highway/protocol
import highway/registry
import highway/runtime
import highway/session
import highway/storage
import highway/storage/archive
import highway/storage/format
import highway/storage/s3
import highway/storage/worker
import highway/storage_fake as fake
import highway/test_support as support

fn wait_ready(o: owner.Owner, attempts: Int) -> domain.Cursor {
  case owner.watermark(o), attempts {
    Ok(c), _ -> c
    _, 0 -> panic as "owner did not initialize"
    _, _ -> {
      process.sleep(5)
      wait_ready(o, attempts - 1)
    }
  }
}

fn wait_progress(o: owner.Owner, offset: Int, attempts: Int) -> domain.Cursor {
  case owner.stats(o).2, attempts {
    Some(c), _ if c.offset >= offset -> c
    _, 0 -> panic as "head progress not confirmed"
    _, _ -> {
      process.sleep(5)
      wait_progress(o, offset, attempts - 1)
    }
  }
}

pub fn blocked_head_never_stalls_live_or_releases_capacity_early_test() {
  let fake = fake.new()
  let blocked = process.new_subject()
  let store =
    storage.Store(fake.store.get, fn(key, body, condition) {
      let archival = case format.decode_head(body) {
        Ok(h) -> h.archived.offset > 0
        Error(_) -> False
      }
      case string.ends_with(key, "head.json") && archival {
        True -> {
          let release = process.new_subject()
          process.send(blocked, release)
          let assert Ok(_) = process.receive(release, 2000)
          fake.store.put(key, body, condition)
        }
        False -> fake.store.put(key, body, condition)
      }
    })
  let limits = config.Limits(..config.defaults(), max_records: 2)
  let scope = archive.scope("test", domain.Tenant("t"), domain.Stream("s"))
  let assert Ok(started) =
    owner.start_storage(limits, Some(store), scope, True, runtime.slots())
  let o = started.data
  let _ = wait_ready(o, 200)
  let assert Ok(first) =
    owner.append(o, "live1", json.null(), runtime.monotonic_ms())
  let assert Ok(release) = process.receive(blocked, 2000)
  assert owner.stats(o).2 == None
  // An old writer completion cannot advance or fence this active batch.
  process.send(
    owner_replies(started.pid),
    worker.Committed(999, "stale-batch", Error(storage.Conflict)),
  )
  let assert Ok(_) = owner.watermark(o)
  assert owner.stats(o).2 == None
  let #(model_bytes, _, _) = owner.stats(o)
  assert model_bytes == first.bytes
  let assert Ok(second) =
    owner.append(o, "live2", json.null(), runtime.monotonic_ms())
  let assert Ok(Some(delivered)) = owner.read(o, first.cursor)
  assert delivered.cursor == second.cursor
  assert owner.append(o, "reject", json.null(), runtime.monotonic_ms())
    == Error(domain.StorageBacklog)
  assert owner.stats(o).0 == first.bytes + second.bytes
  process.send(release, Nil)
  assert wait_progress(o, 1, 200) == first.cursor
  assert owner.stats(o).0 == second.bytes
  // Already committed event remains in bounded live cache.
  let boundary = domain.Cursor(first.cursor.incarnation, first.cursor.epoch, 0)
  let assert Ok(Some(cached)) = owner.read(o, boundary)
  assert cached.cursor == first.cursor
  let assert Ok(_) = owner.stop(o)
  fake.stop(fake)
}

pub fn registry_releases_node_budget_only_after_confirmed_head_test() {
  let fake = fake.new()
  let limits =
    config.Limits(..config.defaults(), node_bytes: 600, stream_bytes: 600)
  let registration = registry.registration()
  let assert Ok(started) =
    registry.start_storage(registration, limits, Some(fake.store), "test")
  let reg = started.data
  let t = domain.Tenant("t")
  let s = domain.Stream("s")
  let assert Ok(_) = archive.open(fake.store, archive.scope("test", t, s), True)
  let o = wait_registry(reg, t, s, 200)
  let assert Ok(first) = registry.publish(reg, t, s, "first", json.null())
  let assert Ok(_) = registry.publish(reg, t, s, "second", json.null())
  assert registry.publish(reg, t, s, "rejected", json.null())
    == Error(domain.StorageBacklog)
  let _ = wait_progress(o, first.cursor.offset + 1, 200)
  let assert Ok(third) =
    registry.publish(reg, t, s, "after-commit", json.null())
  assert third.cursor.offset == 3
  registry.stop(started.data)
  fake.stop(fake)
}

fn wait_registry(
  reg: registry.Registry,
  tenant: domain.Tenant,
  stream: domain.Stream,
  attempts: Int,
) -> owner.Owner {
  case registry.find(reg, tenant, stream), attempts {
    Ok(#(o, _)), _ -> o
    _, 0 -> panic as "registry owner did not initialize"
    _, _ -> {
      process.sleep(5)
      wait_registry(reg, tenant, stream, attempts - 1)
    }
  }
}

pub fn persisted_notices_follow_head_progress_and_refresh_capabilities_test() {
  let fake = fake.new()
  let key = support.key()
  let cfg = support.config(key)
  let cfg =
    config.Config(
      ..cfg,
      storage: config.S3(s3.Settings(
        "http://127.0.0.1:1",
        "test",
        "test",
        "us-east-1",
        "test",
        "test",
        "",
        1000,
      )),
    )
  let registration = registry.registration()
  let assert Ok(started) =
    registry.start_storage(registration, cfg.limits, Some(fake.store), "test")
  let t = domain.Tenant("tenant")
  let stream = domain.Stream("events")
  let assert Ok(_) =
    archive.open(fake.store, archive.scope("test", t, stream), True)
  let o = wait_registry(started.data, t, stream, 200)
  let assert Ok(claims) =
    auth.verify(cfg.auth, support.token(key, ["publish"]), 1000)
  let initial = session.State(..session.new(0), claims: Some(claims))
  let accepted =
    session.handle(
      initial,
      protocol.Publish("p", stream, "m", json.null()),
      cfg,
      started.data,
      1000,
      0,
    )
  let assert [protocol.Accepted(_, _, event)] = accepted.responses
  assert session.tick(accepted.state, cfg, 1000, 0).responses == []
  let _ = wait_progress(o, event.cursor.offset, 200)
  let progress = session.tick(accepted.state, cfg, 1000, 1)
  assert progress.responses == [protocol.Persisted(stream, event.cursor)]
  assert session.tick(progress.state, cfg, 1000, 2).responses == []
  let refreshed =
    session.handle(
      accepted.state,
      protocol.Auth("r", support.token(key, [])),
      cfg,
      started.data,
      1000,
      2,
    )
  assert session.tick(refreshed.state, cfg, 1000, 3).responses == []
  registry.stop(started.data)
  fake.stop(fake)
}

@external(erlang, "highway_test_crypto_ffi", "owner_replies")
fn owner_replies(pid: process.Pid) -> process.Subject(worker.Reply)

pub fn missing_s3_subscription_does_not_create_storage_or_poison_readiness_test() {
  let fake = fake.new()
  let registration = registry.registration()
  let assert Ok(started) =
    registry.start_storage(
      registration,
      config.defaults(),
      Some(fake.store),
      "test",
    )
  let reg = started.data
  let tenant = domain.Tenant("tenant")
  let stream = domain.Stream("events")
  // Readiness now probes storage independently of stream ownership.
  let assert Ok(_) =
    archive.open(fake.store, archive.scope("test", tenant, stream), True)
  let _ = wait_registry(reg, tenant, stream, 200)
  assert registry.ready(reg)
  let unknown = domain.Stream("missing")
  case registry.find(reg, tenant, unknown) {
    Error(domain.Unavailable) -> wait_missing(reg, tenant, unknown, 200)
    Error(domain.StreamNotFound) -> Nil
    _ -> panic as "unexpected missing stream result"
  }
  assert registry.ready(reg)
  assert fake.store.get(archive.scope("test", tenant, unknown) <> "head.json")
    == Error(storage.NotFound)
  registry.stop(reg)
  fake.stop(fake)
}

fn wait_missing(
  reg: registry.Registry,
  tenant: domain.Tenant,
  stream: domain.Stream,
  attempts: Int,
) -> Nil {
  case registry.find(reg, tenant, stream), attempts {
    Error(domain.StreamNotFound), _ -> Nil
    _, 0 -> panic as "missing stream lookup did not complete"
    _, _ -> {
      process.sleep(5)
      wait_missing(reg, tenant, stream, attempts - 1)
    }
  }
}

pub fn timed_out_queued_append_keeps_node_capacity_reserved_test() {
  let limits =
    config.Limits(..config.defaults(), node_bytes: 600, stream_bytes: 600)
  let registration = registry.registration()
  let assert Ok(started) = registry.start(registration, limits)
  let reg = started.data
  let t = domain.Tenant("tenant")
  let s = domain.Stream("events")
  let assert Ok(first) = registry.publish(reg, t, s, "m", json.null())
  let assert Ok(#(o, _)) = registry.find(reg, t, s)
  suspend(owner.pid(o))
  assert registry.publish(reg, t, s, "m", json.null())
    == Error(domain.Unavailable)
  assert owner.stats(o).0 == first.bytes * 2
  assert registry.publish(reg, t, s, "m", json.null())
    == Error(domain.StorageBacklog)
  resume(owner.pid(o))
  let assert Ok(c) = owner.watermark(o)
  assert c.offset == 2
  assert owner.stats(o).0 == first.bytes * 2
  registry.stop(reg)
}

@external(erlang, "highway_test_crypto_ffi", "suspend")
fn suspend(pid: process.Pid) -> Nil

@external(erlang, "highway_test_crypto_ffi", "resume")
fn resume(pid: process.Pid) -> Nil

pub fn scoped_publisher_expiry_cannot_receive_committed_progress_test() {
  let f = fake.new()
  let key = support.key()
  let cfg = support.config(key)
  let cfg =
    config.Config(
      ..cfg,
      storage: config.S3(s3.Settings(
        "http://127.0.0.1:1",
        "test",
        "test",
        "us-east-1",
        "test",
        "test",
        "",
        1000,
      )),
    )
  let assert Ok(started) =
    registry.start_storage(
      registry.registration(),
      cfg.limits,
      Some(f.store),
      "test",
    )
  let tenant = domain.Tenant("tenant")
  let stream = domain.Stream("events")
  let assert Ok(_) =
    archive.open(f.store, archive.scope("test", tenant, stream), True)
  let o = wait_registry(started.data, tenant, stream, 200)
  let short = support.token_for(key, "tenant", "user", 900, 1001, ["publish"])
  let accepted =
    session.handle(
      session.new(0),
      protocol.ScopedPublish("p", stream, "m", json.null(), short),
      cfg,
      started.data,
      1000,
      0,
    )
  let assert [protocol.ScopedAccepted(_, _, event, _, _, _)] =
    accepted.responses
  let _ = wait_progress(o, event.cursor.offset, 200)
  let expired = session.tick(accepted.state, cfg, 1001, 1)
  assert expired.responses == []
  assert expired.close == None
  assert dict.size(expired.state.watches) == 0
  let renewed =
    session.handle(
      expired.state,
      protocol.ScopedPublish(
        "q",
        stream,
        "next",
        json.null(),
        support.token(key, ["publish"]),
      ),
      cfg,
      started.data,
      1001,
      2,
    )
  let assert [protocol.ScopedAccepted(_, _, event, _, _, _)] = renewed.responses
  let _ = wait_progress(o, event.cursor.offset, 200)
  assert session.tick(renewed.state, cfg, 1001, 3).responses
    == [protocol.Persisted(stream, event.cursor)]
  registry.stop(started.data)
  fake.stop(f)
}
