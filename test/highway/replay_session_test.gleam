import gleam/dict
import gleam/erlang/process
import gleam/json
import gleam/list
import gleam/option.{None, Some}
import highway/auth
import highway/config
import highway/domain
import highway/owner
import highway/protocol
import highway/registry
import highway/replay
import highway/replay/read
import highway/session
import highway/storage
import highway/storage/archive
import highway/storage/s3
import highway/storage_fake as fake
import highway/test_support as support

fn settings(key: support.Key) -> config.Config {
  let cfg = support.config(key)
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
}

fn initial(key: support.Key, actions: List(String)) -> session.State {
  let assert Ok(claims) =
    auth.verify(support.settings(key), support.token(key, actions), 1000)
  session.State(..session.new(0), claims: Some(claims))
}

fn seed(f: fake.Fake) {
  let scope =
    archive.scope("test", domain.Tenant("tenant"), domain.Stream("events"))
  let assert Ok(base) = archive.open(f.store, scope, True)
  let #(committed, events, plans) =
    list.fold([1, 2], #(base, [], []), fn(acc, n) {
      let event =
        domain.Event(
          domain.Cursor(base.head.incarnation, 1, n),
          "old",
          json.int(n),
          260,
        )
      let assert Ok(plan) = archive.plan(acc.0, [event])
      let assert Ok(next) = archive.commit(f.store, plan)
      #(next, list.append(acc.1, [event]), list.append(acc.2, [plan]))
    })
  let assert Ok(oldest) = list.first(plans)
  #(committed, events, oldest)
}

fn ready(reg: registry.Registry, attempts: Int) -> owner.Owner {
  case
    registry.find(reg, domain.Tenant("tenant"), domain.Stream("events")),
    attempts
  {
    Ok(#(o, _)), _ -> o
    _, 0 -> panic as "owner recovery timed out"
    _, _ -> {
      process.sleep(5)
      ready(reg, attempts - 1)
    }
  }
}

fn committed(o: owner.Owner, c: domain.Cursor, attempts: Int) -> Nil {
  case owner.stats(o).2, attempts {
    Some(through), _ if through == c -> Nil
    _, 0 -> panic as "commit timed out"
    _, _ -> {
      process.sleep(5)
      committed(o, c, attempts - 1)
    }
  }
}

fn start(cfg: config.Config, store: storage.Store) -> registry.Registry {
  let assert Ok(started) =
    registry.start_storage(
      registry.registration(),
      cfg.limits,
      Some(store),
      "test",
    )
  let _ = ready(started.data, 200)
  started.data
}

fn sub(
  s: session.State,
  cfg: config.Config,
  reg: registry.Registry,
  from: protocol.From,
  follow: Bool,
  window: Int,
) -> session.Step {
  session.handle(
    s,
    protocol.Subscribe("s", "s", domain.Stream("events"), from, follow, window),
    cfg,
    reg,
    1000,
    0,
  )
}

fn advance(s: session.State, cfg: config.Config, ms: Int) -> session.Step {
  let next = case
    dict.to_list(s.subscriptions) |> list.find(fn(pair) { pair.1.job != None })
  {
    Ok(_) -> {
      let assert Ok(reply) = process.receive(s.replies, 2000)
      let step = session.replayed(s, reply, cfg, 1000, ms)
      assert step.close == None
      assert step.responses == []
      step.state
    }
    Error(_) -> s
  }
  session.tick(next, cfg, 1000, ms)
}

fn drain(
  s: session.State,
  cfg: config.Config,
  ms: Int,
  turns: Int,
  frames: List(protocol.Response),
) -> #(session.State, List(protocol.Response)) {
  assert turns > 0
  let step = advance(s, cfg, ms)
  assert step.close == None
  let frames = list.append(frames, step.responses)
  case
    list.any(step.responses, fn(f) {
      case f {
        protocol.CaughtUp(_, _) -> True
        _ -> False
      }
    })
  {
    True -> #(step.state, frames)
    False -> drain(step.state, cfg, ms + 1, turns - 1, frames)
  }
}

fn content(frames: List(protocol.Response)) -> List(protocol.Response) {
  list.filter(frames, fn(f) {
    case f {
      protocol.Persisted(_, _) -> False
      _ -> True
    }
  })
}

pub fn interrupted_replay_joins_moving_archive_pending_and_post_h_live_test() {
  let f = fake.new()
  let #(old, events, plan) = seed(f)
  let blocked = process.new_subject()
  let store =
    storage.Store(
      fn(key) {
        case key == plan.index_key {
          True -> {
            let release = process.new_subject()
            process.send(blocked, release)
            let assert Ok(_) = process.receive(release, 2000)
            f.store.get(key)
          }
          False -> f.store.get(key)
        }
      },
      f.store.put,
    )
  let key = support.key()
  let cfg = settings(key)
  let reg = start(cfg, store)
  let o = ready(reg, 200)
  let assert Ok(at_h) =
    registry.publish(
      reg,
      domain.Tenant("tenant"),
      domain.Stream("events"),
      "H",
      json.null(),
    )
  let initial =
    sub(
      initial(key, ["subscribe", "history"]),
      cfg,
      reg,
      protocol.Earliest,
      True,
      10,
    )
  assert initial.responses == [protocol.Subscribed("s", "s", at_h.cursor)]
  let pending = session.tick(initial.state, cfg, 1000, 1)
  let assert Ok(release) = process.receive(blocked, 2000)
  // Commit H, then another batch evicts it from the one-batch cache while the
  // replay worker holds an older root. Nothing after H belongs in caught_up.
  committed(o, at_h.cursor, 200)
  let assert Ok(post) =
    registry.publish(
      reg,
      domain.Tenant("tenant"),
      domain.Stream("events"),
      "post-H",
      json.null(),
    )
  committed(o, post.cursor, 200)
  process.send(release, Nil)
  // Only this traversal hits the oldest key; subsequent seeks stop at after.
  let #(following, frames) = drain(pending.state, cfg, 2, 30, pending.responses)
  assert content(frames)
    == list.append(
      list.map(events, fn(e) {
        protocol.EventMessage("s", domain.Stream("events"), e)
      }),
      [
        protocol.Discontinuity("s", old.head.archived, 2),
        protocol.EventMessage("s", domain.Stream("events"), at_h),
        protocol.CaughtUp("s", at_h.cursor),
        protocol.EventMessage("s", domain.Stream("events"), post),
      ],
    )
  assert dict.size(following.subscriptions) == 1
  session.cleanup(following)
  registry.stop(reg)
  fake.stop(f)
}

pub fn lost_tail_resume_finite_snapshot_and_page_limit_test() {
  let f = fake.new()
  let #(old, _, _) = seed(f)
  let assert Ok(_) = archive.open(f.store, old.scope, False)
  let key = support.key()
  let cfg = settings(key)
  let cfg =
    config.Config(
      ..cfg,
      limits: config.Limits(..cfg.limits, replay_pages_per_step: 1),
    )
  let reg = start(cfg, f.store)
  let assert Ok(pending) =
    registry.publish(
      reg,
      domain.Tenant("tenant"),
      domain.Stream("events"),
      "pending",
      json.null(),
    )
  let lost = domain.Cursor(old.head.incarnation, 1, 500)
  let started =
    sub(
      initial(key, ["subscribe", "history"]),
      cfg,
      reg,
      protocol.After(lost),
      False,
      10,
    )
  let #(done, frames) = drain(started.state, cfg, 1, 30, [])
  assert content(frames)
    == [
      protocol.Discontinuity("s", old.head.archived, 2),
      protocol.Discontinuity("s", old.head.archived, 3),
      protocol.EventMessage("s", domain.Stream("events"), pending),
      protocol.CaughtUp("s", pending.cursor),
      protocol.Complete("s"),
    ]
  assert dict.size(done.subscriptions) == 0
  let limited_cfg =
    config.Config(
      ..cfg,
      limits: config.Limits(..cfg.limits, replay_max_pages: 1),
    )
  // Factory limits are set at registry creation, not per-connection config.
  registry.stop(reg)
  let reg = start(limited_cfg, f.store)
  let begun =
    sub(
      initial(key, ["subscribe", "history"]),
      limited_cfg,
      reg,
      protocol.Earliest,
      True,
      10,
    )
  let waiting = session.tick(begun.state, limited_cfg, 1000, 1)
  let continued = advance(waiting.state, limited_cfg, 2)
  let assert Ok(reply) = process.receive(continued.state.replies, 2000)
  let failed = session.replayed(continued.state, reply, limited_cfg, 1000, 3)
  assert failed.responses == [protocol.Failure("", "s", domain.ReplayLimit)]
  assert dict.size(failed.state.subscriptions) == 0
  registry.stop(reg)
  fake.stop(f)
}

fn observe_down(pid: process.Pid) -> process.Selector(Nil) {
  process.new_selector()
  |> process.select_specific_monitor(process.monitor(pid), fn(_) { Nil })
}

fn blocking(
  store: storage.Store,
  index: String,
  blocked: process.Subject(#(process.Pid, process.Subject(Nil))),
) -> storage.Store {
  storage.Store(
    fn(key) {
      case key == index {
        True -> {
          let release = process.new_subject()
          process.send(blocked, #(process.self(), release))
          let assert Ok(_) = process.receive(release, 5000)
          store.get(key)
        }
        False -> store.get(key)
      }
    },
    store.put,
  )
}

pub fn blocked_replay_capacity_cancel_reuse_refresh_expiry_and_owner_death_test() {
  let f = fake.new()
  let #(_, _, plan) = seed(f)
  let blocked = process.new_subject()
  let key = support.key()
  let cfg = settings(key)
  let cfg =
    config.Config(..cfg, limits: config.Limits(..cfg.limits, replay_workers: 1))
  let reg = start(cfg, blocking(f.store, plan.index_key, blocked))
  let auth_state = initial(key, ["subscribe", "history"])
  let waiting =
    session.tick(
      sub(auth_state, cfg, reg, protocol.Earliest, True, 10).state,
      cfg,
      1000,
      1,
    )
  let assert Ok(#(worker, _)) = process.receive(blocked, 2000)
  let monitor = observe_down(worker)
  let assert Ok(s) = dict.get(waiting.state.subscriptions, "s")
  let assert Some(job) = s.job
  // A second connection remains bounded and retries rather than spawning an
  // unbounded reader. A second subscription on the first cannot launch either.
  let other =
    session.tick(
      sub(
        initial(key, ["subscribe", "history"]),
        cfg,
        reg,
        protocol.Earliest,
        True,
        10,
      ).state,
      cfg,
      1000,
      1,
    )
  let assert Ok(other_sub) = dict.get(other.state.subscriptions, "s")
  assert other_sub.job == None
  assert other_sub.retry_ms == 101
  assert content(other.responses) == []
  let multiplex =
    session.handle(
      waiting.state,
      protocol.Subscribe(
        "two",
        "two",
        domain.Stream("events"),
        protocol.Earliest,
        True,
        10,
      ),
      cfg,
      reg,
      1000,
      2,
    )
  let multiplex = session.tick(multiplex.state, cfg, 1000, 2)
  let jobs =
    dict.fold(multiplex.state.subscriptions, 0, fn(n, _, s) {
      n
      + case s.job {
        Some(_) -> 1
        None -> 0
      }
    })
  assert jobs == 1
  // Cancellation is prompt even when GET never returns.
  let cancelled =
    session.handle(
      waiting.state,
      protocol.Unsubscribe("u", "s"),
      cfg,
      reg,
      1000,
      2,
    )
  let assert Ok(_) = process.selector_receive(monitor, 1000)
  assert !process.is_alive(worker)
  let reused = sub(cancelled.state, cfg, reg, protocol.Latest, True, 10)
  let stale =
    replay.Reply(
      "s",
      replay.generation(job),
      Ok(read.Ready(read.Batch([read.Boundary(s.cursor, 2)], s.cursor))),
    )
  let ignored = session.replayed(reused.state, stale, cfg, 1000, 3)
  assert ignored.state.subscriptions == reused.state.subscriptions
  assert ignored.responses == []
  session.cleanup(other.state)
  session.cleanup(multiplex.state)
  session.cleanup(reused.state)
  // Refresh removes History while keeping Subscribe, so this historical view
  // must stop, and no completed read may resurrect it.
  let waiting =
    session.tick(
      sub(auth_state, cfg, reg, protocol.Earliest, True, 10).state,
      cfg,
      1000,
      4,
    )
  let assert Ok(#(worker, _)) = process.receive(blocked, 2000)
  let monitor = observe_down(worker)
  let refreshed =
    session.handle(
      waiting.state,
      protocol.Auth("r", support.token(key, ["subscribe"])),
      cfg,
      reg,
      1000,
      5,
    )
  assert list.contains(
    refreshed.responses,
    protocol.Failure("", "s", domain.Forbidden),
  )
  let assert Ok(_) = process.selector_receive(monitor, 1000)
  let waiting =
    session.tick(
      sub(auth_state, cfg, reg, protocol.Earliest, True, 10).state,
      cfg,
      1000,
      6,
    )
  let assert Ok(#(worker, _)) = process.receive(blocked, 2000)
  let monitor = observe_down(worker)
  assert session.tick(waiting.state, cfg, 1100, 7).close == Some(1008)
  let assert Ok(_) = process.selector_receive(monitor, 1000)
  let waiting =
    session.tick(
      sub(auth_state, cfg, reg, protocol.Earliest, True, 10).state,
      cfg,
      1000,
      8,
    )
  let assert Ok(#(worker, _)) = process.receive(blocked, 2000)
  let monitor = observe_down(worker)
  let o = ready(reg, 200)
  let owner_monitor = observe_down(owner.pid(o))
  process.kill(owner.pid(o))
  let assert Ok(_) = process.selector_receive(owner_monitor, 1000)
  let assert Ok(_) = process.selector_receive(monitor, 1000)
  let failed = session.tick(waiting.state, cfg, 1000, 9)
  assert failed.close == Some(1011)
  assert content(failed.responses)
    == [protocol.Failure("", "s", domain.Unavailable)]
  registry.stop(reg)
  fake.stop(f)
}

pub fn read_checks_expiry_after_blocked_io_before_returning_payload_test() {
  let f = fake.new()
  let #(_, _, plan) = seed(f)
  let blocked = process.new_subject()
  let key = support.key()
  let cfg = settings(key)
  let reg = start(cfg, blocking(f.store, plan.index_key, blocked))
  let assert Ok(claims) =
    auth.verify(
      cfg.auth,
      support.token_for(key, "tenant", "user", 900, 1001, [
        "subscribe",
        "history",
      ]),
      1000,
    )
  let initial = session.State(..session.new(0), claims: Some(claims))
  let waiting =
    session.tick(
      sub(initial, cfg, reg, protocol.Earliest, True, 10).state,
      cfg,
      1000,
      1,
    )
  let assert Ok(#(_, release)) = process.receive(blocked, 2000)
  process.sleep(1100)
  process.send(release, Nil)
  let assert Ok(reply) = process.receive(waiting.state.replies, 2000)
  assert reply.result == Error(domain.TokenExpired)
  let expired = session.replayed(waiting.state, reply, cfg, 1001, 1101)
  assert expired.responses == [protocol.Failure("", "", domain.TokenExpired)]
  assert expired.close == Some(1008)
  registry.stop(reg)
  fake.stop(f)
}

pub fn latest_grant_continues_via_archive_but_cannot_cross_its_floor_test() {
  let f = fake.new()
  let _ = seed(f)
  let key = support.key()
  let cfg = settings(key)
  let reg = start(cfg, f.store)
  let o = ready(reg, 200)
  let begun =
    sub(initial(key, ["subscribe"]), cfg, reg, protocol.Latest, True, 10)
  let assert Ok(s) = dict.get(begun.state.subscriptions, "s")
  assert owner.pull(o, s.view, domain.Cursor(s.cursor.incarnation, 1, 0))
    == Error(domain.Forbidden)
  let events =
    list.map([1, 2], fn(_) {
      let assert Ok(e) =
        registry.publish(
          reg,
          domain.Tenant("tenant"),
          domain.Stream("events"),
          "live",
          json.null(),
        )
      committed(o, e.cursor, 200)
      e
    })
  let waiting = session.tick(begun.state, cfg, 1000, 1)
  let delivered = advance(waiting.state, cfg, 2)
  assert content(delivered.responses)
    == list.map(events, fn(e) {
      protocol.EventMessage("s", domain.Stream("events"), e)
    })
  assert !list.any(delivered.responses, fn(f) {
    case f {
      protocol.Discontinuity(_, _, _) -> True
      _ -> False
    }
  })
  session.cleanup(delivered.state)
  registry.stop(reg)
  fake.stop(f)
}

pub fn failed_archive_read_and_oversize_replay_terminate_without_caught_up_test() {
  let f = fake.new()
  let #(_, _, plan) = seed(f)
  let key = support.key()
  let cfg = settings(key)
  let reg = start(cfg, f.store)
  fake.fail_read(f, plan.index_key, storage.Http(503))
  let begun =
    sub(
      initial(key, ["subscribe", "history"]),
      cfg,
      reg,
      protocol.Earliest,
      True,
      10,
    )
  let waiting = session.tick(begun.state, cfg, 1000, 1)
  let assert Ok(reply) = process.receive(waiting.state.replies, 2000)
  assert reply.result == Error(domain.Unavailable)
  let failed = session.replayed(waiting.state, reply, cfg, 1000, 2)
  assert failed.close == Some(1011)
  assert failed.responses == [protocol.Failure("", "s", domain.Unavailable)]
  assert dict.size(failed.state.subscriptions) == 0
  let small =
    config.Config(..cfg, limits: config.Limits(..cfg.limits, output_bytes: 32))
  let begun =
    sub(
      initial(key, ["subscribe", "history"]),
      small,
      reg,
      protocol.Earliest,
      True,
      10,
    )
  let waiting = session.tick(begun.state, small, 1000, 1)
  let assert Ok(reply) = process.receive(waiting.state.replies, 2000)
  assert reply.result == Error(domain.SlowConsumer)
  let failed = session.replayed(waiting.state, reply, small, 1000, 2)
  assert failed.close == None
  assert failed.responses == [protocol.Failure("", "s", domain.SlowConsumer)]
  assert dict.size(failed.state.subscriptions) == 0
  registry.stop(reg)
  fake.stop(f)
}

pub fn same_grant_refresh_discards_inflight_generation_and_refetches_test() {
  let f = fake.new()
  let #(_, events, plan) = seed(f)
  let blocked = process.new_subject()
  let key = support.key()
  let cfg = settings(key)
  let reg = start(cfg, blocking(f.store, plan.index_key, blocked))
  let begun =
    sub(
      initial(key, ["subscribe", "history"]),
      cfg,
      reg,
      protocol.Earliest,
      False,
      10,
    )
  let waiting = session.tick(begun.state, cfg, 1000, 1)
  let assert Ok(#(worker, _)) = process.receive(blocked, 2000)
  let monitor = observe_down(worker)
  let assert Ok(s) = dict.get(waiting.state.subscriptions, "s")
  let assert Some(job) = s.job
  let refreshed =
    session.handle(
      waiting.state,
      protocol.Auth("r", support.token(key, ["subscribe", "history"])),
      cfg,
      reg,
      1000,
      2,
    )
  let assert Ok(_) = process.selector_receive(monitor, 1000)
  let assert Ok(after) = dict.get(refreshed.state.subscriptions, "s")
  assert after.cursor == s.cursor
  assert after.job == None && after.held_bytes == 0 && after.buffer == []
  let stale =
    replay.Reply("s", replay.generation(job), Error(domain.Unavailable))
  assert session.replayed(refreshed.state, stale, cfg, 1000, 3).responses == []
  let waiting = session.tick(refreshed.state, cfg, 1000, 3)
  let assert Ok(#(_, release)) = process.receive(blocked, 2000)
  process.send(release, Nil)
  let #(done, frames) = drain(waiting.state, cfg, 4, 30, [])
  assert list.filter_map(frames, fn(f) {
      case f {
        protocol.EventMessage(_, _, e) -> Ok(e.cursor)
        _ -> Error(Nil)
      }
    })
    == list.map(events, fn(e) { e.cursor })
  assert dict.size(done.subscriptions) == 0
  registry.stop(reg)
  fake.stop(f)
}

pub fn dead_connection_cancels_blocked_reader_and_invalidates_lease_test() {
  let f = fake.new()
  let #(_, _, plan) = seed(f)
  let blocked = process.new_subject()
  let key = support.key()
  let cfg = settings(key)
  let reg = start(cfg, blocking(f.store, plan.index_key, blocked))
  let connection = process.spawn_unlinked(process.sleep_forever)
  let o = ready(reg, 200)
  let assert Ok(v) = owner.capture_view(o, protocol.Earliest, connection)
  let assert Ok(launcher) = registry.replayer(reg)
  let assert Some(claims) = initial(key, ["subscribe", "history"]).claims
  let replies = process.new_subject()
  let assert Ok(_) =
    replay.launch(
      launcher,
      replay.Input(
        "s",
        "generation",
        o,
        v,
        claims,
        domain.Stream("events"),
        v.floor,
        v.high,
        None,
        16,
        131_072,
        1000,
        connection,
        replies,
      ),
    )
  let assert Ok(#(worker, _)) = process.receive(blocked, 2000)
  let monitor = observe_down(worker)
  process.kill(connection)
  let assert Ok(_) = process.selector_receive(monitor, 1000)
  assert owner.pull(o, v, v.floor) == Error(domain.Forbidden)
  assert process.receive(replies, 10) == Error(Nil)
  registry.stop(reg)
  fake.stop(f)
}

pub fn negative_monotonic_clock_still_starts_replay_test() {
  let f = fake.new()
  let _ = seed(f)
  let key = support.key()
  let cfg = settings(key)
  let reg = start(cfg, f.store)
  let assert Some(claims) = initial(key, ["subscribe", "history"]).claims
  let s = session.State(..session.new(-10_000), claims: Some(claims))
  let begun =
    session.handle(
      s,
      protocol.Subscribe(
        "s",
        "s",
        domain.Stream("events"),
        protocol.Earliest,
        False,
        10,
      ),
      cfg,
      reg,
      1000,
      -10_000,
    )
  let waiting = session.tick(begun.state, cfg, 1000, -9999)
  let assert Ok(s) = dict.get(waiting.state.subscriptions, "s")
  assert s.job != None
  let assert Ok(reply) = process.receive(waiting.state.replies, 2000)
  let result = session.replayed(waiting.state, reply, cfg, 1000, -9998)
  assert result.responses == []
  assert list.any(session.tick(result.state, cfg, 1000, -9997).responses, fn(f) {
    case f {
      protocol.EventMessage(_, _, _) -> True
      _ -> False
    }
  })
  session.cleanup(result.state)
  registry.stop(reg)
  fake.stop(f)
}

pub fn scoped_expiry_and_refresh_cancel_only_matching_replay_generation_test() {
  let f = fake.new()
  let #(_, _, plan) = seed(f)
  let blocked = process.new_subject()
  let key = support.key()
  let cfg = settings(key)
  let reg = start(cfg, blocking(f.store, plan.index_key, blocked))
  let expiring =
    support.token_for(key, "tenant", "user", 900, 1001, ["subscribe", "history"])
  let lasting = support.token(key, ["subscribe", "history"])
  let a =
    session.handle(
      session.new(0),
      protocol.ScopedSubscribe(
        "a",
        "a",
        domain.Stream("events"),
        protocol.Earliest,
        True,
        10,
        expiring,
      ),
      cfg,
      reg,
      1000,
      0,
    )
  let waiting = session.tick(a.state, cfg, 1000, 1)
  let assert Ok(#(worker, _)) = process.receive(blocked, 2000)
  let monitor = observe_down(worker)
  let assert Ok(s) = dict.get(waiting.state.subscriptions, "a")
  let assert Some(job) = s.job
  let both =
    session.handle(
      waiting.state,
      protocol.ScopedSubscribe(
        "b",
        "b",
        domain.Stream("events"),
        protocol.Latest,
        True,
        10,
        lasting,
      ),
      cfg,
      reg,
      1000,
      1,
    )
  let expired = session.tick(both.state, cfg, 1001, 2)
  let assert Ok(_) = process.selector_receive(monitor, 1000)
  assert !dict.has_key(expired.state.subscriptions, "a")
  assert dict.has_key(expired.state.subscriptions, "b")
  assert expired.close == None
  let stale =
    replay.Reply(
      "a",
      replay.generation(job),
      Ok(read.Ready(read.Batch([read.Boundary(s.cursor, 2)], s.cursor))),
    )
  let ignored = session.replayed(expired.state, stale, cfg, 1001, 3)
  assert ignored.responses == []
  let assert Ok(event) =
    registry.publish(
      reg,
      domain.Tenant("tenant"),
      domain.Stream("events"),
      "still-live",
      json.null(),
    )
  let live = session.tick(ignored.state, cfg, 1001, 4)
  assert list.contains(
    live.responses,
    protocol.EventMessage("b", domain.Stream("events"), event),
  )
  session.cleanup(live.state)
  // A same-grant refresh cancels blocked I/O too. Old queued replies must not
  // return payload or erase the replacement credential before a new scan.
  let a =
    session.handle(
      session.new(0),
      protocol.ScopedSubscribe(
        "a",
        "a",
        domain.Stream("events"),
        protocol.Earliest,
        True,
        10,
        lasting,
      ),
      cfg,
      reg,
      1000,
      5,
    )
  let waiting = session.tick(a.state, cfg, 1000, 6)
  let assert Ok(#(worker, _)) = process.receive(blocked, 2000)
  let monitor = observe_down(worker)
  let assert Ok(s) = dict.get(waiting.state.subscriptions, "a")
  let assert Some(job) = s.job
  let refreshed =
    session.handle(
      waiting.state,
      protocol.SubscriptionAuth("r", "a", lasting),
      cfg,
      reg,
      1000,
      7,
    )
  let assert Ok(_) = process.selector_receive(monitor, 1000)
  let stale =
    replay.Reply(
      "a",
      replay.generation(job),
      Ok(read.Ready(read.Batch([read.Boundary(s.cursor, 2)], s.cursor))),
    )
  let ignored = session.replayed(refreshed.state, stale, cfg, 1000, 8)
  assert ignored.responses == []
  assert ignored.state.subscriptions == refreshed.state.subscriptions
  session.cleanup(ignored.state)
  registry.stop(reg)
  fake.stop(f)
}
