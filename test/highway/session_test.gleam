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
import highway/runtime
import highway/session
import highway/test_support as support

fn state(key: support.Key, actions: List(String)) -> session.State {
  let assert Ok(claims) =
    auth.verify(support.settings(key), support.token(key, actions), 1000)
  session.State(..session.new(0), claims: Some(claims))
}

fn service(limits: config.Limits) {
  let registration = registry.registration()
  let assert Ok(started) = registry.start(registration, limits)
  started
}

fn subscribe(
  s: session.State,
  cfg: config.Config,
  reg: registry.Registry,
  id: String,
  stream: String,
  from: protocol.From,
  window: Int,
) {
  session.handle(
    s,
    protocol.Subscribe(id, id, domain.Stream(stream), from, True, window),
    cfg,
    reg,
    1000,
    0,
  )
}

pub fn authorize_before_existence_and_history_checks_test() {
  let key = support.key()
  let cfg = support.config(key)
  let service = service(cfg.limits)
  let reg = service.data
  let initial = state(key, ["subscribe"])
  let forbidden =
    subscribe(initial, cfg, reg, "a", "secret", protocol.Latest, 1)
  assert forbidden.responses == [protocol.Failure("a", "a", domain.Forbidden)]
  let missing = subscribe(initial, cfg, reg, "a", "events", protocol.Latest, 1)
  assert missing.responses
    == [protocol.Failure("a", "a", domain.StreamNotFound)]
  let history =
    subscribe(initial, cfg, reg, "a", "events", protocol.Earliest, 1)
  assert history.responses == [protocol.Failure("a", "a", domain.Forbidden)]
  let denied =
    session.handle(
      initial,
      protocol.Publish("p", domain.Stream("events"), "m", json.null()),
      cfg,
      reg,
      1000,
      0,
    )
  assert denied.responses == [protocol.Failure("p", "", domain.Forbidden)]
  assert registry.find(reg, domain.Tenant("tenant"), domain.Stream("events"))
    == Error(domain.StreamNotFound)
  let unauth =
    session.handle(
      session.new(0),
      protocol.Publish("p", domain.Stream("events"), "m", json.null()),
      cfg,
      reg,
      1000,
      0,
    )
  assert unauth.responses == [protocol.Failure("p", "", domain.Unauthenticated)]
  registry.stop(service.data)
}

pub fn multiplex_credit_slow_consumer_and_fast_subscriber_test() {
  let key = support.key()
  let cfg = support.config(key)
  let service = service(cfg.limits)
  let reg = service.data
  let tenant = domain.Tenant("tenant")
  let stream = domain.Stream("events")
  let assert Ok(_) = registry.publish(reg, tenant, stream, "seed", json.null())
  let initial = state(key, ["publish", "subscribe"])
  let slow =
    subscribe(initial, cfg, reg, "slow", "events", protocol.Latest, 1).state
  let fast =
    subscribe(initial, cfg, reg, "fast", "events", protocol.Latest, 10).state
  let assert Ok(first) =
    registry.publish(reg, tenant, stream, "first", json.null())
  let assert Ok(second) =
    registry.publish(reg, tenant, stream, "second", json.null())
  let slow = session.tick(slow, cfg, 1000, 1)
  assert slow.responses == [protocol.EventMessage("slow", stream, first)]
  let fast = session.tick(fast, cfg, 1000, 1)
  assert fast.responses
    == [
      protocol.EventMessage("fast", stream, first),
      protocol.EventMessage("fast", stream, second),
    ]
  let paused = session.tick(slow.state, cfg, 1000, 2)
  assert paused.responses == []
  let resumed =
    session.handle(
      slow.state,
      protocol.Ack("slow", first.cursor),
      cfg,
      reg,
      1000,
      2,
    )
  assert session.tick(resumed.state, cfg, 1000, 2).responses
    == [protocol.EventMessage("slow", stream, second)]
  let timed = session.tick(slow.state, cfg, 1000, 5001)
  assert timed.responses == [protocol.Failure("", "slow", domain.SlowConsumer)]
  assert dict.size(timed.state.subscriptions) == 0
  assert timed.close == None
  registry.stop(service.data)
}

pub fn idle_expiration_auth_deadline_and_refresh_test() {
  let key = support.key()
  let cfg = support.config(key)
  let service = service(cfg.limits)
  let reg = service.data
  assert session.tick(session.new(0), cfg, 1000, 4999).close == None
  assert session.tick(session.new(0), cfg, 1000, 5000).close == Some(1008)
  let initial = state(key, ["subscribe", "publish"])
  let expired = session.tick(initial, cfg, 1100, 1)
  assert expired.responses == [protocol.Failure("", "", domain.TokenExpired)]
  assert expired.close == Some(1008)
  let assert Ok(_) =
    registry.publish(
      reg,
      domain.Tenant("tenant"),
      domain.Stream("events"),
      "seed",
      json.null(),
    )
  let subscribed =
    subscribe(initial, cfg, reg, "live", "events", protocol.Latest, 1).state
  let refresh =
    session.handle(
      subscribed,
      protocol.Auth("a", support.token(key, ["publish"])),
      cfg,
      reg,
      1000,
      1,
    )
  assert dict.size(refresh.state.subscriptions) == 0
  assert list.contains(
    refresh.responses,
    protocol.Failure("", "live", domain.Forbidden),
  )
  let invalid =
    session.handle(subscribed, protocol.Auth("a", "bad"), cfg, reg, 1000, 1)
  assert invalid.state.claims == subscribed.claims
  assert dict.size(invalid.state.subscriptions) == 1
  let different =
    session.handle(
      subscribed,
      protocol.Auth(
        "a",
        support.token_for(key, "other", "user", 900, 1100, ["subscribe"]),
      ),
      cfg,
      reg,
      1000,
      1,
    )
  assert different.responses == [protocol.Failure("a", "", domain.Forbidden)]
  assert different.state.claims == subscribed.claims
  registry.stop(service.data)
}

pub fn owner_crash_closes_followers_and_changes_incarnation_test() {
  let key = support.key()
  let cfg = support.config(key)
  let service = service(cfg.limits)
  let reg = service.data
  let tenant = domain.Tenant("tenant")
  let stream = domain.Stream("events")
  let assert Ok(first) =
    registry.publish(reg, tenant, stream, "seed", json.null())
  let subscribed =
    subscribe(
      state(key, ["subscribe", "history"]),
      cfg,
      reg,
      "live",
      "events",
      protocol.Latest,
      1,
    ).state
  let assert Ok(#(old_owner, _)) = registry.find(reg, tenant, stream)
  let monitor = process.monitor(owner.pid(old_owner))
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_) { Nil })
  process.kill(owner.pid(old_owner))
  let assert Ok(_) = process.selector_receive(selector, 1000)
  assert session.tick(subscribed, cfg, 1000, 1).close == Some(1011)
  let assert Ok(next) =
    registry.publish(reg, tenant, stream, "after crash", json.null())
  assert next.cursor.incarnation != first.cursor.incarnation
  assert next.cursor.offset == 1
  let old_cursor =
    subscribe(
      state(key, ["subscribe", "history"]),
      cfg,
      reg,
      "old",
      "events",
      protocol.After(first.cursor),
      1,
    )
  assert old_cursor.responses
    == [protocol.Failure("old", "old", domain.CursorInvalid)]
  registry.stop(service.data)
}

pub fn concurrent_publishes_share_one_owner_and_order_test() {
  let limits = config.defaults()
  let service = service(limits)
  let reg = service.data
  let tenant = domain.Tenant("tenant")
  let stream = domain.Stream("events")
  let results = process.new_subject()
  list.each(int.range(32, 0, [], list.prepend), fn(_) {
    process.spawn(fn() {
      process.send(
        results,
        registry.publish(reg, tenant, stream, "same-id", json.null()),
      )
    })
  })
  let events =
    list.map(int.range(32, 0, [], list.prepend), fn(_) {
      let assert Ok(Ok(event)) = process.receive(results, 2000)
      event
    })
  let offsets =
    list.map(events, fn(e) { e.cursor.offset }) |> list.sort(int.compare)
  assert offsets == int.range(32, 0, [], list.prepend)
  let assert Ok(#(owner, _)) = registry.find(reg, tenant, stream)
  let assert Ok(example) = list.first(events)
  let assert Ok(Some(first)) =
    owner.read(owner, domain.Cursor(example.cursor.incarnation, 1, 0))
  assert first.cursor.offset == 1
  registry.stop(service.data)
}

import gleam/int

pub fn mailbox_admission_and_socket_slot_boundaries_test() {
  let gate = runtime.slots()
  let permits =
    list.map(int.range(128, 0, [], list.prepend), fn(_) {
      let assert Ok(p) = runtime.permit(gate)
      p
    })
  assert runtime.permit(gate) == Error(Nil)
  let assert Ok(first) = list.first(permits)
  runtime.release_permit(gate, first)
  let assert Ok(_) = runtime.permit(gate)
  assert runtime.permit(gate) == Error(Nil)
  let slots = runtime.slots()
  assert runtime.acquire(slots, 1)
  assert !runtime.acquire(slots, 1)
  runtime.release(slots, process.self())
  assert runtime.acquire(slots, 1)
}

@external(erlang, "highway_test_crypto_ffi", "suspend")
fn suspend(pid: process.Pid) -> Nil

@external(erlang, "highway_test_crypto_ffi", "resume")
fn resume(pid: process.Pid) -> Nil

@external(erlang, "highway_test_crypto_ffi", "mailbox_length")
fn mailbox_length(pid: process.Pid) -> Int

pub fn paused_owner_cannot_grow_mailbox_beyond_admission_permits_test() {
  let assert Ok(started) = owner.start(config.defaults())
  let o = started.data
  suspend(started.pid)
  let replies = process.new_subject()
  list.each(int.range(140, 0, [], list.prepend), fn(_) {
    process.spawn(fn() {
      process.send(replies, owner.append(o, "m", json.null(), 0))
    })
  })
  // 128 requests can enter; the remaining 12 must reject before the owner runs.
  list.each(int.range(12, 0, [], list.prepend), fn(_) {
    let assert Ok(Error(domain.RateLimited)) = process.receive(replies, 500)
  })
  assert mailbox_length(started.pid) <= 128
  resume(started.pid)
  list.each(int.range(128, 0, [], list.prepend), fn(_) {
    let assert Ok(Ok(_)) = process.receive(replies, 2000)
  })
  let assert Ok(c) = owner.watermark(o)
  assert c.offset == 128
  let assert Ok(_) = owner.stop(started.data)
}

pub fn node_and_active_stream_limits_reject_before_creation_test() {
  let limits =
    config.Limits(
      ..config.defaults(),
      max_streams: 1,
      node_bytes: 600,
      stream_bytes: 600,
    )
  let service = service(limits)
  let reg = service.data
  let t = domain.Tenant("tenant")
  let a = domain.Stream("a")
  let b = domain.Stream("b")
  let assert Ok(_) = registry.publish(reg, t, a, "m", json.null())
  assert registry.publish(reg, t, b, "m", json.null())
    == Error(domain.RateLimited)
  let assert Ok(second) = registry.publish(reg, t, a, "m", json.null())
  assert second.cursor.offset == 2
  assert registry.publish(reg, t, b, "m", json.null())
    == Error(domain.StorageBacklog)
  assert registry.find(reg, t, b) == Error(domain.StreamNotFound)
  registry.stop(service.data)
}

pub fn output_bytes_cancellation_and_subscription_id_reuse_test() {
  let key = support.key()
  let cfg = support.config(key)
  let cfg =
    config.Config(..cfg, limits: config.Limits(..cfg.limits, output_bytes: 32))
  let service = service(cfg.limits)
  let reg = service.data
  let t = domain.Tenant("tenant")
  let stream = domain.Stream("events")
  let assert Ok(_) = registry.publish(reg, t, stream, "m", json.null())
  let initial = state(key, ["subscribe"])
  let sub =
    subscribe(initial, cfg, reg, "s", "events", protocol.Latest, 1).state
  assert subscribe(sub, cfg, reg, "s", "events", protocol.Latest, 1).responses
    == [protocol.Failure("s", "s", domain.InvalidFrame)]
  let assert Ok(_) = registry.publish(reg, t, stream, "m", json.null())
  let limited = session.tick(sub, cfg, 1000, 1)
  assert limited.responses == [protocol.Failure("", "s", domain.SlowConsumer)]
  assert dict.size(limited.state.subscriptions) == 0
  let sub =
    subscribe(initial, cfg, reg, "s", "events", protocol.Latest, 1).state
  let cancelled =
    session.handle(sub, protocol.Unsubscribe("u", "s"), cfg, reg, 1000, 1)
  assert cancelled.responses == [protocol.Unsubscribed("u", "s")]
  assert dict.size(cancelled.state.subscriptions) == 0
  assert dict.size(
      subscribe(cancelled.state, cfg, reg, "s", "events", protocol.Latest, 1).state.subscriptions,
    )
    == 1
  registry.stop(service.data)
}

pub fn memory_replay_snapshot_exclusive_resume_and_live_join_test() {
  let key = support.key()
  let cfg = support.config(key)
  let service = service(cfg.limits)
  let reg = service.data
  let t = domain.Tenant("tenant")
  let stream = domain.Stream("events")
  let events =
    list.map([1, 2, 3], fn(n) {
      let assert Ok(e) =
        registry.publish(reg, t, stream, int.to_string(n), json.int(n))
      e
    })
  let assert Ok(last) = list.last(events)
  let initial = state(key, ["subscribe", "history"])
  let finite =
    session.handle(
      initial,
      protocol.Subscribe(
        "finite",
        "finite",
        stream,
        protocol.Earliest,
        False,
        10,
      ),
      cfg,
      reg,
      1000,
      0,
    )
  assert finite.responses
    == [protocol.Subscribed("finite", "finite", last.cursor)]
  let following =
    subscribe(initial, cfg, reg, "follow", "events", protocol.Earliest, 1)
  let assert Ok(post) = registry.publish(reg, t, stream, "post-H", json.null())
  let snapshot = session.tick(finite.state, cfg, 1000, 1)
  assert snapshot.responses
    == list.append(
      list.map(events, fn(e) { protocol.EventMessage("finite", stream, e) }),
      [protocol.CaughtUp("finite", last.cursor), protocol.Complete("finite")],
    )
  assert dict.size(snapshot.state.subscriptions) == 0
  let assert Ok(#(o, _)) = registry.find(reg, t, stream)
  let assert Ok(old) = dict.get(finite.state.subscriptions, "finite")
  assert owner.pull(o, old.view, old.cursor) == Error(domain.Forbidden)
  let #(follow, frames) =
    list.fold(events, #(following.state, []), fn(acc, event) {
      let step = session.tick(acc.0, cfg, 1000, 1)
      let ack =
        session.handle(
          step.state,
          protocol.Ack("follow", event.cursor),
          cfg,
          reg,
          1000,
          2,
        )
      assert ack.responses == []
      #(ack.state, list.append(acc.1, step.responses))
    })
  assert frames
    == list.append(
      list.map(events, fn(e) { protocol.EventMessage("follow", stream, e) }),
      [protocol.CaughtUp("follow", last.cursor)],
    )
  assert session.tick(follow, cfg, 1000, 3).responses
    == [protocol.EventMessage("follow", stream, post)]
  let resumed =
    subscribe(
      initial,
      cfg,
      reg,
      "resume",
      "events",
      protocol.After(last.cursor),
      10,
    )
  assert session.tick(resumed.state, cfg, 1000, 3).responses
    == [
      protocol.EventMessage("resume", stream, post),
      protocol.CaughtUp("resume", post.cursor),
    ]
  registry.stop(reg)
}

pub fn history_removal_cancels_replay_and_releases_view_test() {
  let key = support.key()
  let cfg = support.config(key)
  let service = service(cfg.limits)
  let reg = service.data
  let assert Ok(_) =
    registry.publish(
      reg,
      domain.Tenant("tenant"),
      domain.Stream("events"),
      "m",
      json.null(),
    )
  let sub =
    subscribe(
      state(key, ["subscribe", "history"]),
      cfg,
      reg,
      "s",
      "events",
      protocol.Earliest,
      1,
    )
  let assert Ok(s) = dict.get(sub.state.subscriptions, "s")
  let refresh =
    session.handle(
      sub.state,
      protocol.Auth("r", support.token(key, ["subscribe"])),
      cfg,
      reg,
      1000,
      1,
    )
  assert list.contains(
    refresh.responses,
    protocol.Failure("", "s", domain.Forbidden),
  )
  assert dict.size(refresh.state.subscriptions) == 0
  assert owner.pull(s.owner, s.view, s.cursor) == Error(domain.Forbidden)
  registry.stop(reg)
}

pub fn replay_deadline_includes_credit_pauses_without_false_completion_test() {
  let key = support.key()
  let cfg = support.config(key)
  let cfg =
    config.Config(
      ..cfg,
      limits: config.Limits(..cfg.limits, replay_timeout_ms: 5),
    )
  let service = service(cfg.limits)
  let reg = service.data
  list.each([1, 2], fn(_) {
    let assert Ok(_) =
      registry.publish(
        reg,
        domain.Tenant("tenant"),
        domain.Stream("events"),
        "m",
        json.null(),
      )
  })
  let begun =
    subscribe(
      state(key, ["subscribe", "history"]),
      cfg,
      reg,
      "s",
      "events",
      protocol.Earliest,
      1,
    )
  let one = session.tick(begun.state, cfg, 1000, 1)
  let assert [protocol.EventMessage(_, _, _)] = one.responses
  assert session.tick(one.state, cfg, 1000, 2).responses == []
  let exhausted = session.tick(one.state, cfg, 1000, 5)
  assert exhausted.responses == [protocol.Failure("", "s", domain.ReplayLimit)]
  assert dict.size(exhausted.state.subscriptions) == 0
  assert exhausted.close == None
  registry.stop(reg)
}

pub fn finite_completion_retains_bounded_validated_late_ack_receipts_test() {
  let key = support.key()
  let cfg = support.config(key)
  let service = service(cfg.limits)
  let reg = service.data
  let stream = domain.Stream("events")
  let assert Ok(_) =
    registry.publish(reg, domain.Tenant("tenant"), stream, "seed", json.null())
  let initial = state(key, ["subscribe", "history"])
  let replay = fn(s, n) {
    let id = int.to_string(n)
    let begun =
      session.handle(
        s,
        protocol.Subscribe(id, id, stream, protocol.Earliest, False, 128),
        cfg,
        reg,
        1000,
        0,
      )
    let delivered = session.tick(begun.state, cfg, 1000, 1)
    let delivered = case dict.size(delivered.state.subscriptions) > 0 {
      True -> session.tick(delivered.state, cfg, 1000, 2)
      False -> delivered
    }
    assert list.contains(delivered.responses, protocol.Complete(id))
    delivered.state
  }
  let done = list.fold(int.range(70, 0, [], list.prepend), initial, replay)
  assert dict.size(done.subscriptions) == 0
  assert list.length(done.completed) == 64
  // Bound aggregate cursor metadata as well as completed subscription count.
  let events =
    list.map(int.range(16, 0, [], list.prepend), fn(_) {
      let assert Ok(e) =
        registry.publish(reg, domain.Tenant("tenant"), stream, "m", json.null())
      e
    })
  let assert Ok(event) = list.last(events)
  let done = list.fold(int.range(140, 70, [], list.prepend), done, replay)
  assert list.length(done.completed) == 60
  assert list.fold(done.completed, 0, fn(n, pair) {
      n + list.length(pair.1.credit.pending)
    })
    <= 1024
  let acked =
    session.handle(done, protocol.Ack("140", event.cursor), cfg, reg, 1000, 2)
  assert acked.responses == []
  assert session.handle(
      acked.state,
      protocol.Ack("140", event.cursor),
      cfg,
      reg,
      1000,
      3,
    ).responses
    == []
  let future = domain.Cursor(..event.cursor, offset: event.cursor.offset + 1)
  assert session.handle(done, protocol.Ack("140", future), cfg, reg, 1000, 2).responses
    == [protocol.Failure("", "140", domain.CursorInvalid)]
  assert session.handle(
      done,
      protocol.Ack("1", event.cursor),
      cfg,
      reg,
      1000,
      2,
    ).responses
    == [protocol.Failure("", "1", domain.InvalidFrame)]
  let refreshed =
    session.handle(
      done,
      protocol.Auth("r", support.token(key, ["publish"])),
      cfg,
      reg,
      1000,
      2,
    )
  assert refreshed.state.completed == []
  registry.stop(reg)
}
