import gleam/dict
import gleam/json
import gleam/list
import gleam/option.{None, Some}
import highway/config
import highway/domain
import highway/protocol
import highway/registry
import highway/session
import highway/test_support as support

pub fn token(
  key: support.Key,
  stream: String,
  tenant: String,
  subject: String,
  expires: Int,
  actions: List(String),
) -> String {
  support.sign(
    key,
    "{\"alg\":\"EdDSA\",\"kid\":\"test\"}",
    json.to_string(
      json.object([
        #("iss", json.string("issuer")),
        #("aud", json.string("highway")),
        #("sub", json.string(subject)),
        #("tenant", json.string(tenant)),
        #("iat", json.int(900)),
        #("exp", json.int(expires)),
        #(
          "grants",
          json.array(
            [
              json.object([
                #("stream", json.string(stream)),
                #("actions", json.array(actions, json.string)),
              ]),
            ],
            fn(x) { x },
          ),
        ),
      ]),
    ),
  )
}

fn setup(cfg: config.Config) {
  let assert Ok(service) = registry.start(registry.registration(), cfg.limits)
  list.each(["events", "other"], fn(stream) {
    let assert Ok(_) =
      registry.publish(
        service.data,
        domain.Tenant("tenant"),
        domain.Stream(stream),
        "seed",
        json.null(),
      )
  })
  service.data
}

fn subscribe(
  s: session.State,
  cfg: config.Config,
  reg: registry.Registry,
  id: String,
  stream: String,
  token: String,
) {
  session.handle(
    s,
    protocol.ScopedSubscribe(
      id,
      id,
      domain.Stream(stream),
      protocol.Earliest,
      True,
      10,
      token,
    ),
    cfg,
    reg,
    1000,
    0,
  )
}

pub fn independently_scoped_expiry_and_no_cross_stream_fallback_test() {
  let key = support.key()
  let cfg = support.config(key)
  let reg = setup(cfg)
  let a = token(key, "events", "tenant", "user", 1002, ["subscribe", "history"])
  let b = token(key, "other", "tenant", "user", 1100, ["subscribe", "history"])
  let first = subscribe(session.new(0), cfg, reg, "a", "events", a)
  let both = subscribe(first.state, cfg, reg, "b", "other", b)
  assert dict.size(both.state.subscriptions) == 2
  assert both.state.claims == None
  let forbidden = subscribe(both.state, cfg, reg, "sneak", "other", a)
  assert forbidden.responses
    == [protocol.Failure("sneak", "sneak", domain.Forbidden)]
  let initial = session.tick(both.state, cfg, 1000, 1)
  let assert Ok(event) =
    registry.publish(
      reg,
      domain.Tenant("tenant"),
      domain.Stream("other"),
      "later",
      json.null(),
    )
  let expired = session.tick(initial.state, cfg, 1002, 2)
  assert expired.close == None
  assert !dict.has_key(expired.state.subscriptions, "a")
  assert dict.has_key(expired.state.subscriptions, "b")
  assert list.contains(
    expired.responses,
    protocol.Failure("", "a", domain.TokenExpired),
  )
  assert list.contains(
    expired.responses,
    protocol.EventMessage("b", domain.Stream("other"), event),
  )
  let uncredentialed =
    session.handle(
      expired.state,
      protocol.Subscribe(
        "x",
        "x",
        domain.Stream("other"),
        protocol.Latest,
        True,
        1,
      ),
      cfg,
      reg,
      1002,
      3,
    )
  assert uncredentialed.responses
    == [protocol.Failure("x", "", domain.Unauthenticated)]
  session.cleanup(expired.state)
  registry.stop(reg)
}

pub fn scoped_refresh_removes_only_its_view_and_rejects_identity_changes_test() {
  let key = support.key()
  let cfg = support.config(key)
  let reg = setup(cfg)
  let a = token(key, "events", "tenant", "user", 1002, ["subscribe", "history"])
  let b = token(key, "other", "tenant", "user", 1100, ["subscribe", "history"])
  let both =
    subscribe(
      subscribe(session.new(0), cfg, reg, "a", "events", a).state,
      cfg,
      reg,
      "b",
      "other",
      b,
    )
  let invalid =
    session.handle(
      both.state,
      protocol.SubscriptionAuth("bad", "a", "bad-signature"),
      cfg,
      reg,
      1000,
      1,
    )
  assert invalid.responses
    == [protocol.Failure("bad", "a", domain.Unauthenticated)]
  assert invalid.state.subscriptions == both.state.subscriptions
  list.each(
    [#("other-tenant", "user"), #("tenant", "other-user")],
    fn(identity) {
      let bad =
        token(key, "events", identity.0, identity.1, 1100, [
          "subscribe",
          "history",
        ])
      let rejected =
        session.handle(
          both.state,
          protocol.SubscriptionAuth("bad", "a", bad),
          cfg,
          reg,
          1000,
          1,
        )
      assert rejected.responses
        == [protocol.Failure("bad", "a", domain.Forbidden)]
      assert rejected.state.subscriptions == both.state.subscriptions
    },
  )
  let fresh =
    token(key, "events", "tenant", "user", 1050, ["subscribe", "history"])
  let refreshed =
    session.handle(
      both.state,
      protocol.SubscriptionAuth("r", "a", fresh),
      cfg,
      reg,
      1001,
      2,
    )
  assert refreshed.responses
    == [protocol.SubscriptionAuthenticated("r", "a", "user", "tenant", 1050)]
  let alive = session.tick(refreshed.state, cfg, 1002, 3)
  assert dict.size(alive.state.subscriptions) == 2
  let revoke_history =
    token(key, "events", "tenant", "user", 1100, ["subscribe"])
  let revoked =
    session.handle(
      alive.state,
      protocol.SubscriptionAuth("r", "a", revoke_history),
      cfg,
      reg,
      1002,
      4,
    )
  assert revoked.responses == [protocol.Failure("r", "a", domain.Forbidden)]
  assert !dict.has_key(revoked.state.subscriptions, "a")
  assert dict.has_key(revoked.state.subscriptions, "b")
  assert revoked.close == None
  session.cleanup(revoked.state)
  registry.stop(reg)
}

pub fn scoped_publish_requires_its_own_capability_and_pins_identity_test() {
  let key = support.key()
  let cfg = support.config(key)
  let reg = setup(cfg)
  let subscribe_only =
    token(key, "events", "tenant", "user", 1100, ["subscribe", "history"])
  let begun = subscribe(session.new(0), cfg, reg, "a", "events", subscribe_only)
  let publish_token = token(key, "other", "tenant", "user", 1100, ["publish"])
  let denied =
    session.handle(
      begun.state,
      protocol.ScopedPublish(
        "p",
        domain.Stream("events"),
        "m",
        json.null(),
        publish_token,
      ),
      cfg,
      reg,
      1000,
      1,
    )
  assert denied.responses == [protocol.Failure("p", "", domain.Forbidden)]
  let permitted =
    session.handle(
      begun.state,
      protocol.ScopedPublish(
        "p",
        domain.Stream("other"),
        "m",
        json.null(),
        publish_token,
      ),
      cfg,
      reg,
      1000,
      1,
    )
  let assert [
    protocol.ScopedAccepted(
      "p",
      domain.Stream("other"),
      _,
      "user",
      "tenant",
      1100,
    ),
  ] = permitted.responses
  let legacy =
    session.handle(
      permitted.state,
      protocol.Auth("legacy", support.token(key, ["publish"])),
      cfg,
      reg,
      1000,
      2,
    )
  assert legacy.responses == [protocol.Failure("legacy", "", domain.Forbidden)]
  let expired = token(key, "other", "tenant", "user", 1001, ["publish"])
  let denied =
    session.handle(
      permitted.state,
      protocol.ScopedPublish(
        "p",
        domain.Stream("other"),
        "m",
        json.null(),
        expired,
      ),
      cfg,
      reg,
      1001,
      3,
    )
  assert denied.responses == [protocol.Failure("p", "", domain.TokenExpired)]
  session.cleanup(permitted.state)
  registry.stop(reg)
}

pub fn initial_auth_deadline_and_expired_refresh_cannot_resurrect_test() {
  let key = support.key()
  let cfg = support.config(key)
  let reg = setup(cfg)
  assert session.tick(session.new(0), cfg, 1000, cfg.limits.auth_ms).close
    == Some(1008)
  let old =
    token(key, "events", "tenant", "user", 1001, ["subscribe", "history"])
  let initial = subscribe(session.new(0), cfg, reg, "a", "events", old)
  let renewed =
    token(key, "events", "tenant", "user", 1100, ["subscribe", "history"])
  let too_late =
    session.handle(
      initial.state,
      protocol.SubscriptionAuth("r", "a", renewed),
      cfg,
      reg,
      1001,
      1,
    )
  assert too_late.responses == [protocol.Failure("r", "a", domain.TokenExpired)]
  let expired = session.tick(too_late.state, cfg, 1001, 2)
  assert dict.size(expired.state.subscriptions) == 0
  assert expired.close == None
  let cancelled =
    session.handle(
      expired.state,
      protocol.Unsubscribe("u", "a"),
      cfg,
      reg,
      1001,
      3,
    )
  assert cancelled.responses == [protocol.Unsubscribed("u", "a")]
  registry.stop(reg)
}

pub fn completed_ack_receipt_cannot_borrow_a_sibling_token_test() {
  let key = support.key()
  let cfg = support.config(key)
  let reg = setup(cfg)
  let short =
    token(key, "events", "tenant", "user", 1001, ["subscribe", "history"])
  let finite =
    session.handle(
      session.new(0),
      protocol.ScopedSubscribe(
        "a",
        "a",
        domain.Stream("events"),
        protocol.Earliest,
        False,
        1,
        short,
      ),
      cfg,
      reg,
      1000,
      0,
    )
  let finished = session.tick(finite.state, cfg, 1000, 1)
  let assert [
    protocol.EventMessage("a", _, event),
    protocol.CaughtUp(_, _),
    protocol.Complete(_),
  ] = finished.responses
  assert list.length(finished.state.completed) == 1
  let valid =
    token(key, "events", "tenant", "user", 1100, ["subscribe", "history"])
  let sibling = subscribe(finished.state, cfg, reg, "b", "events", valid)
  let expired_ack =
    session.handle(
      sibling.state,
      protocol.Ack("a", event.cursor),
      cfg,
      reg,
      1001,
      2,
    )
  assert expired_ack.responses
    == [protocol.Failure("", "a", domain.TokenExpired)]
  let pruned = session.tick(expired_ack.state, cfg, 1001, 3)
  assert pruned.state.completed == []
  assert dict.has_key(pruned.state.subscriptions, "b")
  assert pruned.close == None
  session.cleanup(pruned.state)
  registry.stop(reg)
}
