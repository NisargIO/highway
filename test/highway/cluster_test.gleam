import gleam/dict
import gleam/erlang/process
import gleam/json
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam/string
import highway/config
import highway/domain
import highway/owner
import highway/runtime
import highway/storage
import highway/storage/archive
import highway/storage/format
import highway/storage_fake as fake

fn scope(name: String) -> String {
  archive.scope("cluster", domain.Tenant("t"), domain.Stream(name))
}

fn claimant(node: String, now: Int) -> archive.Claimant {
  archive.Claimant(node, "region-a", now, 1000, 100)
}

fn clustered(
  node: String,
  lease: Int,
  skew: Int,
  sleep: Int,
) -> config.Cluster {
  config.Clustered(node, "region-a", lease, skew, sleep)
}

fn head(f: fake.Fake, s: String) -> format.Head {
  let assert Ok(o) = dict.get(fake.objects(f), s <> "head.json")
  let assert Ok(h) = format.decode_head(o.body)
  h
}

fn eventually(attempts: Int, check: fn() -> Bool) -> Nil {
  case check(), attempts {
    True, _ -> Nil
    False, 0 -> panic as "condition not reached"
    False, _ -> {
      process.sleep(10)
      eventually(attempts - 1, check)
    }
  }
}

fn start(
  store: storage.Store,
  s: String,
  cluster: config.Cluster,
) -> owner.Owner {
  let assert Ok(started) =
    owner.start_cluster(
      config.defaults(),
      Some(store),
      s,
      True,
      runtime.slots(),
      None,
      cluster,
      #(domain.Tenant("t"), domain.Stream("s")),
    )
  started.data
}

fn publish(o: owner.Owner, id: String) -> Result(domain.Event, domain.Error) {
  owner.append(o, id, json.null(), runtime.monotonic_ms())
}

pub fn head_v3_round_trips_lease_and_v2_stays_lease_free_test() {
  let lease = format.Lease("a@host.example", "us-east-1", 1_700_000_000_123)
  let h =
    format.Head(
      "inc",
      3,
      Some("p/indexes/x.json"),
      domain.Cursor("inc", 2, 7),
      "commit",
      False,
      Some(lease),
    )
  let body = format.head(h)
  assert string.contains(body, "\"version\":3")
  assert format.decode_head(body) == Ok(h)
  let standalone = format.Head(..h, owner: None)
  assert string.contains(format.head(standalone), "\"version\":2")
  assert format.decode_head(format.head(standalone)) == Ok(standalone)
  // Version and lease presence must agree; node names stay atom-safe.
  let mislabeled =
    string.replace(format.head(standalone), "\"version\":2", "\"version\":3")
  let assert Error(_) = format.decode_head(mislabeled)
  let unsafe =
    format.head(
      format.Head(..h, owner: Some(format.Lease("Bad Node", "us-east-1", 1))),
    )
  let assert Error(_) = format.decode_head(unsafe)
}

pub fn claim_respects_unexpired_foreign_lease_test() {
  let f = fake.new()
  let s = scope("lease")
  let now = runtime.wall_ms()
  let assert Ok(a) = archive.claim(f.store, s, True, Some(claimant("a@h", now)))
  assert a.head.owner == Some(format.Lease("a@h", "region-a", now + 1000))
  // Inside lease + skew, another node is told where the owner lives.
  assert archive.claim(f.store, s, False, Some(claimant("b@h", now + 1099)))
    == Error(storage.Leased("a@h", "region-a", now + 1000))
  // The same node name reclaims at once: its previous owner process is gone.
  let assert Ok(again) =
    archive.claim(f.store, s, False, Some(claimant("a@h", now + 10)))
  assert again.head.epoch == 2
  // After lease + skew, a foreign claim takes over with a new epoch.
  let assert Ok(b) =
    archive.claim(f.store, s, False, Some(claimant("b@h", now + 1111)))
  assert b.head.epoch == 3
  assert b.head.owner == Some(format.Lease("b@h", "region-a", now + 2111))
  // Standalone recovery ignores leases and writes a lease-free v2 head.
  let assert Ok(single) = archive.open(f.store, s, False)
  assert single.head.owner == None
  assert head(f, s).owner == None
}

pub fn sleeping_head_wakes_anywhere_test() {
  let f = fake.new()
  let s = scope("sleep")
  let now = runtime.wall_ms()
  let assert Ok(a) = archive.claim(f.store, s, True, Some(claimant("a@h", now)))
  let assert Ok(_) = archive.close(f.store, archive.closure(a))
  // A closed head binds nobody even while its last lease is unexpired.
  let assert Ok(b) =
    archive.claim(f.store, s, False, Some(claimant("b@h", now + 1)))
  assert b.head.epoch == 2
  assert b.head.closed == False
  assert b.head.owner == Some(format.Lease("b@h", "region-a", now + 1001))
}

pub fn renewal_rewrites_only_the_lease_test() {
  let f = fake.new()
  let s = scope("renew")
  let now = runtime.wall_ms()
  let assert Ok(a) = archive.claim(f.store, s, True, Some(claimant("a@h", now)))
  let lease = format.Lease("a@h", "region-a", now + 5000)
  let assert Ok(renewed) = archive.close(f.store, archive.renewal(a, lease))
  assert renewed.head.owner == Some(lease)
  assert renewed.head.archived == a.head.archived
  assert renewed.head.index == a.head.index
  assert renewed.head.epoch == a.head.epoch
  assert renewed.head.closed == False
}

pub fn owner_waits_out_foreign_lease_then_takes_over_test() {
  let f = fake.new()
  let s = scope("takeover")
  // A lease held by a node this test VM cannot reach (no distribution).
  let now = runtime.wall_ms()
  let assert Ok(_) =
    archive.claim(
      f.store,
      s,
      True,
      Some(archive.Claimant("a@unreachable", "region-a", now, 400, 0)),
    )
  let o = start(f.store, s, clustered("b@h", 1000, 100, 0))
  assert publish(o, "early") == Error(domain.Unavailable)
  eventually(150, fn() { publish(o, "late") != Error(domain.Unavailable) })
  let h = head(f, s)
  assert h.epoch == 2
  let assert Some(lease) = h.owner
  assert lease.node == "b@h"
  // Takeover happened only after the old lease plus skew.
  assert runtime.wall_ms() >= now + 400 + 100
}

type Toggle {
  Set(Bool, process.Subject(Nil))
  Get(process.Subject(Bool))
}

fn toggle() -> process.Subject(Toggle) {
  let assert Ok(started) =
    actor.new(False)
    |> actor.on_message(fn(state, message) {
      case message {
        Set(value, reply) -> {
          process.send(reply, Nil)
          actor.continue(value)
        }
        Get(reply) -> {
          process.send(reply, state)
          actor.continue(state)
        }
      }
    })
    |> actor.start
  started.data
}

pub fn owner_fences_admission_before_lease_lapses_and_resumes_on_renewal_test() {
  let f = fake.new()
  let blocked = toggle()
  let store =
    storage.Store(f.store.get, fn(key, body, condition) {
      case
        string.ends_with(key, "head.json") && process.call(blocked, 1000, Get)
      {
        True -> Error(storage.Transport("blocked head"))
        False -> f.store.put(key, body, condition)
      }
    })
  let s = scope("fence")
  let o = start(store, s, clustered("a@h", 1000, 200, 0))
  eventually(100, fn() { publish(o, "first") |> is_ok })
  process.call(blocked, 1000, Set(True, _))
  // Renewals fail; admission stops before lease - skew, well inside 1 s.
  eventually(150, fn() { publish(o, "fenced") == Error(domain.Unavailable) })
  assert owner.stats(o).1 == False
  process.call(blocked, 1000, Set(False, _))
  // The frozen retry commits (nobody else claimed), renewing the lease.
  eventually(300, fn() { publish(o, "resumed") |> is_ok })
  let assert Some(lease) = head(f, s).owner
  assert lease.until > runtime.wall_ms()
}

pub fn idle_owner_renews_lease_without_history_change_test() {
  let f = fake.new()
  let s = scope("idle")
  let o = start(f.store, s, clustered("a@h", 1000, 100, 0))
  eventually(100, fn() { publish(o, "one") |> is_ok })
  eventually(100, fn() { head(f, s).archived.offset == 1 })
  let assert Some(first) = head(f, s).owner
  process.sleep(800)
  let h = head(f, s)
  let assert Some(later) = h.owner
  assert later.until > first.until
  assert h.archived.offset == 1
  assert publish(o, "two") |> is_ok
}

pub fn idle_owner_sleeps_by_closing_its_head_test() {
  let f = fake.new()
  let s = scope("doze")
  let o = start(f.store, s, clustered("a@h", 1000, 100, 300))
  eventually(100, fn() { publish(o, "one") |> is_ok })
  eventually(200, fn() { !owner.alive(o) })
  let h = head(f, s)
  assert h.closed
  assert h.archived.offset == 1
  // Waking elsewhere needs no lease wait and keeps the archived history.
  let assert Ok(b) =
    archive.claim(f.store, s, False, Some(claimant("b@h", runtime.wall_ms())))
  assert b.head.epoch == h.epoch + 1
  assert b.head.archived == h.archived
}

pub fn standalone_owner_keeps_v2_heads_test() {
  let f = fake.new()
  let s = scope("single")
  let o = start(f.store, s, config.Standalone)
  eventually(100, fn() { publish(o, "one") |> is_ok })
  eventually(100, fn() { head(f, s).archived.offset == 1 })
  assert head(f, s).owner == None
  let assert Ok(o) = dict.get(fake.objects(f), s <> "head.json")
  assert string.contains(o.body, "\"version\":2")
}

pub fn cluster_settings_validate_names_and_timing_test() {
  let valid =
    config.Clustered("highway-a@10.0.0.1", "eu-west-1", 10_000, 1000, 0)
  assert config.validate_cluster(valid) == Ok(valid)
  let assert Error(_) =
    config.validate_cluster(config.Clustered(..valid, node: "no-host"))
  let assert Error(_) =
    config.validate_cluster(config.Clustered(..valid, region: "EU West"))
  let assert Error(_) =
    config.validate_cluster(config.Clustered(..valid, skew_ms: 5000))
  let assert Error(_) =
    config.validate_cluster(config.Clustered(..valid, sleep_ms: 500))
}

fn is_ok(r: Result(a, b)) -> Bool {
  case r {
    Ok(_) -> True
    Error(_) -> False
  }
}
