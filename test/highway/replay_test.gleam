import gleam/json
import gleam/list
import gleam/option.{Some}
import highway/domain
import highway/protocol
import highway/replay/read
import highway/storage
import highway/storage/archive
import highway/storage/format
import highway/storage_fake as fake

fn append(store: storage.Store, base: archive.Session, offsets: List(Int)) {
  let events =
    list.map(offsets, fn(n) {
      domain.Event(
        domain.Cursor(base.head.incarnation, base.head.epoch, n),
        "record",
        json.int(n),
        263,
      )
    })
  let assert Ok(plan) = archive.plan(base, events)
  let assert Ok(committed) = archive.commit(store, plan)
  #(committed, events, plan)
}

fn setup() {
  let f = fake.new()
  let scope =
    archive.scope("test", domain.Tenant("tenant"), domain.Stream("events"))
  let assert Ok(base) = archive.open(f.store, scope, True)
  #(f, base)
}

fn finish(
  f: fake.Fake,
  scan: read.Scan,
  after: domain.Cursor,
  turns: Int,
) -> read.Batch {
  assert turns > 0
  case
    read.step(
      f.store,
      archive.scope("test", domain.Tenant("tenant"), domain.Stream("events")),
      scan,
      after,
      1,
      100,
      16,
      131_072,
      "s",
      domain.Stream("events"),
    )
  {
    Ok(read.Continue(next)) -> {
      assert next.pages == scan.pages + 1
      finish(f, next, after, turns - 1)
    }
    Ok(read.Ready(batch)) -> batch
    _ -> panic as "replay did not produce an ordered batch"
  }
}

pub fn reverse_index_steps_keep_exclusive_order_and_pin_authoritative_root_test() {
  let #(f, base) = setup()
  let #(one, first, _) = append(f.store, base, [1, 2])
  let #(two, second, _) = append(f.store, one, [3, 4])
  let zero = domain.Cursor(base.head.incarnation, 1, 0)
  let target = two.head.archived
  let assert Ok(scan) = read.start(f.store, base.scope, target, 0)
  let assert Ok(read.Continue(scan)) =
    read.step(
      f.store,
      base.scope,
      scan,
      zero,
      1,
      100,
      16,
      131_072,
      "s",
      domain.Stream("events"),
    )
  // Head and cache may move while an immutable traversal is in flight. A
  // speculative uploaded segment is also deliberately left unreachable.
  let #(three, _, _) = append(f.store, two, [5])
  let assert Ok(orphan) =
    archive.plan(three, [
      domain.Event(
        domain.Cursor(base.head.incarnation, 1, 6),
        "orphan",
        json.null(),
        260,
      ),
    ])
  let assert Ok(_) =
    f.store.put(orphan.segment_key, orphan.segment_body, storage.Absent)
  let batch = finish(f, scan, zero, 10)
  assert batch.items == list.map(first, read.Record)
  let assert Ok(scan) = read.start(f.store, base.scope, target, 0)
  let next = finish(f, scan, batch.through, 10)
  assert next.items == list.map(second, read.Record)
  assert next.through == target
  fake.stop(f)
}

pub fn lost_resume_tail_and_multiple_empty_interrupted_epochs_test() {
  let #(f, base) = setup()
  let #(committed, _, _) = append(f.store, base, [1, 2])
  let assert Ok(_) = archive.open(f.store, base.scope, False)
  let assert Ok(_) = archive.open(f.store, base.scope, False)
  let assert Ok(four) = archive.open(f.store, base.scope, False)
  let #(four, events, _) = append(f.store, four, [1])
  let lost = domain.Cursor(base.head.incarnation, 1, 999)
  let #(items, through) =
    list.fold([2, 3, 4], #([], lost), fn(acc, before) {
      let assert Ok(scan) =
        read.start(f.store, base.scope, four.head.archived, 0)
      let batch = finish(f, scan, acc.1, 10)
      assert batch.items == [read.Boundary(committed.head.archived, before)]
      #(list.append(acc.0, batch.items), batch.through)
    })
  assert list.length(items) == 3
  assert through == domain.Cursor(base.head.incarnation, 4, 0)
  let assert Ok(scan) = read.start(f.store, base.scope, four.head.archived, 0)
  assert finish(f, scan, through, 10).items == list.map(events, read.Record)
  assert protocol.encode(protocol.Discontinuity("s", committed.head.archived, 2))
    == "{\"type\":\"discontinuity\",\"subscription\":\"s\",\"after\":\""
    <> domain.cursor_string(committed.head.archived)
    <> "\",\"before_epoch\":\"2\",\"reason\":\"interrupted_epoch\",\"lost_count\":null}"
  fake.stop(f)
}

pub fn reader_limits_errors_and_corrupt_chain_never_become_eof_test() {
  let #(f, base) = setup()
  let #(one, _, _) = append(f.store, base, [1])
  let #(two, _, plan) = append(f.store, one, [2])
  let zero = domain.Cursor(base.head.incarnation, 1, 0)
  let assert Ok(scan) = read.start(f.store, base.scope, two.head.archived, 0)
  assert read.step(
      f.store,
      base.scope,
      scan,
      zero,
      8,
      1,
      16,
      131_072,
      "s",
      domain.Stream("events"),
    )
    == Error(storage.Limit)
  assert read.step(
      f.store,
      base.scope,
      scan,
      zero,
      8,
      10,
      1,
      1,
      "s",
      domain.Stream("events"),
    )
    == Error(storage.Access(domain.SlowConsumer))
  fake.fail_read(f, plan.index_key, storage.Http(503))
  assert read.step(
      f.store,
      base.scope,
      scan,
      zero,
      8,
      10,
      16,
      131_072,
      "s",
      domain.Stream("events"),
    )
    == Error(storage.Http(503))
  let assert Ok(object) = f.store.get(plan.index_key)
  let assert Ok(index) = format.decode_index(object.body)
  // A self-link does not require an unbounded visited-key set: monotonic cursor
  // continuity detects it before any candidate segment can be exposed.
  let assert Ok(_) =
    f.store.put(
      plan.index_key,
      format.index(format.Index(..index, previous: Some(plan.index_key))),
      storage.Match(object.etag),
    )
  let assert Error(storage.Corrupt(_)) =
    read.step(
      f.store,
      base.scope,
      scan,
      zero,
      8,
      10,
      16,
      131_072,
      "s",
      domain.Stream("events"),
    )
  fake.stop(f)
}
