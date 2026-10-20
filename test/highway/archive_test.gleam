import gleam/dict
import gleam/json
import gleam/list
import gleam/option.{None, Some}
import highway/config
import highway/domain
import highway/storage
import highway/storage/archive
import highway/storage/format
import highway/storage_fake as fake
import highway/stream

fn setup() {
  let fake = fake.new()
  let scope =
    archive.scope("test", domain.Tenant("tenant"), domain.Stream("events"))
  let assert Ok(session) = archive.open(fake.store, scope, True)
  let state =
    stream.State(
      ..stream.new(session.head.incarnation),
      epoch: session.head.epoch,
    )
  let assert Ok(#(_, event)) =
    stream.append(
      state,
      "m",
      json.object([#("text", json.string("hello"))]),
      config.defaults(),
      0,
    )
  let assert Ok(plan) = archive.plan(session, [event])
  #(fake, plan, event)
}

fn head(fake: fake.Fake, scope: String) {
  let assert Ok(object) = fake.store.get(scope <> "head.json")
  let assert Ok(head) = format.decode_head(object.body)
  head
}

pub fn uploads_are_not_persistence_and_retry_preserves_batch_identity_test() {
  let #(fake, plan, event) = setup()
  fake.fail(
    fake,
    plan.index_key,
    fake.Before(storage.Transport("index unavailable")),
  )
  let assert Error(_) = archive.commit(fake.store, plan)
  assert dict.has_key(fake.objects(fake), plan.segment_key)
  let assert Ok(#(_, None, [])) =
    archive.published_tip(fake.store, plan.base.scope)
  assert head(fake, plan.base.scope).index == None
  assert head(fake, plan.base.scope).archived.offset == 0
  fake.fail(
    fake,
    plan.base.scope <> "head.json",
    fake.Before(storage.Http(503)),
  )
  let assert Error(_) = archive.commit(fake.store, plan)
  assert dict.has_key(fake.objects(fake), plan.index_key)
  assert head(fake, plan.base.scope).archived.offset == 0
  let assert Ok(committed) = archive.commit(fake.store, plan)
  assert committed.head.archived == event.cursor
  assert committed.head.index == Some(plan.index_key)
  let assert Ok(index) =
    archive.read_index(fake.store, plan.base.scope, plan.index_key)
  let assert Ok([stored]) =
    archive.read_segment(fake.store, plan.base.scope, index.entry)
  assert stored.message_id == event.message_id
  assert json.to_string(stored.data) == json.to_string(event.data)
  fake.stop(fake)
}

pub fn segment_failure_and_ambiguous_immutable_upload_test() {
  let #(fake, plan, _) = setup()
  fake.fail(fake, plan.segment_key, fake.Before(storage.Http(503)))
  let assert Error(_) = archive.commit(fake.store, plan)
  assert head(fake, plan.base.scope).index == None
  fake.fail(
    fake,
    plan.segment_key,
    fake.After(storage.Ambiguous("lost segment response")),
  )
  let assert Ok(_) = archive.commit(fake.store, plan)
  assert head(fake, plan.base.scope).index == Some(plan.index_key)
  fake.stop(fake)
}

pub fn head_timeout_is_reconciled_and_lost_response_retry_is_idempotent_test() {
  let #(fake, plan, event) = setup()
  fake.fail(
    fake,
    plan.base.scope <> "head.json",
    fake.After(storage.Ambiguous("lost head response")),
  )
  let assert Ok(committed) = archive.commit(fake.store, plan)
  assert committed.head.archived == event.cursor
  // Lost worker/publisher completion: a repeated frozen plan cannot duplicate.
  let assert Ok(retried) = archive.commit(fake.store, plan)
  assert retried.head == committed.head
  assert dict.size(fake.objects(fake)) == 3
  fake.stop(fake)
}

pub fn ambiguous_head_and_failed_reconciliation_remain_unconfirmed_test() {
  let #(fake, plan, event) = setup()
  fake.fail(
    fake,
    plan.base.scope <> "head.json",
    fake.After(storage.Ambiguous("timeout")),
  )
  fake.fail_read(
    fake,
    plan.base.scope <> "head.json",
    storage.Transport("head read failed"),
  )
  let assert Error(storage.Transport("head read failed")) =
    archive.commit(fake.store, plan)
  // Actual durable state exists, but caller cannot report persisted yet.
  assert head(fake, plan.base.scope).archived == event.cursor
  let assert Ok(recovered) = archive.commit(fake.store, plan)
  assert recovered.head.archived == event.cursor
  fake.stop(fake)
}

pub fn conflicting_head_fences_writer_without_unconditional_overwrite_test() {
  let #(fake, plan, _) = setup()
  let competing = format.Head(..plan.base.head, commit_id: "another-owner")
  let assert Ok(_) =
    fake.store.put(
      plan.base.scope <> "head.json",
      format.head(competing),
      storage.Match(plan.base.etag),
    )
  assert archive.commit(fake.store, plan) == Error(storage.Conflict)
  assert head(fake, plan.base.scope) == competing
  fake.stop(fake)
}

pub fn restart_preserves_archive_and_records_unknown_tail_before_new_epoch_test() {
  let #(fake, plan, archived) = setup()
  let assert Ok(first) = archive.commit(fake.store, plan)
  let old =
    stream.State(
      ..stream.new(first.head.incarnation),
      epoch: first.head.epoch,
      offset: archived.cursor.offset,
    )
  let assert Ok(#(_, lost)) =
    stream.append(old, "lost-live", json.null(), config.defaults(), 0)
  let assert Ok(recovered) = archive.open(fake.store, first.scope, False)
  assert recovered.head.incarnation == first.head.incarnation
  assert recovered.head.epoch == first.head.epoch + 1
  assert recovered.head.archived == archived.cursor
  let assert Some(root) = recovered.head.index
  let assert Ok(boundary) = archive.read_index(fake.store, first.scope, root)
  assert boundary.previous == first.head.index
  assert boundary.entry
    == format.Interrupted(archived.cursor, recovered.head.epoch)
  let fresh =
    stream.State(
      ..stream.new(recovered.head.incarnation),
      epoch: recovered.head.epoch,
    )
  let assert Ok(#(_, next)) =
    stream.append(fresh, "new", json.null(), config.defaults(), 0)
  assert next.cursor != lost.cursor
  assert next.cursor.offset == 1
  // Another restart before accepting anything must retain both boundaries.
  let assert Ok(again) = archive.open(fake.store, recovered.scope, False)
  let assert Some(root2) = again.head.index
  let assert Ok(second_boundary) =
    archive.read_index(fake.store, recovered.scope, root2)
  assert second_boundary.previous == recovered.head.index
  assert again.head.epoch == 3
  fake.stop(fake)
}

pub fn startup_cannot_admit_before_epoch_and_boundary_head_commit_test() {
  let #(fake, plan, _) = setup()
  fake.fail(
    fake,
    plan.base.scope <> "head.json",
    fake.Before(storage.Http(503)),
  )
  let assert Error(_) = archive.open(fake.store, plan.base.scope, False)
  assert head(fake, plan.base.scope).epoch == 1
  // The uploaded boundary is orphaned; only a later committed head exposes it.
  let assert Ok(recovered) = archive.open(fake.store, plan.base.scope, False)
  assert recovered.head.epoch == 2
  fake.stop(fake)
}

pub fn storage_versions_order_checksums_and_cross_stream_paths_test() {
  let #(fake, plan, _) = setup()
  assert format.decode_head("{\"version\":2}")
    == Error(storage.Corrupt("head format"))
  let assert Ok(_) = archive.commit(fake.store, plan)
  let assert Ok(index) =
    archive.read_index(fake.store, plan.base.scope, plan.index_key)
  let assert Ok(object) = fake.store.get(plan.segment_key)
  let assert Ok(_) =
    fake.store.put(plan.segment_key, "{}", storage.Match(object.etag))
  let assert Error(_) =
    archive.read_segment(fake.store, plan.base.scope, index.entry)
  assert archive.read_index(fake.store, "other/", plan.index_key)
    == Error(storage.Corrupt("cross-stream object reference"))
  let assert Ok(decoded) = format.decode_segment(plan.segment_body, index.entry)
  assert list.length(decoded) == 1
  fake.stop(fake)
}

pub fn deterministic_store_obeys_conformance_contract_test() {
  let fake = fake.new()
  assert conformance.check(fake.store, "test") == Ok(Nil)
  fake.stop(fake)
}

import highway/storage/conformance
