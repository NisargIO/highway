import gleam/json
import highway/config
import highway/domain
import highway/stream

pub fn ordering_capacity_age_and_commit_boundary_test() {
  let limits =
    config.Limits(
      ..config.defaults(),
      stream_bytes: 600,
      max_records: 2,
      pending_age_ms: 100,
    )
  let empty = stream.new("one")
  let assert Ok(#(one, first)) =
    stream.append(empty, "m", json.null(), limits, 0)
  let assert Ok(#(two, second)) =
    stream.append(one, "m", json.null(), limits, 99)
  assert first.cursor.offset == 1
  assert second.cursor.offset == 2
  assert two.persisted == 0
  assert stream.append(two, "m", json.null(), limits, 99)
    == Error(domain.StorageBacklog)
  assert stream.append(one, "m", json.null(), limits, 100)
    == Error(domain.StorageBacklog)
  assert stream.watermark(one).offset == 1
  let assert Ok(committed) = stream.committed(two, second.cursor)
  assert committed.persisted == 2
  assert stream.committed(committed, first.cursor)
    == Error(domain.CursorInvalid)
}

pub fn cross_stream_and_restart_cursors_never_resume_test() {
  let old = stream.new("old")
  let restarted = stream.interrupted("new")
  assert stream.validate_cursor(restarted, stream.watermark(old))
    == Error(domain.CursorInvalid)
  assert stream.validate_cursor(old, domain.Cursor("other-stream", 1, 0))
    == Error(domain.CursorInvalid)
  assert stream.validate_cursor(old, domain.Cursor("old", 1, 1))
    == Error(domain.CursorAhead)
}

pub fn confirmed_commits_reset_pending_age_and_bound_live_cache_without_skips_test() {
  let limits = config.defaults()
  let initial = stream.new("live")
  let assert Ok(#(one, first)) =
    stream.append(initial, "first", json.null(), limits, 100)
  let assert Ok(#(two, second)) =
    stream.append(one, "second", json.null(), limits, 200)
  let assert Ok(committed_first) = stream.committed(two, first.cursor)
  assert committed_first.oldest_ms == Some(200)
  assert committed_first.bytes == second.bytes
  let assert Ok(committed_both) =
    stream.committed(committed_first, second.cursor)
  assert committed_both.oldest_ms == None
  assert committed_both.bytes == 0
  assert stream.next(committed_both, domain.Cursor("live", 1, 0))
    == Error(domain.SlowConsumer)
  let assert Ok(Some(last)) = stream.next(committed_both, first.cursor)
  assert last.cursor == second.cursor
}

import gleam/option.{None, Some}
