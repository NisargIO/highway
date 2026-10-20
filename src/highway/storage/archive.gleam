import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import highway/domain.{type Event, type Stream, type Tenant}
import highway/runtime
import highway/storage.{type Error, type Store}
import highway/storage/format.{type Head}

pub type Session {
  Session(scope: String, head: Head, etag: String)
}

pub type Plan {
  Plan(
    base: Session,
    next: Head,
    segment_key: String,
    segment_body: String,
    index_key: String,
    index_body: String,
  )
}

pub fn scope(prefix: String, tenant: Tenant, stream: Stream) -> String {
  let domain.Tenant(t) = tenant
  let domain.Stream(s) = stream
  prefix
  <> "/v1/tenants/"
  <> storage.hex(t)
  <> "/streams/"
  <> storage.hex(s)
  <> "/"
}

/// Immutable retry is allowed only when existing bytes match the frozen plan.
pub fn immutable(
  store: Store,
  key: String,
  body: String,
) -> Result(Nil, Error) {
  case store.put(key, body, storage.Absent) {
    Ok(_) -> Ok(Nil)
    Error(cause) ->
      case store.get(key) {
        Ok(object) if object.body == body -> Ok(Nil)
        Ok(_) -> Error(storage.Conflict)
        Error(storage.NotFound) -> Error(cause)
        Error(read_cause) -> Error(read_cause)
      }
  }
}

/// Reconcile even a failed/ambiguous head PUT before deciding whether committed.
/// A concurrent head is never overwritten or adopted as our successful commit.
fn publish(
  store: Store,
  scope: String,
  base: Option(storage.Object),
  next: Head,
) -> Result(Session, Error) {
  let key = scope <> "head.json"
  let body = format.head(next)
  let condition = case base {
    None -> storage.Absent
    Some(o) -> storage.Match(o.etag)
  }
  case store.put(key, body, condition) {
    Ok(tag) -> Ok(Session(scope, next, tag))
    Error(cause) ->
      case store.get(key) {
        Ok(current) if current.body == body ->
          Ok(Session(scope, next, current.etag))
        Ok(current) ->
          case base {
            Some(old) if old.body == current.body && old.etag == current.etag ->
              Error(cause)
            _ -> Error(storage.Conflict)
          }
        Error(storage.NotFound) -> Error(cause)
        Error(read_cause) -> Error(read_cause)
      }
  }
}

pub fn open(
  store: Store,
  scope: String,
  create: Bool,
) -> Result(Session, Error) {
  case store.get(scope <> "head.json") {
    Error(storage.NotFound) if create -> {
      let incarnation = runtime.incarnation()
      let head =
        format.Head(
          incarnation,
          1,
          None,
          domain.Cursor(incarnation, 1, 0),
          runtime.incarnation(),
          False,
        )
      publish(store, scope, None, head)
    }
    Error(e) -> Error(e)
    Ok(object) -> {
      use old <- result.try(format.decode_head(object.body))
      // Read the authoritative tip before recovery; never use bucket listing.
      use _ <- result.try(validate_tip(store, scope, old))
      let id = runtime.incarnation()
      let key = scope <> "indexes/" <> id <> ".json"
      let boundary =
        format.Index(old.index, format.Interrupted(old.archived, old.epoch + 1))
      use _ <- result.try(immutable(store, key, format.index(boundary)))
      let next =
        format.Head(
          ..old,
          epoch: old.epoch + 1,
          index: Some(key),
          commit_id: id,
          closed: False,
        )
      publish(store, scope, Some(object), next)
    }
  }
}

fn validate_tip(store: Store, scope: String, head: Head) -> Result(Nil, Error) {
  case head.index {
    None -> Ok(Nil)
    Some(key) -> {
      use index <- result.try(read_index(store, scope, key))
      case index.entry {
        format.Interrupted(after, before) ->
          case after == head.archived && before == head.epoch {
            True -> Ok(Nil)
            False -> Error(storage.Corrupt("head boundary"))
          }
        format.Segment(_, _, _, last, _) ->
          case last == head.archived && last.epoch == head.epoch {
            True -> {
              use _ <- result.try(read_segment(store, scope, index.entry))
              Ok(Nil)
            }
            False -> Error(storage.Corrupt("head segment"))
          }
      }
    }
  }
}

pub fn plan(base: Session, events: List(Event)) -> Result(Plan, Error) {
  use _ <- result.try(case base.head.closed {
    True -> Error(storage.Conflict)
    False -> Ok(Nil)
  })
  use first <- result.try(
    list.first(events) |> result.replace_error(storage.Corrupt("empty batch")),
  )
  use last <- result.try(
    list.last(events) |> result.replace_error(storage.Corrupt("empty batch")),
  )
  let expected = case base.head.archived.epoch == base.head.epoch {
    True -> base.head.archived.offset + 1
    False -> 1
  }
  let #(valid, _) =
    list.fold(events, #(True, expected), fn(acc, e) {
      #(
        acc.0
          && e.cursor
          == domain.Cursor(base.head.incarnation, base.head.epoch, acc.1),
        acc.1 + 1,
      )
    })
  use _ <- result.try(case valid && list.length(events) <= 4096 {
    True -> Ok(Nil)
    False -> Error(storage.Corrupt("noncontiguous batch"))
  })
  let id = runtime.incarnation()
  let segment_key =
    base.scope
    <> "segments/"
    <> int.to_string(base.head.epoch)
    <> "/"
    <> id
    <> ".json"
  let index_key = base.scope <> "indexes/" <> id <> ".json"
  let body = format.segment(events)
  let entry =
    format.Segment(
      segment_key,
      storage.sha256(body),
      first.cursor,
      last.cursor,
      list.length(events),
    )
  let index = format.index(format.Index(base.head.index, entry))
  Ok(Plan(
    base,
    format.Head(
      ..base.head,
      index: Some(index_key),
      archived: last.cursor,
      commit_id: id,
    ),
    segment_key,
    body,
    index_key,
    index,
  ))
}

pub fn commit(store: Store, p: Plan) -> Result(Session, Error) {
  use _ <- result.try(immutable(store, p.segment_key, p.segment_body))
  use _ <- result.try(immutable(store, p.index_key, p.index_body))
  publish(
    store,
    p.base.scope,
    Some(storage.Object(format.head(p.base.head), p.base.etag)),
    p.next,
  )
}

pub fn read_index(
  store: Store,
  scope: String,
  key: String,
) -> Result(format.Index, Error) {
  use _ <- result.try(inside(key, scope <> "indexes/"))
  use object <- result.try(store.get(key))
  use index <- result.try(format.decode_index(object.body))
  use _ <- result.try(case index.previous {
    None -> Ok(Nil)
    Some(previous) -> inside(previous, scope <> "indexes/")
  })
  Ok(index)
}

pub fn read_segment(
  store: Store,
  scope: String,
  entry: format.Entry,
) -> Result(List(Event), Error) {
  case entry {
    format.Interrupted(_, _) -> Error(storage.Corrupt("boundary has no data"))
    format.Segment(key, _, _, _, _) -> {
      use _ <- result.try(inside(key, scope <> "segments/"))
      use object <- result.try(store.get(key))
      format.decode_segment(object.body, entry)
    }
  }
}

fn inside(key: String, prefix: String) -> Result(Nil, Error) {
  case
    string.starts_with(key, prefix)
    && !string.contains(key, "..")
    && !string.contains(key, "//")
    && string.byte_size(key) <= 1024
    && string.ends_with(key, ".json")
  {
    True -> Ok(Nil)
    False -> Error(storage.Corrupt("cross-stream object reference"))
  }
}

/// A bounded published snapshot for diagnostics/conformance. Full replay uses
/// the separate bounded reverse-index scanner.
/// It follows only the root named in head; bucket uploads/listing are not reads.
pub fn published_tip(
  store: Store,
  scope: String,
) -> Result(#(Head, Option(format.Index), List(Event)), Error) {
  use object <- result.try(store.get(scope <> "head.json"))
  use head <- result.try(format.decode_head(object.body))
  use _ <- result.try(validate_tip(store, scope, head))
  case head.index {
    None -> Ok(#(head, None, []))
    Some(key) -> {
      use index <- result.try(read_index(store, scope, key))
      case index.entry {
        format.Interrupted(_, _) -> Ok(#(head, Some(index), []))
        entry -> {
          use events <- result.try(read_segment(store, scope, entry))
          Ok(#(head, Some(index), events))
        }
      }
    }
  }
}

/// Frozen closing plan. A close is authoritative only after head CAS or exact
/// reconciliation; retries must retain this same commit identity.
pub type Closure {
  Closure(base: Session, next: Head)
}

pub fn closure(base: Session) -> Closure {
  Closure(
    base,
    format.Head(..base.head, closed: True, commit_id: runtime.incarnation()),
  )
}

pub fn close(store: Store, plan: Closure) -> Result(Session, Error) {
  publish(
    store,
    plan.base.scope,
    Some(storage.Object(format.head(plan.base.head), plan.base.etag)),
    plan.next,
  )
}
