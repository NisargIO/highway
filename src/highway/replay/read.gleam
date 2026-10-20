/// Bounded reverse traversal of authoritative immutable index links. Only one
/// candidate page is retained; each step validates continuity before exposing it.
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import highway/domain.{type Cursor, type Event, type Stream}
import highway/protocol
import highway/storage.{type Error, type Store}
import highway/storage/archive
import highway/storage/format

pub type Item {
  Record(Event)
  Boundary(after: Cursor, before_epoch: Int)
}

pub type Scan {
  Scan(
    root: Option(String),
    next: Option(String),
    candidate: Option(format.Entry),
    expected: Cursor,
    anchor: Option(Cursor),
    pages: Int,
    started_ms: Int,
    target: Cursor,
  )
}

pub type Batch {
  Batch(items: List(Item), through: Cursor)
}

pub type Step {
  Continue(Scan)
  Ready(Batch)
  Missing
}

pub fn position(item: Item) -> Cursor {
  case item {
    Record(e) -> e.cursor
    Boundary(after, before) -> domain.Cursor(after.incarnation, before, 0)
  }
}

pub fn response(item: Item, sub: String, stream: Stream) -> protocol.Response {
  case item {
    Record(e) -> protocol.EventMessage(sub, stream, e)
    Boundary(after, before) -> protocol.Discontinuity(sub, after, before)
  }
}

pub fn start(
  store: Store,
  scope: String,
  target: Cursor,
  ms: Int,
) -> Result(Scan, Error) {
  use object <- result.try(store.get(scope <> "head.json"))
  use head <- result.try(format.decode_head(object.body))
  case head.incarnation == target.incarnation && head.epoch == target.epoch {
    False -> Error(storage.Conflict)
    True -> {
      let logical =
        domain.Cursor(
          head.incarnation,
          head.epoch,
          case head.archived.epoch == head.epoch {
            True -> head.archived.offset
            False -> 0
          },
        )
      Ok(Scan(
        head.index,
        head.index,
        None,
        logical,
        Some(head.archived),
        0,
        ms,
        target,
      ))
    }
  }
}

pub fn step(
  store: Store,
  scope: String,
  scan: Scan,
  after: Cursor,
  pages: Int,
  max_pages: Int,
  count: Int,
  bytes: Int,
  sub: String,
  stream: Stream,
) -> Result(Step, Error) {
  let reached = domain.at_or_before(scan.expected, after)
  case scan.next {
    None -> {
      use _ <- result.try(
        case scan.expected == domain.Cursor(scan.target.incarnation, 1, 0) {
          True -> Ok(Nil)
          False -> Error(storage.Corrupt("missing index prefix"))
        },
      )
      finish(store, scope, scan, after, count, bytes, sub, stream)
    }
    Some(_) if reached ->
      finish(store, scope, scan, after, count, bytes, sub, stream)
    Some(_) if pages <= 0 -> Ok(Continue(scan))
    Some(_) if scan.pages >= max_pages -> Error(storage.Limit)
    Some(key) -> {
      use index <- result.try(archive.read_index(store, scope, key))
      use back <- result.try(previous(scan, index.entry))
      use _ <- result.try(
        case
          back.0 == domain.Cursor(scan.target.incarnation, 1, 0)
          && index.previous != None
        {
          True -> Error(storage.Corrupt("index before genesis"))
          False -> Ok(Nil)
        },
      )
      let candidate = case eligible(index.entry, after, scan.target) {
        True -> Some(index.entry)
        False -> scan.candidate
      }
      let next =
        Scan(
          ..scan,
          next: index.previous,
          candidate: candidate,
          expected: back.0,
          anchor: back.1,
          pages: scan.pages + 1,
        )
      step(
        store,
        scope,
        next,
        after,
        pages - 1,
        max_pages,
        count,
        bytes,
        sub,
        stream,
      )
    }
  }
}

fn previous(
  scan: Scan,
  entry: format.Entry,
) -> Result(#(Cursor, Option(Cursor)), Error) {
  case entry {
    format.Segment(_, _, first, last, _) -> {
      let anchored = case scan.anchor {
        None -> True
        Some(c) -> c == last
      }
      case last == scan.expected && anchored {
        False -> Error(storage.Corrupt("segment/index continuity"))
        True -> {
          let before =
            domain.Cursor(first.incarnation, first.epoch, first.offset - 1)
          Ok(
            #(before, case first.offset == 1 {
              True -> None
              False -> Some(before)
            }),
          )
        }
      }
    }
    format.Interrupted(after, before) -> {
      let anchored = case scan.anchor {
        None -> True
        Some(c) -> c == after
      }
      case
        scan.expected == domain.Cursor(after.incarnation, before, 0)
        && before >= 2
        && anchored
      {
        False -> Error(storage.Corrupt("interrupted epoch continuity"))
        True ->
          Ok(#(
            domain.Cursor(
              after.incarnation,
              before - 1,
              case after.epoch == before - 1 {
                True -> after.offset
                False -> 0
              },
            ),
            Some(after),
          ))
      }
    }
  }
}

fn eligible(entry: format.Entry, after: Cursor, target: Cursor) -> Bool {
  case entry {
    format.Segment(_, _, first, last, _) ->
      domain.after(last, after) && domain.at_or_before(first, target)
    format.Interrupted(c, before) -> {
      let boundary = domain.Cursor(c.incarnation, before, 0)
      domain.after(boundary, after) && domain.at_or_before(boundary, target)
    }
  }
}

fn finish(
  store: Store,
  scope: String,
  scan: Scan,
  after: Cursor,
  count: Int,
  bytes: Int,
  sub: String,
  stream: Stream,
) -> Result(Step, Error) {
  case scan.candidate {
    None -> Ok(Missing)
    Some(format.Interrupted(c, before)) -> {
      let item = Boundary(c, before)
      case
        string.byte_size(protocol.encode(response(item, sub, stream))) <= bytes
      {
        True -> Ok(Ready(Batch([item], position(item))))
        False -> Error(storage.Access(domain.SlowConsumer))
      }
    }
    Some(entry) -> {
      use events <- result.try(archive.read_segment(store, scope, entry))
      let events =
        list.filter(events, fn(e) {
          domain.after(e.cursor, after)
          && domain.at_or_before(e.cursor, scan.target)
        })
      select(events, count, bytes, sub, stream, [], after)
    }
  }
}

fn select(
  events: List(Event),
  count: Int,
  bytes: Int,
  sub: String,
  stream: Stream,
  items: List(Item),
  through: Cursor,
) -> Result(Step, Error) {
  case events {
    [] ->
      case items {
        [] -> Error(storage.Corrupt("empty replay interval"))
        _ -> Ok(Ready(Batch(list.reverse(items), through)))
      }
    [_, ..] if count <= 0 -> Ok(Ready(Batch(list.reverse(items), through)))
    [e, ..rest] -> {
      let size =
        string.byte_size(protocol.encode(protocol.EventMessage(sub, stream, e)))
      case size > bytes {
        True ->
          case items {
            [] -> Error(storage.Access(domain.SlowConsumer))
            _ -> Ok(Ready(Batch(list.reverse(items), through)))
          }
        False ->
          select(
            rest,
            count - 1,
            bytes - size,
            sub,
            stream,
            [Record(e), ..items],
            e.cursor,
          )
      }
    }
  }
}
