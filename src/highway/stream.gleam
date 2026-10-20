import gleam/dict.{type Dict}
import gleam/json
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import highway/config.{type Limits}
import highway/domain.{type Cursor, type Error, type Event, Cursor, Event}

pub type State {
  State(
    incarnation: String,
    epoch: Int,
    offset: Int,
    bytes: Int,
    oldest_ms: Option(Int),
    events: Dict(Int, Event),
    persisted: Int,
    times: Dict(Int, Int),
  )
}

pub fn new(incarnation: String) -> State {
  State(incarnation, 1, 0, 0, None, dict.new(), 0, dict.new())
}

pub fn watermark(s: State) -> Cursor {
  Cursor(s.incarnation, s.epoch, s.offset)
}

pub fn event_bytes(id: String, data: json.Json) -> Int {
  string.byte_size(json.to_string(data)) + string.byte_size(id) + 256
}

pub fn append(
  s: State,
  message_id: String,
  data: json.Json,
  limits: Limits,
  now_ms: Int,
) -> Result(#(State, Event), Error) {
  let size = event_bytes(message_id, data)
  let too_old = case s.oldest_ms {
    Some(t) -> now_ms - t >= limits.pending_age_ms
    None -> False
  }
  case
    size + s.bytes > limits.stream_bytes
    || dict.size(s.times) >= limits.max_records
    || too_old
  {
    True -> Error(domain.StorageBacklog)
    False -> {
      let e =
        Event(
          Cursor(s.incarnation, s.epoch, s.offset + 1),
          message_id,
          data,
          size,
        )
      Ok(#(
        State(
          ..s,
          offset: s.offset + 1,
          bytes: s.bytes + size,
          oldest_ms: case s.oldest_ms {
            None -> Some(now_ms)
            some -> some
          },
          events: dict.insert(s.events, e.cursor.offset, e),
          times: dict.insert(s.times, e.cursor.offset, now_ms),
        ),
        e,
      ))
    }
  }
}

pub fn validate_cursor(s: State, c: Cursor) -> Result(Nil, Error) {
  case c.incarnation != s.incarnation || c.epoch != s.epoch {
    True -> Error(domain.CursorInvalid)
    False ->
      case c.offset > s.offset {
        True -> Error(domain.CursorAhead)
        False -> Ok(Nil)
      }
  }
}

pub fn next(s: State, after: Cursor) -> Result(Option(Event), Error) {
  use _ <- result.try(validate_cursor(s, after))
  case dict.get(s.events, after.offset + 1) {
    Ok(e) -> Ok(Some(e))
    Error(_) ->
      case after.offset < s.offset {
        True -> Error(domain.SlowConsumer)
        False -> Ok(None)
      }
  }
}

/// Called only after an authoritative head commit. Keep one committed batch
/// as a bounded live cache; pending data is never discarded for admission.
pub fn committed(s: State, through: Cursor) -> Result(State, Error) {
  use _ <- result.try(validate_cursor(s, through))
  case through.offset < s.persisted {
    True -> Error(domain.CursorInvalid)
    False -> {
      let events = dict.filter(s.events, fn(offset, _) { offset > s.persisted })
      let times =
        dict.filter(s.times, fn(offset, _) { offset > through.offset })
      let bytes =
        dict.fold(events, 0, fn(n, offset, e) {
          case offset > through.offset {
            True -> n + e.bytes
            False -> n
          }
        })
      let oldest =
        dict.fold(times, None, fn(old, _, time) {
          case old {
            Some(t) if t <= time -> old
            _ -> Some(time)
          }
        })
      Ok(
        State(
          ..s,
          persisted: through.offset,
          events: events,
          times: times,
          bytes: bytes,
          oldest_ms: oldest,
        ),
      )
    }
  }
}

pub fn batch(s: State, max_bytes: Int) -> List(Event) {
  batch_loop(s, s.persisted + 1, max_bytes, [], 0)
}

fn batch_loop(
  s: State,
  offset: Int,
  remaining: Int,
  events: List(Event),
  count: Int,
) -> List(Event) {
  case dict.get(s.events, offset) {
    Ok(e) if e.bytes <= remaining && count < 4096 ->
      batch_loop(s, offset + 1, remaining - e.bytes, [e, ..events], count + 1)
    _ -> list.reverse(events)
  }
}

import gleam/list

/// Interrupted memory owners always receive a fresh incarnation, never resume.
pub fn interrupted(new_incarnation: String) -> State {
  new(new_incarnation)
}
