import gleam/int
import gleam/json
import gleam/list
import gleam/result
import gleam/string

pub type Tenant {
  Tenant(String)
}

pub type Stream {
  Stream(String)
}

pub type Action {
  Publish
  Subscribe
  History
}

pub type Cursor {
  Cursor(incarnation: String, epoch: Int, offset: Int)
}

pub type Event {
  Event(cursor: Cursor, message_id: String, data: json.Json, bytes: Int)
}

pub type Error {
  InvalidFrame
  UnknownCommand
  UnsupportedVersion
  Unauthenticated
  TokenExpired
  Forbidden
  FrameTooLarge
  StreamNotFound
  CursorInvalid
  CursorAhead
  SlowConsumer
  RateLimited
  StorageBacklog
  Unavailable
  Internal
  ReplayLimit
}

pub fn identifier(value: String) -> Bool {
  string.byte_size(value) > 0
  && string.byte_size(value) <= 200
  && list.all(string.to_utf_codepoints(value), fn(c) {
    let n = string.utf_codepoint_to_int(c)
    n >= 32 && n != 127
  })
}

pub fn cursor_string(c: Cursor) -> String {
  "v1:"
  <> c.incarnation
  <> ":"
  <> int.to_string(c.epoch)
  <> ":"
  <> int.to_string(c.offset)
}

pub fn cursor(value: String) -> Result(Cursor, Error) {
  case string.split(value, ":") {
    ["v1", incarnation, epoch, offset] -> {
      use _ <- result.try(case valid_incarnation(incarnation) {
        True -> Ok(Nil)
        False -> Error(CursorInvalid)
      })
      use e <- result.try(
        int.parse(epoch) |> result.replace_error(CursorInvalid),
      )
      use o <- result.try(
        int.parse(offset) |> result.replace_error(CursorInvalid),
      )
      case
        e > 0
        && o >= 0
        && int.to_string(e) == epoch
        && int.to_string(o) == offset
      {
        True -> Ok(Cursor(incarnation, e, o))
        False -> Error(CursorInvalid)
      }
    }
    _ -> Error(CursorInvalid)
  }
}

pub fn code(error: Error) -> String {
  case error {
    InvalidFrame -> "invalid_frame"
    UnknownCommand -> "unknown_command"
    UnsupportedVersion -> "unsupported_version"
    Unauthenticated -> "unauthenticated"
    TokenExpired -> "token_expired"
    Forbidden -> "forbidden"
    FrameTooLarge -> "frame_too_large"
    StreamNotFound -> "stream_not_found"
    CursorInvalid -> "cursor_invalid"
    CursorAhead -> "cursor_ahead"
    SlowConsumer -> "slow_consumer"
    RateLimited -> "rate_limited"
    StorageBacklog -> "storage_backlog"
    Unavailable -> "unavailable"
    Internal -> "internal"
    ReplayLimit -> "replay_limit"
  }
}

fn valid_incarnation(value: String) -> Bool {
  string.byte_size(value) > 0
  && string.byte_size(value) <= 128
  && list.all(string.to_utf_codepoints(value), fn(c) {
    let n = string.utf_codepoint_to_int(c)
    n >= 48
    && n <= 57
    || n >= 65
    && n <= 90
    || n >= 97
    && n <= 122
    || n == 95
    || n == 45
  })
}

/// Ordering is meaningful only inside one stream incarnation.
pub fn after(a: Cursor, b: Cursor) -> Bool {
  a.incarnation == b.incarnation
  && { a.epoch > b.epoch || { a.epoch == b.epoch && a.offset > b.offset } }
}

pub fn at_or_before(a: Cursor, b: Cursor) -> Bool {
  a == b || after(b, a)
}
