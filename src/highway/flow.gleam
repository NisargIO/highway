import gleam/list
import gleam/option.{type Option, None, Some}
import highway/domain.{type Cursor, type Error}

pub type Outstanding {
  Outstanding(cursor: Cursor, bytes: Int, sent_ms: Int)
}

pub type Credit {
  Credit(
    window: Int,
    bytes: Int,
    pending: List(Outstanding),
    last_ack: Option(Cursor),
  )
}

pub fn new(window: Int) -> Credit {
  Credit(window, 0, [], None)
}

pub fn available(c: Credit) -> Bool {
  list.length(c.pending) < c.window
}

pub fn sent(c: Credit, cursor: Cursor, bytes: Int, now: Int) -> Credit {
  Credit(
    ..c,
    bytes: c.bytes + bytes,
    pending: list.append(c.pending, [Outstanding(cursor, bytes, now)]),
  )
}

pub fn ack(c: Credit, through: Cursor) -> Result(Credit, Error) {
  case c.last_ack == Some(through) {
    True -> Ok(c)
    False ->
      case list.any(c.pending, fn(p) { p.cursor == through }) {
        False -> Error(domain.CursorInvalid)
        True -> {
          let remaining =
            list.filter(c.pending, fn(p) { domain.after(p.cursor, through) })
          Ok(
            Credit(
              ..c,
              pending: remaining,
              bytes: list.fold(remaining, 0, fn(n, p) { n + p.bytes }),
              last_ack: Some(through),
            ),
          )
        }
      }
  }
}

pub fn timed_out(c: Credit, now: Int, timeout: Int) -> Bool {
  case c.pending {
    [first, ..] -> now - first.sent_ms >= timeout
    [] -> False
  }
}
