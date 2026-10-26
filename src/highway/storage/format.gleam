import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import highway/config
import highway/domain.{type Cursor, type Event}
import highway/protocol
import highway/storage.{type Error}
import highway/stream

pub type Head {
  Head(
    incarnation: String,
    epoch: Int,
    index: Option(String),
    archived: Cursor,
    commit_id: String,
    closed: Bool,
    owner: Option(Lease),
  )
}

/// Cluster ownership lease (head format v3). `until` is the owner's wall-clock
/// deadline in milliseconds. It binds other nodes only while the head is open.
pub type Lease {
  Lease(node: String, region: String, until: Int)
}

pub type Entry {
  Segment(
    key: String,
    checksum: String,
    first: Cursor,
    last: Cursor,
    count: Int,
  )
  Interrupted(after: Cursor, before_epoch: Int)
}

pub type Index {
  Index(previous: Option(String), entry: Entry)
}

fn decimal() -> decode.Decoder(Int) {
  use s <- decode.then(decode.string)
  case int.parse(s) {
    Ok(n) if n >= 0 ->
      case int.to_string(n) == s && string.byte_size(s) <= 64 {
        True -> decode.success(n)
        False -> decode.failure(0, "canonical decimal")
      }
    _ -> decode.failure(0, "canonical decimal")
  }
}

pub fn cursor() -> decode.Decoder(Cursor) {
  use s <- decode.then(decode.string)
  case domain.cursor(s) {
    Ok(c) -> decode.success(c)
    Error(_) -> decode.failure(domain.Cursor("invalid", 1, 0), "cursor v1")
  }
}

fn version() -> decode.Decoder(Nil) {
  use v <- decode.field("version", decode.int)
  case v {
    1 -> decode.success(Nil)
    _ -> decode.failure(Nil, "storage format version 1")
  }
}

fn parse(
  bytes: String,
  decoder: decode.Decoder(a),
  label: String,
) -> Result(a, Error) {
  json.parse(bytes, decoder) |> result.replace_error(storage.Corrupt(label))
}

pub fn decode_head(bytes: String) -> Result(Head, Error) {
  use h <- result.try(parse(
    bytes,
    {
      use v <- decode.field("version", decode.int)
      use incarnation <- decode.field("incarnation", protocol.name())
      use epoch <- decode.field("epoch", decimal())
      use status <- decode.field("status", decode.string)
      use index <- decode.field("index", decode.optional(decode.string))
      use archived <- decode.field("archived", cursor())
      use commit <- decode.field("commit_id", protocol.name())
      use owner <- decode.optional_field(
        "owner",
        None,
        decode.optional({
          use node <- decode.field("node", decode.string)
          use region <- decode.field("region", decode.string)
          use until <- decode.field("lease_until", decimal())
          case config.node_name(node) && config.region_name(region) {
            True -> decode.success(Lease(node, region, until))
            False -> decode.failure(Lease("", "", 0), "owner lease")
          }
        }),
      )
      case
        {
          v == 1
          && status == "open"
          && owner == None
          || v == 2
          && { status == "open" || status == "closed" }
          && owner == None
          || v == 3
          && { status == "open" || status == "closed" }
          && owner != None
        }
        && epoch > 0
      {
        True ->
          decode.success(Head(
            incarnation,
            epoch,
            index,
            archived,
            commit,
            status == "closed",
            owner,
          ))
        False ->
          decode.failure(
            Head("", 1, None, archived, "", False, None),
            "open epoch",
          )
      }
    },
    "head format",
  ))
  case
    h.archived.incarnation == h.incarnation
    && h.archived.epoch <= h.epoch
    && { h.index != None || { h.archived.offset == 0 && h.epoch == 1 } }
  {
    True -> Ok(h)
    False -> Error(storage.Corrupt("head cursor/index relationship"))
  }
}

/// Standalone heads stay v2 so a singleton deployment can still roll back to a
/// v2 reader. Only clustered owners write v3 with an ownership lease.
pub fn head(h: Head) -> String {
  let owner = case h.owner {
    None -> []
    Some(Lease(node, region, until)) -> [
      #(
        "owner",
        json.object([
          #("node", json.string(node)),
          #("region", json.string(region)),
          #("lease_until", dec(until)),
        ]),
      ),
    ]
  }
  json.object([
    #(
      "version",
      json.int(case h.owner {
        None -> 2
        Some(_) -> 3
      }),
    ),
    #("incarnation", json.string(h.incarnation)),
    #("epoch", dec(h.epoch)),
    #(
      "status",
      json.string(case h.closed {
        True -> "closed"
        False -> "open"
      }),
    ),
    #("index", json.nullable(h.index, json.string)),
    #("archived", cur(h.archived)),
    #("commit_id", json.string(h.commit_id)),
    ..owner
  ])
  |> json.to_string
}

pub fn index(i: Index) -> String {
  let entry = case i.entry {
    Segment(key, checksum, first, last, count) ->
      json.object([
        #("kind", json.string("segment")),
        #("key", json.string(key)),
        #("sha256", json.string(checksum)),
        #("first", cur(first)),
        #("last", cur(last)),
        #("count", json.int(count)),
      ])
    Interrupted(after, before) ->
      json.object([
        #("kind", json.string("interrupted_epoch")),
        #("after", cur(after)),
        #("before_epoch", dec(before)),
        #("lost_count", json.null()),
      ])
  }
  json.object([
    #("version", json.int(1)),
    #("previous", json.nullable(i.previous, json.string)),
    #("entry", entry),
  ])
  |> json.to_string
}

pub fn decode_index(bytes: String) -> Result(Index, Error) {
  parse(
    bytes,
    {
      use _ <- decode.then(version())
      use previous <- decode.field("previous", decode.optional(decode.string))
      use entry <- decode.field("entry", {
        use kind <- decode.field("kind", decode.string)
        case kind {
          "segment" -> {
            use key <- decode.field("key", decode.string)
            use hash <- decode.field("sha256", decode.string)
            use first <- decode.field("first", cursor())
            use last <- decode.field("last", cursor())
            use count <- decode.field("count", decode.int)
            case
              first.incarnation == last.incarnation
              && first.epoch == last.epoch
              && first.offset > 0
              && count > 0
              && count <= 4096
              && last.offset - first.offset + 1 == count
            {
              True -> decode.success(Segment(key, hash, first, last, count))
              False ->
                decode.failure(
                  Interrupted(first, 1),
                  "contiguous segment bounds",
                )
            }
          }
          "interrupted_epoch" -> {
            use after <- decode.field("after", cursor())
            use before <- decode.field("before_epoch", decimal())
            use lost <- decode.field("lost_count", decode.optional(decode.int))
            case before > after.epoch && lost == None {
              True -> decode.success(Interrupted(after, before))
              False ->
                decode.failure(
                  Interrupted(after, before),
                  "uncertain interrupted epoch",
                )
            }
          }
          _ ->
            decode.failure(
              Interrupted(domain.Cursor("invalid", 1, 0), 1),
              "known index entry",
            )
        }
      })
      decode.success(Index(previous, entry))
    },
    "index format",
  )
}

pub fn segment(events: List(Event)) -> String {
  json.object([
    #("version", json.int(1)),
    #(
      "events",
      json.array(events, fn(e) {
        json.object([
          #("cursor", cur(e.cursor)),
          #("message_id", json.string(e.message_id)),
          #("data", e.data),
        ])
      }),
    ),
  ])
  |> json.to_string
}

pub fn decode_segment(
  bytes: String,
  entry: Entry,
) -> Result(List(Event), Error) {
  use events <- result.try(parse(
    bytes,
    {
      use _ <- decode.then(version())
      use events <- decode.field(
        "events",
        decode.list({
          use c <- decode.field("cursor", cursor())
          use id <- decode.field("message_id", protocol.name())
          use data <- decode.field("data", protocol.json_value())
          decode.success(domain.Event(c, id, data, stream.event_bytes(id, data)))
        }),
      )
      decode.success(events)
    },
    "segment format",
  ))
  case entry {
    Interrupted(_, _) -> Error(storage.Corrupt("boundary is not a segment"))
    Segment(_, hash, first, last, count) -> {
      let valid = list.length(events) == count && storage.sha256(bytes) == hash
      let #(contiguous, next) =
        list.fold(events, #(True, first.offset), fn(acc, e) {
          #(
            acc.0
              && e.cursor
              == domain.Cursor(first.incarnation, first.epoch, acc.1),
            acc.1 + 1,
          )
        })
      case valid && contiguous && next == last.offset + 1 {
        True -> Ok(events)
        False -> Error(storage.Corrupt("segment checksum or order"))
      }
    }
  }
}

fn cur(c: Cursor) -> json.Json {
  domain.cursor_string(c) |> json.string
}

fn dec(n: Int) -> json.Json {
  int.to_string(n) |> json.string
}
