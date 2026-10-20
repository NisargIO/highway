import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{None, Some}
import gleam/result
import highway/domain.{type Cursor, type Error, type Event, type Stream, Stream}

pub type From {
  Latest
  Earliest
  After(Cursor)
}

pub type Command {
  Auth(id: String, token: String)
  Publish(id: String, stream: Stream, message_id: String, data: json.Json)
  Subscribe(
    id: String,
    subscription: String,
    stream: Stream,
    from: From,
    follow: Bool,
    window: Int,
  )
  Ack(subscription: String, through: Cursor)
  Unsubscribe(id: String, subscription: String)
}

pub type Response {
  Hello(auth_timeout_ms: Int)
  S3Hello(auth_timeout_ms: Int)
  Persisted(stream: Stream, through: Cursor)
  Discontinuity(subscription: String, after: Cursor, before_epoch: Int)
  Authenticated(id: String, subject: String, tenant: String, expires_at: Int)
  Accepted(id: String, stream: Stream, event: Event)
  Subscribed(id: String, subscription: String, high_watermark: Cursor)
  EventMessage(subscription: String, stream: Stream, event: Event)
  CaughtUp(subscription: String, through: Cursor)
  Complete(subscription: String)
  Unsubscribed(id: String, subscription: String)
  Failure(id: String, subscription: String, error: Error)
}

pub fn name() -> decode.Decoder(String) {
  use value <- decode.then(decode.string)
  case domain.identifier(value) {
    True -> decode.success(value)
    False -> decode.failure("", "nonempty identifier up to 200 UTF-8 bytes")
  }
}

pub fn json_value() -> decode.Decoder(json.Json) {
  decode.one_of(decode.map(decode.int, json.int), [
    decode.map(decode.float, json.float),
    decode.map(decode.bool, json.bool),
    decode.map(decode.optional(decode.string), fn(x) {
      case x {
        Some(s) -> json.string(s)
        None -> json.null()
      }
    }),
    decode.map(
      decode.list(decode.recursive(json_value)),
      json.preprocessed_array,
    ),
    decode.map(
      decode.dict(decode.string, decode.recursive(json_value)),
      json.dict(_, fn(x) { x }, fn(x) { x }),
    ),
  ])
}

fn cursor_decoder() -> decode.Decoder(Cursor) {
  use value <- decode.then(decode.string)
  case domain.cursor(value) {
    Ok(c) -> decode.success(c)
    Error(_) -> decode.failure(domain.Cursor("invalid", 1, 0), "v1 cursor")
  }
}

fn from_decoder() -> decode.Decoder(From) {
  decode.one_of(
    {
      use s <- decode.then(decode.string)
      case s {
        "latest" -> decode.success(Latest)
        "earliest" -> decode.success(Earliest)
        _ -> decode.failure(Latest, "latest or earliest")
      }
    },
    [
      {
        use c <- decode.field("after", cursor_decoder())
        decode.success(After(c))
      },
    ],
  )
}

fn command() -> decode.Decoder(Command) {
  use version <- decode.optional_field("version", 1, decode.int)
  use kind <- decode.field("type", decode.string)
  case version, kind {
    1, "ack" -> {
      use sub <- decode.field("subscription", name())
      use c <- decode.field("through", cursor_decoder())
      decode.success(Ack(sub, c))
    }
    1, "auth" -> {
      use id <- decode.field("id", name())
      use token <- decode.field("token", decode.string)
      decode.success(Auth(id, token))
    }
    1, "publish" -> {
      use id <- decode.field("id", name())
      use stream <- decode.field("stream", name())
      use message <- decode.field("message_id", name())
      use data <- decode.field("data", json_value())
      decode.success(Publish(id, Stream(stream), message, data))
    }
    1, "subscribe" -> {
      use id <- decode.field("id", name())
      use sub <- decode.field("subscription", name())
      use stream <- decode.field("stream", name())
      use from <- decode.optional_field("from", Latest, from_decoder())
      use follow <- decode.optional_field("follow", True, decode.bool)
      use window <- decode.optional_field("window", 128, decode.int)
      case window > 0 && window <= 1024 {
        True ->
          decode.success(Subscribe(
            id,
            sub,
            Stream(stream),
            from,
            follow,
            window,
          ))
        False -> decode.failure(Auth("", ""), "window between 1 and 1024")
      }
    }
    1, "unsubscribe" -> {
      use id <- decode.field("id", name())
      use sub <- decode.field("subscription", name())
      decode.success(Unsubscribe(id, sub))
    }
    _, _ -> decode.failure(Auth("", ""), "supported v1 command")
  }
}

pub fn decode(frame: String) -> Result(Command, Error) {
  use header <- result.try(
    json.parse(frame, {
      use version <- decode.optional_field("version", 1, decode.int)
      use kind <- decode.field("type", decode.string)
      decode.success(#(version, kind))
    })
    |> result.replace_error(domain.InvalidFrame),
  )
  case header {
    #(v, _) if v != 1 -> Error(domain.UnsupportedVersion)
    #(_, kind) ->
      case
        list.contains(
          ["auth", "publish", "subscribe", "ack", "unsubscribe"],
          kind,
        )
      {
        False -> Error(domain.UnknownCommand)
        True ->
          json.parse(frame, command())
          |> result.replace_error(domain.InvalidFrame)
      }
  }
}

pub fn encode(response: Response) -> String {
  let fields = case response {
    Hello(timeout) -> [
      #("type", json.string("hello")),
      #("version", json.int(1)),
      #("auth_timeout_ms", json.int(timeout)),
      #("storage", json.string("development_memory")),
      #("durable", json.bool(False)),
    ]
    S3Hello(timeout) -> [
      #("type", json.string("hello")),
      #("version", json.int(1)),
      #("auth_timeout_ms", json.int(timeout)),
      #("storage", json.string("s3_async")),
      #("durable", json.bool(False)),
    ]
    Discontinuity(sub, after, before) -> [
      #("type", json.string("discontinuity")),
      #("subscription", json.string(sub)),
      #("after", json.string(domain.cursor_string(after))),
      #("before_epoch", json.string(int.to_string(before))),
      #("reason", json.string("interrupted_epoch")),
      #("lost_count", json.null()),
    ]
    Persisted(Stream(stream), c) -> [
      #("type", json.string("persisted")),
      #("stream", json.string(stream)),
      #("through", json.string(domain.cursor_string(c))),
    ]
    Authenticated(id, subject, tenant, expires) -> [
      #("type", json.string("authenticated")),
      #("id", json.string(id)),
      #("subject", json.string(subject)),
      #("tenant", json.string(tenant)),
      #("expires_at", json.int(expires)),
    ]
    Accepted(id, Stream(stream), event) -> [
      #("type", json.string("accepted")),
      #("id", json.string(id)),
      #("stream", json.string(stream)),
      #("cursor", json.string(domain.cursor_string(event.cursor))),
      #("message_id", json.string(event.message_id)),
    ]
    Subscribed(id, sub, c) -> [
      #("type", json.string("subscribed")),
      #("id", json.string(id)),
      #("subscription", json.string(sub)),
      #("high_watermark", json.string(domain.cursor_string(c))),
    ]
    EventMessage(sub, Stream(stream), event) -> [
      #("type", json.string("event")),
      #("subscription", json.string(sub)),
      #("stream", json.string(stream)),
      #("cursor", json.string(domain.cursor_string(event.cursor))),
      #("message_id", json.string(event.message_id)),
      #("data", event.data),
    ]
    CaughtUp(sub, c) -> [
      #("type", json.string("caught_up")),
      #("subscription", json.string(sub)),
      #("through", json.string(domain.cursor_string(c))),
    ]
    Complete(sub) -> [
      #("type", json.string("complete")),
      #("subscription", json.string(sub)),
    ]
    Unsubscribed(id, sub) -> [
      #("type", json.string("unsubscribed")),
      #("id", json.string(id)),
      #("subscription", json.string(sub)),
    ]
    Failure(id, sub, error) -> [
      #("type", json.string("error")),
      #("id", json.string(id)),
      #("subscription", json.string(sub)),
      #("code", json.string(domain.code(error))),
      #(
        "retryable",
        json.bool(list.contains(
          [domain.RateLimited, domain.StorageBacklog, domain.Unavailable],
          error,
        )),
      ),
    ]
  }
  json.object(fields) |> json.to_string
}
