import envoy
import gleam/dict
import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleam/list
import gleam/result
import highway/auth
import highway/protocol
import highway/storage/s3
import simplifile

pub type Limits {
  Limits(
    max_connections: Int,
    max_streams: Int,
    stream_bytes: Int,
    node_bytes: Int,
    max_records: Int,
    pending_age_ms: Int,
    subscriptions: Int,
    output_bytes: Int,
    slow_ms: Int,
    auth_ms: Int,
    idle_ms: Int,
    tick_ms: Int,
    replay_workers: Int,
    replay_pages_per_step: Int,
    replay_max_pages: Int,
    replay_timeout_ms: Int,
  )
}

pub type StorageMode {
  DevelopmentMemory
  S3(s3.Settings)
}

pub type Config {
  Config(
    bind: String,
    port: Int,
    auth: auth.Settings,
    origins: List(String),
    limits: Limits,
    storage: StorageMode,
  )
}

pub fn defaults() -> Limits {
  Limits(
    128,
    128,
    8_388_608,
    268_435_456,
    65_536,
    30_000,
    64,
    1_048_576,
    5000,
    5000,
    60_000,
    20,
    8,
    8,
    16_384,
    30_000,
  )
}

pub fn validate(l: Limits) -> Result(Limits, String) {
  case
    list.all(
      [
        l.max_connections, l.max_streams, l.stream_bytes, l.node_bytes,
        l.max_records, l.pending_age_ms, l.subscriptions, l.output_bytes,
        l.slow_ms, l.auth_ms, l.idle_ms, l.tick_ms, l.replay_workers,
        l.replay_pages_per_step, l.replay_max_pages, l.replay_timeout_ms,
      ],
      fn(n) { n > 0 },
    )
    && l.max_connections <= 1024
    && l.max_streams <= 1024
    && l.subscriptions <= 64
    && l.stream_bytes <= l.node_bytes
    && l.tick_ms <= 1000
    && l.replay_workers <= 16
    && l.replay_pages_per_step <= 64
    && l.replay_max_pages <= 1_000_000
    && l.replay_timeout_ms <= 300_000
  {
    True -> Ok(l)
    False -> Error("invalid positive capacity/time limits")
  }
}

fn key_decoder() -> decode.Decoder(#(String, String)) {
  use kid <- decode.field("kid", protocol.name())
  use key <- decode.then(protocol.json_value())
  decode.success(#(kid, json.to_string(key)))
}

pub fn load() -> Result(Config, String) {
  use mode <- result.try(
    envoy.get("HIGHWAY_STORAGE_MODE")
    |> result.replace_error("set HIGHWAY_STORAGE_MODE explicitly"),
  )
  use mode <- result.try(case mode {
    "development_memory" -> Ok(DevelopmentMemory)
    "s3_async" -> s3.load() |> result.map(S3)
    _ -> Error("storage mode must be development_memory or s3_async")
  })
  use path <- result.try(
    envoy.get("HIGHWAY_JWKS_FILE")
    |> result.replace_error("HIGHWAY_JWKS_FILE is required"),
  )
  use bytes <- result.try(
    simplifile.read(path) |> result.map_error(simplifile.describe_error),
  )
  use keys <- result.try(
    json.parse(bytes, {
      use keys <- decode.field("keys", decode.list(key_decoder()))
      decode.success(keys)
    })
    |> result.replace_error("invalid JWKS document"),
  )
  use _ <- result.try(
    case
      keys != []
      && list.length(keys) <= 16
      && dict.size(dict.from_list(keys)) == list.length(keys)
    {
      True -> Ok(Nil)
      False -> Error("JWKS needs 1..16 unique kid entries")
    },
  )
  use _ <- result.try(list.try_each(keys, fn(pair) { auth.valid_key(pair.1) }))
  use issuer <- result.try(
    envoy.get("HIGHWAY_JWT_ISSUER")
    |> result.replace_error("HIGHWAY_JWT_ISSUER is required"),
  )
  use audience <- result.try(
    envoy.get("HIGHWAY_JWT_AUDIENCE")
    |> result.replace_error("HIGHWAY_JWT_AUDIENCE is required"),
  )
  use _ <- result.try(
    case protocol_name_valid(issuer) && protocol_name_valid(audience) {
      True -> Ok(Nil)
      False -> Error("issuer/audience must be nonempty")
    },
  )
  let port = envoy.get("HIGHWAY_PORT") |> result.unwrap("4000")
  use port <- result.try(
    int.parse(port) |> result.replace_error("invalid port"),
  )
  use _ <- result.try(case port > 0 && port <= 65_535 {
    True -> Ok(Nil)
    False -> Error("invalid port")
  })
  let bind = envoy.get("HIGHWAY_BIND") |> result.unwrap("127.0.0.1")
  use _ <- result.try(case bind == "127.0.0.1" {
    True -> Ok(Nil)
    False ->
      Error(
        "current server requires loopback 127.0.0.1; production packaging is pending",
      )
  })
  use limits <- result.try(load_limits())
  let origins = [
    envoy.get("HIGHWAY_ORIGIN") |> result.unwrap("http://localhost:3000"),
  ]
  Ok(Config(
    bind,
    port,
    auth.Settings(issuer, audience, dict.from_list(keys), 3600, 3600, 5),
    origins,
    limits,
    mode,
  ))
}

fn protocol_name_valid(s: String) -> Bool {
  s != ""
}

pub fn decode_limits(bytes: String) -> Result(Limits, String) {
  let d = defaults()
  use l <- result.try(
    json.parse(bytes, {
      use connections <- decode.optional_field(
        "max_connections",
        d.max_connections,
        decode.int,
      )
      use streams <- decode.optional_field(
        "max_streams",
        d.max_streams,
        decode.int,
      )
      use stream_bytes <- decode.optional_field(
        "stream_bytes",
        d.stream_bytes,
        decode.int,
      )
      use node_bytes <- decode.optional_field(
        "node_bytes",
        d.node_bytes,
        decode.int,
      )
      use records <- decode.optional_field(
        "max_records",
        d.max_records,
        decode.int,
      )
      use age <- decode.optional_field(
        "pending_age_ms",
        d.pending_age_ms,
        decode.int,
      )
      use subscriptions <- decode.optional_field(
        "subscriptions",
        d.subscriptions,
        decode.int,
      )
      use output <- decode.optional_field(
        "output_bytes",
        d.output_bytes,
        decode.int,
      )
      use slow <- decode.optional_field("slow_ms", d.slow_ms, decode.int)
      use auth <- decode.optional_field("auth_ms", d.auth_ms, decode.int)
      use idle <- decode.optional_field("idle_ms", d.idle_ms, decode.int)
      use tick <- decode.optional_field("tick_ms", d.tick_ms, decode.int)
      use replay_workers <- decode.optional_field(
        "replay_workers",
        d.replay_workers,
        decode.int,
      )
      use replay_pages <- decode.optional_field(
        "replay_pages_per_step",
        d.replay_pages_per_step,
        decode.int,
      )
      use replay_max <- decode.optional_field(
        "replay_max_pages",
        d.replay_max_pages,
        decode.int,
      )
      use replay_timeout <- decode.optional_field(
        "replay_timeout_ms",
        d.replay_timeout_ms,
        decode.int,
      )
      decode.success(Limits(
        connections,
        streams,
        stream_bytes,
        node_bytes,
        records,
        age,
        subscriptions,
        output,
        slow,
        auth,
        idle,
        tick,
        replay_workers,
        replay_pages,
        replay_max,
        replay_timeout,
      ))
    })
    |> result.replace_error("invalid limits JSON"),
  )
  validate(l)
}

fn load_limits() -> Result(Limits, String) {
  case envoy.get("HIGHWAY_LIMITS_FILE") {
    Error(_) -> validate(defaults())
    Ok(path) -> {
      use bytes <- result.try(
        simplifile.read(path) |> result.map_error(simplifile.describe_error),
      )
      decode_limits(bytes)
    }
  }
}
