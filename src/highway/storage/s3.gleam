import envoy
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/result
import gleam/string
import gleam/uri
import highway/secrets
import highway/storage.{type Error, type Store}

pub type Settings {
  Settings(
    endpoint: String,
    bucket: String,
    prefix: String,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: String,
    timeout_ms: Int,
  )
}

@external(erlang, "highway_s3_ffi", "request")
fn request(
  settings: Settings,
  method: String,
  key: String,
  body: String,
  headers: List(#(String, String)),
) -> Result(#(Int, List(#(String, String)), String), Error)

pub fn validate(s: Settings) -> Result(Settings, String) {
  use url <- result.try(
    uri.parse(s.endpoint) |> result.replace_error("invalid S3 endpoint"),
  )
  let local = url.host == Some("127.0.0.1") || url.host == Some("localhost")
  case
    { url.scheme == Some("https") || url.scheme == Some("http") && local }
    && url.host != None
    && url.host != Some("")
    && url.userinfo == None
    && url.query == None
    && url.fragment == None
    && { url.path == "" || url.path == "/" }
    && s.bucket != ""
    && s.region != ""
    && string.byte_size(s.bucket) <= 63
    && string.byte_size(s.prefix) <= 64
    && safe_component(s.bucket)
    && safe_prefix(s.prefix)
    && s.access_key != ""
    && s.secret_key != ""
    && s.timeout_ms >= 100
    && s.timeout_ms <= 10_000
  {
    True -> Ok(s)
    False ->
      Error(
        "S3 requires HTTPS (or loopback HTTP), canonical bucket/prefix, credentials and a 100..10000 ms timeout",
      )
  }
}

fn safe_component(s: String) -> Bool {
  s != ""
  && list.all(string.to_utf_codepoints(s), fn(c) {
    let n = string.utf_codepoint_to_int(c)
    n >= 97 && n <= 122 || n >= 48 && n <= 57 || n == 45 || n == 46
  })
  && s != "."
  && !string.contains(s, "..")
}

fn safe_prefix(s: String) -> Bool {
  s != "" && list.all(string.split(s, "/"), fn(part) { safe_component(part) })
}

pub fn load() -> Result(Settings, String) {
  use bucket <- result.try(required("HIGHWAY_S3_BUCKET"))
  use prefix <- result.try(required("HIGHWAY_S3_PREFIX"))
  use region <- result.try(required("AWS_REGION"))
  use access <- result.try(secrets.required("AWS_ACCESS_KEY_ID"))
  use secret <- result.try(secrets.required("AWS_SECRET_ACCESS_KEY"))
  let endpoint =
    envoy.get("HIGHWAY_S3_ENDPOINT")
    |> result.unwrap("https://s3." <> region <> ".amazonaws.com")
  use timeout <- result.try(
    int.parse(envoy.get("HIGHWAY_S3_TIMEOUT_MS") |> result.unwrap("2000"))
    |> result.replace_error("invalid S3 timeout"),
  )
  use session <- result.try(secrets.optional("AWS_SESSION_TOKEN"))
  validate(Settings(
    case string.ends_with(endpoint, "/") {
      True -> string.drop_end(endpoint, 1)
      False -> endpoint
    },
    bucket,
    prefix,
    region,
    access,
    secret,
    case session {
      Some(value) -> value
      None -> ""
    },
    timeout,
  ))
}

fn required(name: String) -> Result(String, String) {
  envoy.get(name) |> result.replace_error(name <> " is required")
}

fn status(code: Int) -> Error {
  case code {
    404 -> storage.NotFound
    409 -> storage.Conflict
    412 -> storage.Precondition
    _ -> storage.Http(code)
  }
}

fn etag(headers: List(#(String, String))) -> Result(String, Error) {
  list.key_find(headers, "etag")
  |> result.replace_error(storage.Corrupt("missing ETag"))
}

pub fn store(settings: Settings) -> Store {
  storage.Store(
    get: fn(key) {
      use response <- result.try(request(settings, "GET", key, "", []))
      case response.0 {
        200 -> {
          use tag <- result.try(etag(response.1))
          Ok(storage.Object(response.2, tag))
        }
        code -> Error(status(code))
      }
    },
    put: fn(key, body, condition) {
      let header = case condition {
        storage.Absent -> #("if-none-match", "*")
        storage.Match(tag) -> #("if-match", tag)
      }
      use response <- result.try(request(settings, "PUT", key, body, [header]))
      case response.0 {
        200 -> etag(response.1)
        code -> Error(status(code))
      }
    },
  )
}
