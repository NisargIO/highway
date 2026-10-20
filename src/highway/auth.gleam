import gleam/dict.{type Dict}
import gleam/dynamic/decode
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import highway/domain.{type Action, type Error, type Stream, type Tenant, Tenant}
import highway/protocol

pub type Settings {
  Settings(
    issuer: String,
    audience: String,
    keys: Dict(String, String),
    max_age: Int,
    max_lifetime: Int,
    skew: Int,
  )
}

pub type Grant {
  Grant(stream: Stream, actions: List(Action))
}

pub type Claims {
  Claims(subject: String, tenant: Tenant, expires_at: Int, grants: List(Grant))
}

type Payload {
  Payload(
    issuer: String,
    audiences: List(String),
    subject: String,
    tenant: Tenant,
    issued: Int,
    expires: Int,
    not_before: Option(Int),
    grants: List(Grant),
  )
}

@external(erlang, "highway_crypto_ffi", "header")
fn header(token: String) -> Result(String, String)

@external(erlang, "highway_crypto_ffi", "verify")
fn signature(key: String, token: String) -> Result(String, String)

@external(erlang, "highway_crypto_ffi", "valid_key")
pub fn valid_key(key: String) -> Result(Nil, String)

fn action() -> decode.Decoder(Action) {
  use s <- decode.then(decode.string)
  case s {
    "publish" -> decode.success(domain.Publish)
    "subscribe" -> decode.success(domain.Subscribe)
    "history" -> decode.success(domain.History)
    _ -> decode.failure(domain.Publish, "publish, subscribe or history")
  }
}

fn grant() -> decode.Decoder(Grant) {
  use stream <- decode.field("stream", protocol.name())
  use actions <- decode.field("actions", decode.list(action()))
  decode.success(Grant(domain.Stream(stream), actions))
}

fn payload() -> decode.Decoder(Payload) {
  use iss <- decode.field("iss", decode.string)
  use aud <- decode.field(
    "aud",
    decode.one_of(decode.list(decode.string), [
      decode.map(decode.string, fn(s) { [s] }),
    ]),
  )
  use sub <- decode.field("sub", protocol.name())
  use tenant <- decode.field("tenant", protocol.name())
  use iat <- decode.field("iat", decode.int)
  use exp <- decode.field("exp", decode.int)
  use nbf <- decode.optional_field("nbf", None, decode.map(decode.int, Some))
  use grants <- decode.field("grants", decode.list(grant()))
  decode.success(Payload(iss, aud, sub, Tenant(tenant), iat, exp, nbf, grants))
}

pub fn verify(
  settings: Settings,
  token: String,
  now: Int,
) -> Result(Claims, Error) {
  use protected <- result.try(
    header(token) |> result.replace_error(domain.Unauthenticated),
  )
  use kid <- result.try(
    json.parse(protected, {
      use alg <- decode.field("alg", decode.string)
      use kid <- decode.field("kid", protocol.name())
      use fields <- decode.then(decode.dict(decode.string, decode.dynamic))
      case
        alg == "EdDSA"
        && !dict.has_key(fields, "crit")
        && !dict.has_key(fields, "jku")
        && !dict.has_key(fields, "jwk")
        && !dict.has_key(fields, "x5u")
        && !dict.has_key(fields, "b64")
      {
        True -> decode.success(kid)
        False ->
          decode.failure(
            "",
            "configured EdDSA header without key URLs/extensions",
          )
      }
    })
    |> result.replace_error(domain.Unauthenticated),
  )
  use key <- result.try(
    dict.get(settings.keys, kid) |> result.replace_error(domain.Unauthenticated),
  )
  use bytes <- result.try(
    signature(key, token) |> result.replace_error(domain.Unauthenticated),
  )
  use p <- result.try(
    json.parse(bytes, payload()) |> result.replace_error(domain.Unauthenticated),
  )
  case True {
    _ if now >= p.expires -> Error(domain.TokenExpired)
    _ if p.issuer != settings.issuer -> Error(domain.Unauthenticated)
    _
      if p.issued > now + settings.skew
      || now - p.issued > settings.max_age + settings.skew
      || p.expires <= p.issued
      || p.expires - p.issued > settings.max_lifetime
    -> Error(domain.Unauthenticated)
    _ ->
      case list.contains(p.audiences, settings.audience), p.not_before {
        False, _ -> Error(domain.Unauthenticated)
        _, Some(nbf) if nbf > now + settings.skew ->
          Error(domain.Unauthenticated)
        _, _ -> Ok(Claims(p.subject, p.tenant, p.expires, p.grants))
      }
  }
}

pub fn authorize(
  claims: Claims,
  tenant: Tenant,
  stream: Stream,
  action: Action,
  now: Int,
) -> Result(Nil, Error) {
  case now >= claims.expires_at {
    True -> Error(domain.TokenExpired)
    False ->
      case
        claims.tenant == tenant
        && list.any(claims.grants, fn(g) {
          g.stream == stream && list.contains(g.actions, action)
        })
      {
        True -> Ok(Nil)
        False -> Error(domain.Forbidden)
      }
  }
}
