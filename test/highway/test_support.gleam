import gleam/dict
import gleam/json
import highway/auth
import highway/config

pub type Key

@external(erlang, "highway_test_crypto_ffi", "key")
pub fn key() -> Key

@external(erlang, "highway_test_crypto_ffi", "public")
pub fn public(key: Key) -> String

@external(erlang, "highway_test_crypto_ffi", "sign")
pub fn sign(key: Key, header: String, payload: String) -> String

pub fn settings(key: Key) -> auth.Settings {
  auth.Settings(
    "issuer",
    "highway",
    dict.from_list([#("test", public(key))]),
    300,
    300,
    5,
  )
}

pub fn config(key: Key) -> config.Config {
  config.Config(
    "127.0.0.1",
    4000,
    settings(key),
    ["http://localhost:3000"],
    config.defaults(),
    config.DevelopmentMemory,
  )
}

pub fn payload(
  tenant: String,
  subject: String,
  iat: Int,
  exp: Int,
  actions: List(String),
) -> json.Json {
  json.object([
    #("iss", json.string("issuer")),
    #("aud", json.string("highway")),
    #("sub", json.string(subject)),
    #("tenant", json.string(tenant)),
    #("iat", json.int(iat)),
    #("exp", json.int(exp)),
    #(
      "grants",
      json.array(
        [
          json.object([
            #("stream", json.string("events")),
            #("actions", json.array(actions, json.string)),
          ]),
        ],
        fn(x) { x },
      ),
    ),
  ])
}

pub fn token(key: Key, actions: List(String)) -> String {
  token_for(key, "tenant", "user", 900, 1100, actions)
}

pub fn token_for(
  key: Key,
  tenant: String,
  subject: String,
  iat: Int,
  exp: Int,
  actions: List(String),
) -> String {
  sign(
    key,
    "{\"alg\":\"EdDSA\",\"kid\":\"test\"}",
    json.to_string(payload(tenant, subject, iat, exp, actions)),
  )
}
