import gleam/json
import gleam/string
import highway/auth
import highway/domain
import highway/test_support as support

pub fn real_signature_and_exact_capabilities_test() {
  let key = support.key()
  assert auth.valid_key(support.public(key)) == Ok(Nil)
  let assert Error(_) = auth.valid_key("{\"kty\":\"oct\",\"k\":\"c2VjcmV0\"}")
  let token = support.token(key, ["publish", "subscribe"])
  let assert Ok(claims) = auth.verify(support.settings(key), token, 1000)
  assert auth.authorize(
      claims,
      domain.Tenant("tenant"),
      domain.Stream("events"),
      domain.Publish,
      1000,
    )
    == Ok(Nil)
  assert auth.authorize(
      claims,
      domain.Tenant("other"),
      domain.Stream("events"),
      domain.Publish,
      1000,
    )
    == Error(domain.Forbidden)
  assert auth.authorize(
      claims,
      domain.Tenant("tenant"),
      domain.Stream("events/child"),
      domain.Publish,
      1000,
    )
    == Error(domain.Forbidden)
  assert auth.authorize(
      claims,
      domain.Tenant("tenant"),
      domain.Stream("events"),
      domain.History,
      1000,
    )
    == Error(domain.Forbidden)
  assert auth.authorize(
      claims,
      domain.Tenant("tenant"),
      domain.Stream("events"),
      domain.Subscribe,
      1100,
    )
    == Error(domain.TokenExpired)
}

pub fn signature_key_and_algorithm_confusion_test() {
  let key = support.key()
  let token = support.token(key, ["publish"])
  assert auth.verify(support.settings(support.key()), token, 1000)
    == Error(domain.Unauthenticated)
  let payload =
    support.payload("tenant", "user", 900, 1100, ["publish"]) |> json.to_string
  let unknown =
    support.sign(key, "{\"alg\":\"EdDSA\",\"kid\":\"unknown\"}", payload)
  assert auth.verify(support.settings(key), unknown, 1000)
    == Error(domain.Unauthenticated)
  // Changing the protected algorithm invalidates the signature and must never
  // select an HMAC verification path, even with an asymmetric key configured.
  let parts = string.split(token, ".")
  let assert [_, body, signature] = parts
  let confused =
    "eyJhbGciOiJIUzI1NiIsImtpZCI6InRlc3QifQ." <> body <> "." <> signature
  assert auth.verify(support.settings(key), confused, 1000)
    == Error(domain.Unauthenticated)
  let none = "eyJhbGciOiJub25lIiwia2lkIjoidGVzdCJ9." <> body <> "."
  assert auth.verify(support.settings(key), none, 1000)
    == Error(domain.Unauthenticated)
  let remote =
    support.sign(
      key,
      "{\"alg\":\"EdDSA\",\"kid\":\"test\",\"jku\":\"https://evil.example/keys\"}",
      payload,
    )
  assert auth.verify(support.settings(key), remote, 1000)
    == Error(domain.Unauthenticated)
}

pub fn timing_issuer_audience_and_claim_decoding_test() {
  let key = support.key()
  let settings = support.settings(key)
  assert auth.verify(settings, support.token(key, []), 1100)
    == Error(domain.TokenExpired)
  assert auth.verify(
      settings,
      support.token_for(key, "tenant", "user", 1006, 1100, []),
      1000,
    )
    == Error(domain.Unauthenticated)
  assert auth.verify(
      settings,
      support.token_for(key, "tenant", "user", 699, 1001, []),
      1000,
    )
    == Error(domain.Unauthenticated)
  assert auth.verify(
      settings,
      support.token_for(key, "tenant", "user", 999, 1300, []),
      1000,
    )
    == Error(domain.Unauthenticated)
  let payload =
    support.payload("tenant", "user", 900, 1100, ["unknown"]) |> json.to_string
  let bad = support.sign(key, "{\"alg\":\"EdDSA\",\"kid\":\"test\"}", payload)
  assert auth.verify(settings, bad, 1000) == Error(domain.Unauthenticated)
  let payload =
    support.payload("tenant", "user", 900, 1100, []) |> json.to_string
  let wrong_issuer =
    support.sign(
      key,
      "{\"alg\":\"EdDSA\",\"kid\":\"test\"}",
      string.replace(payload, "issuer", "wrong"),
    )
  assert auth.verify(settings, wrong_issuer, 1000)
    == Error(domain.Unauthenticated)
  let wrong_aud =
    support.sign(
      key,
      "{\"alg\":\"EdDSA\",\"kid\":\"test\"}",
      string.replace(payload, "highway", "wrong"),
    )
  assert auth.verify(settings, wrong_aud, 1000) == Error(domain.Unauthenticated)
  let nbf = "{\"nbf\":1006," <> string.drop_start(payload, 1)
  assert auth.verify(
      settings,
      support.sign(key, "{\"alg\":\"EdDSA\",\"kid\":\"test\"}", nbf),
      1000,
    )
    == Error(domain.Unauthenticated)
}
