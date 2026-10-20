import gleam/json
import gleam/string
import highway/domain
import highway/protocol

pub fn canonical_lossless_cursor_fixture_test() {
  let c = domain.Cursor("abc", 9_007_199_254_740_993, 9_007_199_254_740_995)
  assert domain.cursor_string(c) == "v1:abc:9007199254740993:9007199254740995"
  assert domain.cursor(domain.cursor_string(c)) == Ok(c)
  assert domain.cursor("v2:abc:1:1") == Error(domain.CursorInvalid)
  assert domain.cursor("v1:abc:01:1") == Error(domain.CursorInvalid)
  assert domain.cursor("v1:abc:1:-1") == Error(domain.CursorInvalid)
  assert domain.cursor("v1:abc:0:0") == Error(domain.CursorInvalid)
}

pub fn explicit_command_boundaries_test() {
  assert protocol.decode("{}") == Error(domain.InvalidFrame)
  assert protocol.decode("{\"type\":\"wat\"}") == Error(domain.UnknownCommand)
  assert protocol.decode("{\"type\":\"publish\",\"version\":2}")
    == Error(domain.UnsupportedVersion)
  assert protocol.decode(
      "{\"type\":\"publish\",\"id\":\"p\",\"stream\":\"events\",\"message_id\":\"m\"}",
    )
    == Error(domain.InvalidFrame)
  assert protocol.decode(
      "{\"type\":\"subscribe\",\"id\":\"s\",\"subscription\":\"s\",\"stream\":\"events\",\"window\":0}",
    )
    == Error(domain.InvalidFrame)
  let assert Ok(protocol.Subscribe(
    "s",
    "s",
    domain.Stream("events"),
    protocol.Latest,
    True,
    128,
  )) =
    protocol.decode(
      "{\"type\":\"subscribe\",\"id\":\"s\",\"subscription\":\"s\",\"stream\":\"events\"}",
    )
}

pub fn nested_payload_and_response_encoding_test() {
  let assert Ok(protocol.Publish(_, _, _, data)) =
    protocol.decode(
      "{\"type\":\"publish\",\"id\":\"p\",\"stream\":\"events\",\"message_id\":\"m\",\"data\":[true,null,{\"nested\":[1,2.5,\"hi\"]}]}",
    )
  let e = domain.Event(domain.Cursor("abc", 1, 1), "m", data, 100)
  let frame =
    protocol.encode(protocol.EventMessage("sub", domain.Stream("events"), e))
  assert string.contains(frame, "\"nested\"")
  assert string.contains(
    protocol.encode(protocol.Accepted("p", domain.Stream("events"), e)),
    "\"accepted\"",
  )
  assert !string.contains(protocol.encode(protocol.Hello(5000)), "persisted")
  assert json.to_string(data) == "[true,null,{\"nested\":[1,2.5,\"hi\"]}]"
}
