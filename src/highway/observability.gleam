/// Service log fields are safe operation context. Never pass tokens, payloads,
/// keys, requests, or session state. OTP reports use a redacting formatter.
import gleam/json
import highway/runtime

pub type LogLevel {
  Emergency
  Alert
  Critical
  Error
  Warning
  Notice
  Info
  Debug
}

@external(erlang, "highway_observability_ffi", "configure")
pub fn configure() -> Nil

@external(erlang, "highway_observability_ffi", "write")
fn write(level: LogLevel, json: String) -> Nil

pub fn log(level: LogLevel, message: String) -> Nil {
  write(
    level,
    json.to_string(
      json.object([
        #("event", json.string("service")),
        #(
          "level",
          json.string(case level {
            Emergency -> "emergency"
            Alert -> "alert"
            Critical -> "critical"
            Error -> "error"
            Warning -> "warning"
            Notice -> "notice"
            Info -> "info"
            Debug -> "debug"
          }),
        ),
        #("time", json.int(runtime.now())),
        #("message", json.string(message)),
      ]),
    ),
  )
}
