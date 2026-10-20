/// One guarded probe per node. The same private canary is read, conditionally
/// updated, and read back on every probe, testing both permissions while idle.
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/json
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string
import highway/observability as logging
import highway/operations
import highway/protocol
import highway/runtime
import highway/storage

pub type Tick {
  Tick
}

pub fn start(
  c: operations.Context,
  store: Option(storage.Store),
  prefix: String,
  interval: Int,
) -> actor.StartResult(process.Subject(Tick)) {
  let parent = process.self()
  let key = prefix <> "/health/" <> runtime.incarnation() <> ".json"
  operations.probe(c, False, runtime.monotonic_ms())
  actor.new_with_initialiser(1000, fn(subject) {
    let worker = process.self()
    let _ =
      process.spawn(fn() {
        let selector =
          process.new_selector()
          |> process.select_specific_monitor(process.monitor(parent), fn(_) {
            True
          })
          |> process.select_specific_monitor(process.monitor(worker), fn(_) {
            False
          })
        case process.selector_receive_forever(selector) {
          True -> process.kill(worker)
          False -> Nil
        }
      })
    process.send(subject, Tick)
    actor.initialised(#(None, subject)) |> actor.returning(subject) |> Ok
  })
  |> actor.on_message(fn(state, _) {
    let #(previous_error, subject) = state
    let outcome = case store {
      None -> Error(storage.NotFound)
      Some(store) -> probe(store, key)
    }
    let current_error = case outcome {
      Error(cause) -> {
        case store != None && previous_error != Some(cause) {
          True ->
            logging.log(
              logging.Warning,
              "Storage probe failed: " <> string.inspect(cause),
            )
          False -> Nil
        }
        Some(cause)
      }
      Ok(_) -> {
        case previous_error != None {
          True -> logging.log(logging.Info, "Storage probe recovered")
          False -> Nil
        }
        None
      }
    }
    operations.probe(c, outcome == Ok(Nil), runtime.monotonic_ms())
    let _ = process.send_after(subject, interval, Tick)
    actor.continue(#(current_error, subject))
  })
  |> actor.start
}

fn validate(body: String) -> Result(Nil, storage.Error) {
  json.parse(body, {
    use v <- decode.field("version", decode.int)
    use flag <- decode.field("probe", decode.bool)
    use _ <- decode.field("nonce", protocol.name())
    case v == 1 && flag {
      True -> decode.success(Nil)
      False -> decode.failure(Nil, "probe version 1")
    }
  })
  |> result.replace_error(storage.Corrupt("probe format"))
}

pub fn probe(store: storage.Store, key: String) -> Result(Nil, storage.Error) {
  use condition <- result.try(case store.get(key) {
    Error(storage.NotFound) -> Ok(storage.Absent)
    Error(e) -> Error(e)
    Ok(object) -> {
      use _ <- result.try(validate(object.body))
      Ok(storage.Match(object.etag))
    }
  })
  let body =
    json.to_string(
      json.object([
        #("version", json.int(1)),
        #("probe", json.bool(True)),
        #("nonce", json.string(runtime.incarnation())),
      ]),
    )
  let written = store.put(key, body, condition)
  // An exact fresh nonce is the only successful reconciliation after timeout.
  case store.get(key), written {
    Ok(object), _ if object.body == body -> Ok(Nil)
    Ok(_), Error(cause) -> Error(cause)
    Ok(_), Ok(_) -> Error(storage.Corrupt("probe readback"))
    Error(cause), _ -> Error(cause)
  }
}
