import gleam/erlang/process
import highway/observability as logging
import highway/operations
import highway/registry
import highway/runtime

pub type Signal {
  Stop
}

/// Replaces OTP's immediate SIGTERM handler with a single typed notification.
@external(erlang, "highway_lifecycle_ffi", "install")
pub fn install(subject: process.Subject(Signal)) -> Result(Nil, String)

@external(erlang, "highway_lifecycle_ffi", "exit")
pub fn exit(code: Int) -> Nil

pub type Outcome {
  Drained
  TimedOut
}

pub fn drain(registry: registry.Registry, timeout_ms: Int) -> Outcome {
  let deadline = runtime.monotonic_ms() + timeout_ms
  logging.log(logging.Notice, "Drain started; admission closed")
  operations.begin_drain(registry.context(registry))
  wait(registry, deadline, False)
}

fn wait(r: registry.Registry, deadline: Int, begun: Bool) -> Outcome {
  case runtime.monotonic_ms() >= deadline {
    True -> {
      operations.add(registry.context(r), operations.DrainTimeout, 1)
      logging.log(
        logging.Error,
        "Drain deadline expired; unconfirmed tail remains uncertain",
      )
      TimedOut
    }
    False -> {
      let begun = begun || registry.begin_drain(r) == Ok(Nil)
      let complete = case begun {
        False -> False
        True ->
          case registry.inspect(r) {
            Ok(s) -> s.drain_complete
            Error(_) -> False
          }
      }
      case complete && runtime.monotonic_ms() < deadline {
        True -> {
          operations.add(registry.context(r), operations.DrainCompleted, 1)
          logging.log(
            logging.Notice,
            "Drain completed; shutdown barrier confirmed",
          )
          Drained
        }
        False -> {
          process.sleep(10)
          wait(r, deadline, begun)
        }
      }
    }
  }
}
