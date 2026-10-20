import gleam/erlang/process
import gleam/option.{None, Some}
import gleam/otp/static_supervisor as supervisor
import gleam/otp/supervision
import gleam/string
import highway/config
import highway/http
import highway/lifecycle
import highway/observability as logging
import highway/operations
import highway/registry
import highway/runtime
import highway/storage/s3
import mist

pub fn main() -> Nil {
  logging.configure()
  let config = case config.load() {
    Ok(config) -> config
    Error(reason) -> panic as reason
  }
  let assert Ok(_) = config.validate(config.limits)
  let #(store, prefix) = case config.storage {
    config.DevelopmentMemory -> #(None, "")
    config.S3(settings) -> #(Some(s3.store(settings)), settings.prefix)
  }
  let registration = registry.registration()
  let registry = registry.reference(registration)
  let slots = runtime.slots()
  let signals = process.new_subject()
  let assert Ok(_) = lifecycle.install(signals)
  let listener =
    mist.new(http.handle(_, config, registry, slots))
    |> mist.bind(config.bind)
    |> mist.port(config.port)
  case
    supervisor.new(supervisor.OneForAll)
    |> supervisor.add(
      supervision.worker(fn() {
        registry.start_operational(
          registration,
          config.limits,
          store,
          prefix,
          config.operational,
        )
      }),
    )
    |> supervisor.add(mist.supervised(listener))
    |> supervisor.start
  {
    Ok(_) -> {
      logging.log(logging.Warning, case config.storage {
        config.DevelopmentMemory ->
          "Explicit development memory mode: ephemeral; never emits persisted"
        config.S3(_) ->
          "S3 archival enabled: acceptance precedes durability; one server per prefix"
      })
      let lifecycle.Stop = process.receive_forever(signals)
      let result = lifecycle.drain(registry, config.operational.drain_ms)
      // Keep sockets alive briefly to send confirmed persistence progress, then
      // close them with 1012 so clients reconnect after the singleton upgrade.
      process.sleep(config.limits.tick_ms + 100)
      operations.finish_drain(registry.context(registry))
      process.sleep(config.limits.tick_ms + 100)
      lifecycle.exit(case result {
        lifecycle.Drained -> 0
        lifecycle.TimedOut -> 1
      })
    }
    Error(reason) -> {
      let message = "Service failed to start: " <> string.inspect(reason)
      panic as message
    }
  }
}
