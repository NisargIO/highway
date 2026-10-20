import gleam/erlang/process
import gleam/option.{None, Some}
import gleam/otp/static_supervisor as supervisor
import gleam/otp/supervision
import gleam/string
import highway/config
import highway/http
import highway/registry
import highway/runtime
import highway/storage/s3
import logging
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
  let listener =
    mist.new(http.handle(_, config, registry, slots))
    |> mist.bind(config.bind)
    |> mist.port(config.port)
  case
    supervisor.new(supervisor.OneForAll)
    |> supervisor.add(
      supervision.worker(fn() {
        registry.start_storage(registration, config.limits, store, prefix)
      }),
    )
    |> supervisor.add(mist.supervised(listener))
    |> supervisor.start
  {
    Ok(_) -> {
      logging.log(logging.Warning, case config.storage {
        config.DevelopmentMemory ->
          "Explicit development memory mode: ephemeral; no persistence or history"
        config.S3(_) ->
          "S3 archival enabled: acceptance precedes durability; one server per prefix"
      })
      process.sleep_forever()
    }
    Error(reason) -> {
      let message = "Service failed to start: " <> string.inspect(reason)
      panic as message
    }
  }
}
