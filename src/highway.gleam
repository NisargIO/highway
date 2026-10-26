import gleam/erlang/process
import gleam/option.{None, Some}
import gleam/otp/static_supervisor as supervisor
import gleam/otp/supervision
import gleam/string
import highway/cluster
import highway/config
import highway/http
import highway/lifecycle
import highway/observability as logging
import highway/operations
import highway/registry
import highway/runtime
import highway/secrets
import highway/storage/s3
import mist

pub fn main() -> Nil {
  logging.configure()
  let config = case config.load() {
    Ok(config) -> config
    Error(reason) -> panic as reason
  }
  let assert Ok(_) = config.validate(config.limits)
  let cluster_settings = case config.load_cluster(config.storage) {
    Ok(c) -> c
    Error(reason) -> panic as reason
  }
  case cluster_settings {
    config.Standalone -> Nil
    config.Clustered(node: node, ..) -> {
      let cookie = case secrets.required("HIGHWAY_CLUSTER_COOKIE") {
        Ok(cookie) -> cookie
        Error(reason) -> panic as reason
      }
      case cluster.start(node, cookie) {
        Ok(_) -> Nil
        Error(reason) -> panic as reason
      }
    }
  }
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
        registry.start_cluster(
          registration,
          config.limits,
          store,
          prefix,
          config.operational,
          cluster_settings,
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
          case cluster_settings {
            config.Standalone ->
              "S3 archival enabled: acceptance precedes durability; one server per prefix"
            config.Clustered(node: node, region: region, ..) ->
              "S3 archival enabled in cluster mode as "
              <> node
              <> " ("
              <> region
              <> "): stream ownership uses head leases"
          }
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
