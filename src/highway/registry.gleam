import gleam/dict.{type Dict}
import gleam/erlang/process.{type Name, type Subject}
import gleam/json
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/factory_supervisor as factory
import gleam/otp/supervision
import gleam/result
import gleam/string
import highway/config.{type Limits}
import highway/domain.{
  type Cursor, type Error, type Event, type Stream, type Tenant,
}
import highway/owner.{type Owner}
import highway/replay
import highway/runtime
import highway/storage.{type Store}
import highway/storage/archive
import highway/stream
import logging

pub opaque type Registry {
  Registry(subject: Subject(Message), gate: runtime.Slots)
}

pub opaque type Registration {
  Registration(name: Name(Message), gate: runtime.Slots)
}

type Message {
  Request(runtime.Permit, Operation)
}

type Operation {
  Shutdown(Subject(Nil))
  Publish(Tenant, Stream, String, json.Json, Subject(Result(Event, Error)))
  Find(Tenant, Stream, Subject(Result(#(Owner, Cursor), Error)))
  Ready(Subject(Bool))
  Replayer(Subject(replay.Launcher))
}

type State {
  State(
    owners: Dict(#(Tenant, Stream), Owner),
    factory: factory.Supervisor(#(Tenant, Stream, Bool), Owner),
    replayer: replay.Launcher,
  )
}

pub fn registration() -> Registration {
  Registration(process.new_name("highway_registry"), runtime.slots())
}

pub fn reference(registration: Registration) -> Registry {
  Registry(process.named_subject(registration.name), registration.gate)
}

pub fn start(
  registration: Registration,
  limits: Limits,
) -> actor.StartResult(Registry) {
  start_storage(registration, limits, None, "")
}

pub fn start_storage(
  registration: Registration,
  limits: Limits,
  store: Option(Store),
  prefix: String,
) -> actor.StartResult(Registry) {
  runtime.reset(registration.gate)
  actor.new_with_initialiser(1000, fn(_) {
    let slots = runtime.slots()
    use workers <- result.try(
      factory.worker_child(fn(args: #(Tenant, Stream, Bool)) {
        owner.start_storage(
          limits,
          store,
          archive.scope(prefix, args.0, args.1),
          args.2,
          slots,
        )
      })
      |> factory.restart_strategy(supervision.Temporary)
      |> factory.start
      |> result.map_error(string.inspect),
    )
    use readers <- result.try(
      replay.start(store, prefix, limits, slots)
      |> result.map_error(fn(_) { "replay supervisor startup failed" }),
    )
    actor.initialised(State(dict.new(), workers.data, readers.data)) |> Ok
  })
  |> actor.named(registration.name)
  |> actor.on_message(fn(state, request) {
    let Request(permit, msg) = request
    runtime.release_permit(registration.gate, permit)
    let state =
      State(
        ..state,
        owners: dict.filter(state.owners, fn(_, o) { owner.alive(o) }),
      )
    let state = case msg {
      Publish(t, s, _, _, _) | Find(t, s, _) -> prune_missing(state, #(t, s))
      _ -> state
    }
    case msg {
      Replayer(reply) -> {
        process.send(reply, state.replayer)
        actor.continue(state)
      }
      Shutdown(reply) -> {
        process.send(reply, Nil)
        actor.stop()
      }
      Publish(tenant, stream, id, data, reply) -> {
        let key = #(tenant, stream)
        case
          pending_bytes(state) + stream.event_bytes(id, data)
          > limits.node_bytes
        {
          True -> {
            process.send(reply, Error(domain.StorageBacklog))
            actor.continue(state)
          }
          False ->
            case get_or_start(state, key, True, limits) {
              Error(e) -> {
                process.send(reply, Error(e))
                actor.continue(state)
              }
              Ok(o) -> {
                let outcome = owner.append(o, id, data, runtime.monotonic_ms())
                let owners = case
                  outcome,
                  store,
                  dict.has_key(state.owners, key)
                {
                  Error(_), None, False -> {
                    stop_owner(o)
                    state.owners
                  }
                  _, _, _ -> dict.insert(state.owners, key, o)
                }
                process.send(reply, outcome)
                actor.continue(State(..state, owners: owners))
              }
            }
        }
      }
      Find(tenant, stream, reply) -> {
        let key = #(tenant, stream)
        let found = case dict.get(state.owners, key), store {
          Ok(o), _ -> Ok(o)
          Error(_), Some(_) -> get_or_start(state, key, False, limits)
          _, _ -> Error(domain.StreamNotFound)
        }
        case found {
          Error(e) -> {
            process.send(reply, Error(e))
            actor.continue(state)
          }
          Ok(o) -> {
            let result = owner.watermark(o)
            let owners = case result {
              Error(domain.StreamNotFound) -> {
                stop_owner(o)
                dict.delete(state.owners, key)
              }
              _ -> dict.insert(state.owners, key, o)
            }
            process.send(reply, result |> result.map(fn(c) { #(o, c) }))
            actor.continue(State(..state, owners: owners))
          }
        }
      }
      Ready(reply) -> {
        let active = dict.filter(state.owners, fn(_, o) { !owner.missing(o) })
        let health_state = State(..state, owners: active)
        let healthy =
          store != None
          && dict.size(active) > 0
          && pending_bytes(health_state) < limits.node_bytes
          && dict.fold(active, True, fn(ok, _, o) { ok && owner.stats(o).1 })
        process.send(reply, healthy)
        actor.continue(state)
      }
    }
  })
  |> actor.start
  |> result.map(fn(s) { actor.Started(..s, data: reference(registration)) })
}

fn pending_bytes(s: State) -> Int {
  dict.fold(s.owners, 0, fn(n, _, o) { n + owner.stats(o).0 })
}

fn get_or_start(
  s: State,
  key: #(Tenant, Stream),
  create: Bool,
  limits: Limits,
) -> Result(Owner, Error) {
  case dict.get(s.owners, key) {
    Ok(o) -> Ok(o)
    Error(_) ->
      case dict.size(s.owners) >= limits.max_streams {
        True -> Error(domain.RateLimited)
        False ->
          factory.start_child(s.factory, #(key.0, key.1, create))
          |> result.map(fn(s) { s.data })
          |> result.map_error(fn(_) {
            logging.log(logging.Error, "Stream owner failed to start")
            domain.Unavailable
          })
      }
  }
}

pub fn publish(
  registry: Registry,
  tenant: Tenant,
  stream: Stream,
  id: String,
  data: json.Json,
) -> Result(Event, Error) {
  use permit <- result.try(
    runtime.permit(registry.gate) |> result.replace_error(domain.RateLimited),
  )
  runtime.call(registry.subject, 2000, fn(reply) {
    Request(permit, Publish(tenant, stream, id, data, reply))
  })
  |> result.map_error(log_call_error)
  |> result.unwrap(Error(domain.Unavailable))
}

pub fn find(
  registry: Registry,
  tenant: Tenant,
  stream: Stream,
) -> Result(#(Owner, Cursor), Error) {
  use permit <- result.try(
    runtime.permit(registry.gate) |> result.replace_error(domain.RateLimited),
  )
  runtime.call(registry.subject, 2000, fn(reply) {
    Request(permit, Find(tenant, stream, reply))
  })
  |> result.map_error(log_call_error)
  |> result.unwrap(Error(domain.Unavailable))
}

pub fn ready(registry: Registry) -> Bool {
  case runtime.permit(registry.gate) {
    Error(_) -> False
    Ok(permit) ->
      runtime.call(registry.subject, 100, fn(reply) {
        Request(permit, Ready(reply))
      })
      |> result.unwrap(False)
  }
}

fn log_call_error(reason: String) -> Error {
  logging.log(logging.Warning, "Registry request failed: " <> reason)
  domain.Unavailable
}

pub fn stop(registry: Registry) -> Nil {
  case runtime.permit(registry.gate) {
    Error(_) ->
      logging.log(logging.Warning, "Registry shutdown admission unavailable")
    Ok(permit) ->
      case
        runtime.call(registry.subject, 1000, fn(reply) {
          Request(permit, Shutdown(reply))
        })
      {
        Ok(_) -> Nil
        Error(reason) -> {
          let _ = log_call_error(reason)
          Nil
        }
      }
  }
}

fn prune_missing(s: State, keep: #(Tenant, Stream)) -> State {
  State(
    ..s,
    owners: dict.filter(s.owners, fn(key, o) {
      case key != keep && owner.missing(o) {
        True -> {
          case owner.stop(o) {
            Ok(_) -> Nil
            Error(_) ->
              logging.log(logging.Warning, "Missing owner cleanup failed")
          }
          False
        }
        False -> True
      }
    }),
  )
}

fn stop_owner(o: Owner) -> Nil {
  case owner.stop(o) {
    Ok(_) -> Nil
    Error(e) ->
      logging.log(logging.Warning, "Owner cleanup failed: " <> domain.code(e))
  }
}

pub fn replayer(reg: Registry) -> Result(replay.Launcher, Error) {
  use permit <- result.try(
    runtime.permit(reg.gate) |> result.replace_error(domain.RateLimited),
  )
  runtime.call(reg.subject, 1000, fn(reply) { Request(permit, Replayer(reply)) })
  |> result.map_error(log_call_error)
}
