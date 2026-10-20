import gleam/dict.{type Dict}
import gleam/erlang/process.{type Name, type Subject}
import gleam/json
import gleam/list
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
import highway/health
import highway/observability as logging
import highway/operations
import highway/owner.{type Owner}
import highway/replay
import highway/runtime
import highway/storage.{type Store}
import highway/storage/archive
import highway/stream

pub opaque type Registry {
  Registry(
    subject: Subject(Message),
    gate: runtime.Slots,
    context: operations.Context,
  )
}

pub opaque type Registration {
  Registration(
    name: Name(Message),
    gate: runtime.Slots,
    context: operations.Context,
  )
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
  BeginDrain(Subject(Result(Nil, Error)))
  Inspect(Subject(Snapshot))
}

type State {
  State(
    owners: Dict(#(Tenant, Stream), Owner),
    factory: factory.Supervisor(#(Tenant, Stream, Bool), Owner),
    replayer: replay.Launcher,
    drain_owners: Option(List(Owner)),
    drain_failed: Bool,
  )
}

pub fn registration() -> Registration {
  Registration(
    process.new_name("highway_registry"),
    runtime.slots(),
    operations.new(),
  )
}

pub fn reference(registration: Registration) -> Registry {
  Registry(
    process.named_subject(registration.name),
    registration.gate,
    registration.context,
  )
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
  start_operational(
    registration,
    limits,
    store,
    prefix,
    config.operational_defaults(),
  )
}

pub fn start_operational(
  registration: Registration,
  limits: Limits,
  store: Option(Store),
  prefix: String,
  settings: config.Operational,
) -> actor.StartResult(Registry) {
  let context = registration.context
  let store = option.map(store, operations.measured(context, _))
  runtime.reset(registration.gate)
  actor.new_with_initialiser(1000, fn(_) {
    let slots = runtime.slots()
    use workers <- result.try(
      factory.worker_child(fn(args: #(Tenant, Stream, Bool)) {
        owner.start_operational(
          limits,
          store,
          archive.scope(prefix, args.0, args.1),
          args.2,
          slots,
          Some(context),
        )
      })
      |> factory.restart_strategy(supervision.Temporary)
      |> factory.start
      |> result.map_error(string.inspect),
    )
    use readers <- result.try(
      replay.start_operational(store, prefix, limits, slots, Some(context))
      |> result.map_error(fn(_) { "replay supervisor startup failed" }),
    )
    use _ <- result.try(
      health.start(context, store, prefix, settings.probe_ms)
      |> result.map_error(fn(_) { "health worker startup failed" }),
    )
    actor.initialised(State(dict.new(), workers.data, readers.data, None, False))
    |> Ok
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
      BeginDrain(reply) -> {
        let owners = case state.drain_owners {
          Some(owners) -> owners
          None -> dict.values(state.owners)
        }
        // Mailbox saturation is transient: retry the same idempotent barrier
        // until the lifecycle deadline, without replacing any owner process.
        let admitted =
          list.fold(owners, True, fn(ok, o) {
            owner.begin_drain(o) == Ok(Nil) && ok
          })
        let failed = list.any(owners, fn(o) { !owner.alive(o) })
        process.send(reply, case admitted {
          True -> Ok(Nil)
          False -> Error(domain.RateLimited)
        })
        actor.continue(
          State(
            ..state,
            drain_owners: Some(owners),
            drain_failed: state.drain_failed || failed,
          ),
        )
      }
      Inspect(reply) -> {
        process.send(reply, snapshot(state))
        actor.continue(state)
      }
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
        case operations.draining(context) {
          True -> {
            operations.add(context, operations.PublishRejected, 1)
            process.send(reply, Error(domain.Unavailable))
            actor.continue(state)
          }
          False ->
            case
              pending_bytes(state) + stream.event_bytes(id, data)
              > limits.node_bytes
            {
              True -> {
                operations.add(context, operations.PublishRejected, 1)
                process.send(reply, Error(domain.StorageBacklog))
                actor.continue(state)
              }
              False ->
                case get_or_start(state, key, True, limits) {
                  Error(e) -> {
                    operations.add(context, operations.PublishRejected, 1)
                    process.send(reply, Error(e))
                    actor.continue(state)
                  }
                  Ok(o) -> {
                    let outcome =
                      owner.append(o, id, data, runtime.monotonic_ms())
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
                    case outcome {
                      Error(_) ->
                        operations.add(context, operations.PublishRejected, 1)
                      _ -> Nil
                    }
                    process.send(reply, outcome)
                    actor.continue(State(..state, owners: owners))
                  }
                }
            }
        }
      }
      Find(tenant, stream, reply) -> {
        let key = #(tenant, stream)
        let found = case operations.draining(context) {
          True -> Error(domain.Unavailable)
          False ->
            case dict.get(state.owners, key), store {
              Ok(o), _ -> Ok(o)
              Error(_), Some(_) -> get_or_start(state, key, False, limits)
              _, _ -> Error(domain.StreamNotFound)
            }
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
          && !operations.draining(context)
          && operations.storage_ready(
            context,
            runtime.monotonic_ms(),
            settings.stale_ms,
          )
          && operations.active(context, operations.Connection)
          < limits.max_connections
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
  use _ <- result.try(case operations.draining(registry.context) {
    True -> {
      operations.add(registry.context, operations.PublishRejected, 1)
      Error(domain.Unavailable)
    }
    False -> Ok(Nil)
  })
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

pub type Snapshot {
  Snapshot(
    owners: Int,
    pending: Int,
    unhealthy: Int,
    drain_started: Bool,
    drain_complete: Bool,
    drain_failed: Bool,
  )
}

fn snapshot(s: State) -> Snapshot {
  let all = case s.drain_owners {
    Some(owners) -> owners
    None -> dict.values(s.owners)
  }
  let pending = list.fold(all, 0, fn(n, o) { n + owner.stats(o).0 })
  let failed = s.drain_failed || list.any(all, fn(o) { !owner.alive(o) })
  Snapshot(
    list.length(all),
    pending,
    list.fold(all, 0, fn(n, o) {
      n
      + case owner.stats(o).1 && owner.alive(o) {
        True -> 0
        False -> 1
      }
    }),
    s.drain_owners != None,
    s.drain_owners != None && !failed && list.all(all, owner.drained),
    failed,
  )
}

pub fn context(r: Registry) -> operations.Context {
  r.context
}

pub fn draining(r: Registry) -> Bool {
  operations.draining(r.context)
}

/// Latch first, then serialize a barrier behind all outstanding admissions.
pub fn begin_drain(r: Registry) -> Result(Nil, Error) {
  operations.begin_drain(r.context)
  use permit <- result.try(
    runtime.permit(r.gate) |> result.replace_error(domain.RateLimited),
  )
  runtime.call(r.subject, 100, fn(reply) { Request(permit, BeginDrain(reply)) })
  |> result.unwrap(Error(domain.Unavailable))
}

pub fn inspect(r: Registry) -> Result(Snapshot, Error) {
  use permit <- result.try(
    runtime.permit(r.gate) |> result.replace_error(domain.RateLimited),
  )
  runtime.call(r.subject, 100, fn(reply) { Request(permit, Inspect(reply)) })
  |> result.replace_error(domain.Unavailable)
}

pub fn metrics(r: Registry) -> String {
  let state = inspect(r)
  operations.render(r.context)
  <> operations.gauge("beam_memory_bytes", runtime.memory_bytes())
  <> case state {
    Ok(s) ->
      operations.gauge("owners_active", s.owners)
      <> operations.gauge("pending_bytes", s.pending)
      <> operations.gauge("owners_unhealthy", s.unhealthy)
    Error(_) -> operations.gauge("registry_available", 0)
  }
  <> operations.gauge("ready", case ready(r) {
    True -> 1
    False -> 0
  })
}
