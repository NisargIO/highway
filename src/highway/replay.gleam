/// Temporary, supervised, one-step replay jobs. A connection has at most one
/// in flight; the factory additionally bounds concurrent jobs across the node.
import exception
import gleam/erlang/process.{type Pid, type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/factory_supervisor as factory
import gleam/otp/supervision
import gleam/result
import gleam/string
import highway/auth.{type Claims}
import highway/config.{type Limits}
import highway/domain.{type Cursor, type Error, type Stream}
import highway/observability as logging
import highway/operations
import highway/owner.{type Owner}
import highway/protocol
import highway/replay/read
import highway/replay/view.{type View}
import highway/runtime
import highway/storage.{type Store}
import highway/storage/archive

pub type Input {
  Input(
    subscription: String,
    generation: String,
    owner: Owner,
    view: View,
    claims: Claims,
    stream: Stream,
    after: Cursor,
    target: Cursor,
    scan: Option(read.Scan),
    count: Int,
    bytes: Int,
    now: Int,
    connection: Pid,
    reply: Subject(Reply),
  )
}

pub type Reply {
  Reply(
    subscription: String,
    generation: String,
    result: Result(read.Step, Error),
  )
}

pub opaque type Job {
  Job(pid: Pid, generation: String)
}

pub opaque type Launcher {
  Launcher(factory: factory.Supervisor(Input, Pid))
}

type Work {
  Work
}

type Down {
  Cancel
  Finished
}

pub fn start(
  store: Option(Store),
  prefix: String,
  limits: Limits,
  io_slots: runtime.Slots,
) -> actor.StartResult(Launcher) {
  start_operational(store, prefix, limits, io_slots, None)
}

pub fn start_operational(
  store: Option(Store),
  prefix: String,
  limits: Limits,
  io_slots: runtime.Slots,
  metrics: Option(operations.Context),
) -> actor.StartResult(Launcher) {
  let slots = runtime.slots()
  factory.worker_child(fn(input) {
    start_job(input, store, prefix, limits, slots, io_slots, metrics)
  })
  |> factory.restart_strategy(supervision.Temporary)
  |> factory.start
  |> result.map(fn(s) { actor.Started(..s, data: Launcher(s.data)) })
}

pub fn launch(launcher: Launcher, input: Input) -> Result(Job, Error) {
  case exception.rescue(fn() { factory.start_child(launcher.factory, input) }) {
    Ok(Ok(started)) -> Ok(Job(started.data, input.generation))
    Ok(Error(actor.InitFailed("replay_capacity"))) -> Error(domain.RateLimited)
    _ -> {
      logging.log(logging.Warning, "Replay worker startup failed")
      Error(domain.Unavailable)
    }
  }
}

pub fn generation(job: Job) -> String {
  job.generation
}

pub fn cancel(job: Job) -> Nil {
  runtime.shutdown(job.pid)
}

fn guardian(job: Pid, connection: Pid, owner: Pid) -> Nil {
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(process.monitor(job), fn(_) { Finished })
    |> process.select_specific_monitor(process.monitor(connection), fn(_) {
      Cancel
    })
    |> process.select_specific_monitor(process.monitor(owner), fn(_) { Cancel })
  case process.selector_receive_forever(selector) {
    Finished -> Nil
    Cancel -> process.kill(job)
  }
}

fn start_job(
  input: Input,
  store: Option(Store),
  prefix: String,
  limits: Limits,
  slots: runtime.Slots,
  io_slots: runtime.Slots,
  metrics: Option(operations.Context),
) -> actor.StartResult(Pid) {
  actor.new_with_initialiser(1000, fn(subject) {
    case runtime.acquire(slots, limits.replay_workers) {
      False -> Error("replay_capacity")
      True -> {
        case metrics {
          Some(c) -> operations.activity(c, operations.Replay, True)
          None -> Nil
        }
        let pid = process.self()
        let _ =
          process.spawn(fn() {
            guardian(pid, input.connection, owner.pid(input.owner))
          })
        process.send(subject, Work)
        actor.initialised(Nil) |> actor.returning(pid) |> Ok
      }
    }
  })
  |> actor.on_message(fn(_, _) {
    let outcome =
      exception.rescue(fn() {
        use <- exception.defer(fn() { runtime.release(slots, process.self()) })
        case runtime.acquire(io_slots, 16) {
          False -> Error(domain.RateLimited)
          True -> {
            use <- exception.defer(fn() {
              runtime.release(io_slots, process.self())
            })
            run(input, store, prefix, limits)
          }
        }
      })
      |> result.map_error(fn(_) {
        // Exceptions may contain decoded payloads. Preserve safe operation
        // context without inspecting exception values or actor input.
        logging.log(logging.Warning, "Replay worker execution failed")
        domain.Unavailable
      })
      |> result.unwrap(Error(domain.Unavailable))
    case metrics {
      Some(c) -> operations.activity(c, operations.Replay, False)
      None -> Nil
    }
    case outcome {
      Ok(read.Ready(batch)) ->
        operations.record(
          metrics,
          operations.ReplayItems,
          list.length(batch.items),
        )
      Error(_) -> operations.record(metrics, operations.ReplayErrors, 1)
      _ -> Nil
    }
    process.send(
      input.reply,
      Reply(input.subscription, input.generation, outcome),
    )
    actor.stop()
  })
  |> actor.start
}

fn authorize(input: Input, started: Int) -> Result(Nil, Error) {
  let now = input.now + { runtime.monotonic_ms() - started } / 1000
  use _ <- result.try(auth.authorize(
    input.claims,
    input.claims.tenant,
    input.stream,
    domain.Subscribe,
    now,
  ))
  use _ <- result.try(case input.view.historical {
    True ->
      auth.authorize(
        input.claims,
        input.claims.tenant,
        input.stream,
        domain.History,
        now,
      )
    False -> Ok(Nil)
  })
  use _ <- result.try(owner.pull(input.owner, input.view, input.after))
  case process.is_alive(input.connection) {
    True -> Ok(Nil)
    False -> Error(domain.Unavailable)
  }
}

fn checked(store: Store, input: Input, started: Int) -> Store {
  storage.Store(
    get: fn(key) {
      use _ <- result.try(
        authorize(input, started) |> result.map_error(storage.Access),
      )
      use object <- result.try(store.get(key))
      use _ <- result.try(
        authorize(input, started) |> result.map_error(storage.Access),
      )
      Ok(object)
    },
    put: fn(_, _, _) { Error(storage.Access(domain.Forbidden)) },
  )
}

fn run(
  input: Input,
  store: Option(Store),
  prefix: String,
  limits: Limits,
) -> Result(read.Step, Error) {
  let started = runtime.monotonic_ms()
  use _ <- result.try(authorize(input, started))
  use store <- result.try(case store {
    Some(s) -> Ok(checked(s, input, started))
    None -> Error(domain.Unavailable)
  })
  let scope = archive.scope(prefix, input.claims.tenant, input.stream)
  let outcome = {
    use scan <- result.try(case input.scan {
      Some(s) -> Ok(s)
      None -> read.start(store, scope, input.target, started)
    })
    use _ <- result.try(
      case
        runtime.monotonic_ms() - scan.started_ms >= limits.replay_timeout_ms
      {
        True -> Error(storage.Limit)
        False -> Ok(Nil)
      },
    )
    read.step(
      store,
      scope,
      scan,
      input.after,
      limits.replay_pages_per_step,
      limits.replay_max_pages,
      input.count,
      input.bytes,
      input.subscription,
      input.stream,
    )
  }
  use step <- result.try(
    outcome
    |> result.map_error(fn(e) {
      case e {
        storage.Access(cause) -> cause
        storage.Busy -> domain.RateLimited
        storage.Limit -> domain.ReplayLimit
        _ -> {
          logging.log(
            logging.Warning,
            "Replay storage read failed: " <> string.inspect(e),
          )
          domain.Unavailable
        }
      }
    }),
  )
  use _ <- result.try(authorize(input, started))
  case step {
    read.Missing -> {
      // The owner may have retained/moved the suffix during traversal. Never
      // turn a missing cache/archive range into caught_up or complete.
      use pull <- result.try(owner.pull(input.owner, input.view, input.after))
      case pull.next {
        view.Available(event) -> {
          let size =
            string.byte_size(
              protocol.encode(read.response(
                read.Record(event),
                input.subscription,
                input.stream,
              )),
            )
          case
            domain.at_or_before(event.cursor, input.target)
            && size <= input.bytes
            && input.count > 0
          {
            True ->
              Ok(read.Ready(read.Batch([read.Record(event)], event.cursor)))
            False -> Error(domain.SlowConsumer)
          }
        }
        _ -> Error(domain.Unavailable)
      }
    }
    _ -> Ok(step)
  }
}
