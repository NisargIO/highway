import gleam/dict
import gleam/erlang/process.{type Subject}
import gleam/json
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string
import highway/config.{type Limits}
import highway/domain.{type Cursor, type Error, type Event}
import highway/observability as logging
import highway/operations
import highway/protocol
import highway/replay/view
import highway/runtime
import highway/storage.{type Store}
import highway/storage/archive
import highway/storage/worker
import highway/stream

pub opaque type Owner {
  Owner(subject: Subject(Message), gate: runtime.Slots)
}

type Message {
  Request(runtime.Permit, Operation)
  Tick
  Storage(worker.Reply)
}

type Operation {
  Drain
  Capture(protocol.From, process.Pid, Subject(Result(view.View, Error)))
  Pull(String, Cursor, Subject(Result(view.Pull, Error)))
  Release(String)
  Stop(Subject(Result(Nil, Error)))
  Append(String, json.Json, Int, Subject(Result(Event, Error)))
  Watermark(Subject(Result(Cursor, Error)))
  Read(Cursor, Subject(Result(Option(Event), Error)))
}

type ArchiveState {
  Memory
  Starting
  Missing
  Opening
  Active(archive.Session)
  Writing(archive.Plan)
  Retrying(archive.Plan)
  Fenced
  Closing(archive.Closure)
  RetryClose(archive.Closure)
  Closed
}

type State {
  State(
    stream: stream.State,
    gate: runtime.Slots,
    subject: Subject(Message),
    replies: Subject(worker.Reply),
    writer: Option(Subject(worker.Request)),
    archive: ArchiveState,
    scope: String,
    retry_ms: Int,
    create: Bool,
    leases: dict.Dict(String, view.Lease),
    draining: Bool,
    metrics: Option(operations.Context),
  )
}

pub fn start(limits: Limits) -> actor.StartResult(Owner) {
  start_storage(limits, None, "", True, runtime.slots())
}

pub fn start_storage(
  limits: Limits,
  store: Option(Store),
  scope: String,
  create: Bool,
  slots: runtime.Slots,
) -> actor.StartResult(Owner) {
  start_operational(limits, store, scope, create, slots, None)
}

pub fn start_operational(
  limits: Limits,
  store: Option(Store),
  scope: String,
  create: Bool,
  slots: runtime.Slots,
  metrics: Option(operations.Context),
) -> actor.StartResult(Owner) {
  actor.new_with_initialiser(1000, fn(subject) {
    let gate = runtime.slots()
    let replies = process.new_subject()
    let initial =
      State(
        stream.new(runtime.incarnation()),
        gate,
        subject,
        replies,
        None,
        Memory,
        scope,
        0,
        create,
        dict.new(),
        False,
        metrics,
      )
    let initial = case store {
      None -> Ok(initial)
      Some(store) -> {
        use writer <- result.try(
          worker.start(store, slots, process.self())
          |> result.map_error(fn(_) { "storage worker startup failed" }),
        )
        process.send(writer.data, worker.Open(scope, create, replies))
        Ok(State(..initial, writer: Some(writer.data), archive: Opening))
      }
    }
    use initial <- result.try(initial)
    let _ = process.send_after(subject, 250, Tick)
    account(initial, limits)
    actor.initialised(initial)
    |> actor.selecting(
      process.new_selector()
      |> process.select(subject)
      |> process.select_map(replies, Storage),
    )
    |> actor.returning(#(subject, gate))
    |> Ok
  })
  |> actor.on_message(fn(state, msg) {
    case msg {
      Request(permit, op) -> {
        runtime.release_permit(state.gate, permit)
        case op {
          Drain -> {
            let next = drain_step(State(..state, draining: True))
            account(next, limits)
            actor.continue(next)
          }
          Capture(from, connection, reply) -> {
            let state = clean_leases(state)
            let captured = capture(state, from)
            case captured {
              Error(e) -> {
                process.send(reply, Error(e))
                actor.continue(state)
              }
              Ok(v) ->
                case dict.size(state.leases) >= 1024 {
                  True -> {
                    process.send(reply, Error(domain.RateLimited))
                    actor.continue(state)
                  }
                  False -> {
                    process.send(reply, Ok(v))
                    actor.continue(
                      State(
                        ..state,
                        leases: dict.insert(
                          state.leases,
                          v.id,
                          view.Lease(v, connection),
                        ),
                      ),
                    )
                  }
                }
            }
          }
          Release(id) -> {
            actor.continue(
              State(..state, leases: dict.delete(state.leases, id)),
            )
          }
          Pull(id, after, reply) -> {
            process.send(reply, pull_state(state, id, after))
            actor.continue(state)
          }

          Stop(reply) -> {
            case state.writer {
              None -> Nil
              Some(writer) -> {
                let assert Ok(pid) = process.subject_owner(writer)
                process.unlink(pid)
                process.kill(pid)
              }
            }
            process.send(reply, Ok(Nil))
            actor.stop()
          }
          Append(id, data, now, reply) ->
            case state.draining {
              True -> {
                runtime.release_bytes(state.gate, stream.event_bytes(id, data))
                process.send(reply, Error(domain.Unavailable))
                actor.continue(state)
              }
              False ->
                case state.archive {
                  Starting | Missing -> {
                    let next = send_open(state, True)
                    runtime.release_bytes(
                      state.gate,
                      stream.event_bytes(id, data),
                    )
                    process.send(reply, Error(domain.Unavailable))
                    actor.continue(next)
                  }
                  Opening | Fenced -> {
                    runtime.release_bytes(
                      state.gate,
                      stream.event_bytes(id, data),
                    )
                    process.send(reply, Error(domain.Unavailable))
                    actor.continue(state)
                  }
                  _ ->
                    case stream.append(state.stream, id, data, limits, now) {
                      Error(e) -> {
                        runtime.release_bytes(
                          state.gate,
                          stream.event_bytes(id, data),
                        )
                        process.send(reply, Error(e))
                        actor.continue(state)
                      }
                      Ok(#(next, event)) -> {
                        let next = State(..state, stream: next)
                        account(next, limits)
                        runtime.release_bytes(
                          state.gate,
                          stream.event_bytes(id, data),
                        )
                        operations.record(
                          next.metrics,
                          operations.AcceptedEvents,
                          1,
                        )
                        operations.record(
                          next.metrics,
                          operations.AcceptedBytes,
                          event.bytes,
                        )
                        process.send(reply, Ok(event))
                        actor.continue(case next.stream.bytes >= 1_048_576 {
                          True -> flush(next)
                          False -> next
                        })
                      }
                    }
                }
            }
          Watermark(reply) -> {
            let result = case state.archive {
              Starting | Opening | Fenced -> Error(domain.Unavailable)
              Missing -> Error(domain.StreamNotFound)
              _ -> Ok(stream.watermark(state.stream))
            }
            process.send(reply, result)
            actor.continue(state)
          }
          Read(c, reply) -> {
            process.send(reply, case state.archive {
              Fenced -> Error(domain.Unavailable)
              _ -> stream.next(state.stream, c)
            })
            actor.continue(state)
          }
        }
      }
      Tick -> {
        let _ = process.send_after(state.subject, 250, Tick)
        let state = clean_leases(state)
        let next = case state.archive {
          Starting -> send_open(state, state.create)
          RetryClose(plan) ->
            case runtime.monotonic_ms() >= state.retry_ms {
              True -> send_close(state, plan)
              False -> state
            }
          Retrying(plan) ->
            case runtime.monotonic_ms() >= state.retry_ms {
              True -> send_commit(state, plan)
              False -> state
            }
          Active(_) ->
            case state.draining {
              True -> drain_step(state)
              False -> flush(state)
            }
          _ -> state
        }
        account(next, limits)
        actor.continue(next)
      }
      Storage(worker.Closed(epoch, id, outcome)) -> {
        let next = case state.archive {
          Closing(plan)
            if plan.next.epoch == epoch && plan.next.commit_id == id
          ->
            case outcome {
              Ok(_) -> State(..state, archive: Closed)
              Error(storage.Conflict)
              | Error(storage.Precondition)
              | Error(storage.Corrupt(_)) -> {
                log_error(outcome)
                State(..state, archive: Fenced)
              }
              Error(_) -> {
                log_error(outcome)
                State(
                  ..state,
                  archive: RetryClose(plan),
                  retry_ms: runtime.monotonic_ms() + 1000,
                )
              }
            }
          _ -> {
            logging.log(logging.Warning, "Discarded stale close completion")
            state
          }
        }
        account(next, limits)
        actor.continue(next)
      }
      Storage(worker.Opened(outcome)) -> {
        let next = case state.archive {
          Opening ->
            case outcome {
              Ok(session) ->
                State(
                  ..state,
                  stream: stream.State(
                    ..stream.new(session.head.incarnation),
                    epoch: session.head.epoch,
                  ),
                  archive: Active(session),
                )
              Error(storage.NotFound) -> State(..state, archive: Missing)
              Error(storage.Conflict)
              | Error(storage.Precondition)
              | Error(storage.Corrupt(_)) -> {
                log_error(outcome)
                State(..state, archive: Fenced)
              }
              Error(_) -> {
                log_error(outcome)
                State(..state, archive: Starting)
              }
            }
          _ -> state
        }
        account(next, limits)
        actor.continue(next)
      }
      Storage(worker.Committed(epoch, id, outcome)) -> {
        let next = case state.archive {
          Writing(plan)
            if plan.next.epoch == epoch && plan.next.commit_id == id
          ->
            case outcome {
              Ok(session) ->
                case stream.committed(state.stream, session.head.archived) {
                  Ok(model) -> {
                    operations.record(
                      state.metrics,
                      operations.PersistedEvents,
                      model.persisted - state.stream.persisted,
                    )
                    operations.record(
                      state.metrics,
                      operations.PersistedBytes,
                      state.stream.bytes - model.bytes,
                    )
                    let next =
                      State(..state, stream: model, archive: Active(session))
                    case next.draining {
                      True -> drain_step(next)
                      False -> next
                    }
                  }
                  Error(_) -> State(..state, archive: Fenced)
                }
              Error(storage.Conflict)
              | Error(storage.Precondition)
              | Error(storage.Corrupt(_)) -> {
                log_error(outcome)
                State(..state, archive: Fenced)
              }
              Error(_) -> {
                log_error(outcome)
                State(
                  ..state,
                  archive: Retrying(plan),
                  retry_ms: runtime.monotonic_ms() + 1000,
                )
              }
            }
          _ -> {
            logging.log(logging.Warning, "Discarded stale archival completion")
            state
          }
        }
        account(next, limits)
        actor.continue(next)
      }
    }
  })
  |> actor.start
  |> result.map(fn(s) { actor.Started(..s, data: Owner(s.data.0, s.data.1)) })
}

fn log_error(outcome: Result(a, storage.Error)) -> Nil {
  case outcome {
    Error(e) ->
      logging.log(
        logging.Warning,
        "Archival operation failed: " <> string.inspect(e),
      )
    Ok(_) -> Nil
  }
}

fn send_open(state: State, create: Bool) -> State {
  case state.writer {
    Some(writer) -> {
      process.send(writer, worker.Open(state.scope, create, state.replies))
      State(..state, archive: Opening, create: create)
    }
    None -> state
  }
}

fn send_commit(state: State, plan: archive.Plan) -> State {
  case state.writer {
    Some(writer) -> {
      process.send(writer, worker.Commit(plan, state.replies))
      State(..state, archive: Writing(plan))
    }
    None -> state
  }
}

fn flush(state: State) -> State {
  case state.archive {
    Active(session) ->
      case stream.batch(state.stream, 1_048_576) {
        [] -> state
        events ->
          case archive.plan(session, events) {
            Ok(plan) -> send_commit(state, plan)
            Error(e) -> {
              logging.log(
                logging.Error,
                "Invalid archival batch: " <> string.inspect(e),
              )
              State(..state, archive: Fenced)
            }
          }
      }
    _ -> state
  }
}

fn account(s: State, limits: Limits) -> Nil {
  runtime.set_drained(
    s.gate,
    s.archive == Closed
      || s.archive == Missing
      || { s.archive == Memory && s.draining },
  )
  runtime.set_missing(s.gate, s.archive == Missing)
  let healthy = case s.archive {
    Active(_) | Writing(_) -> True
    Memory
    | Starting
    | Missing
    | Opening
    | Retrying(_)
    | Fenced
    | Closing(_)
    | RetryClose(_)
    | Closed -> False
  }
  let age_ok = case s.stream.oldest_ms {
    None -> True
    Some(ms) -> runtime.monotonic_ms() - ms < limits.pending_age_ms
  }
  let progress = case s.stream.persisted > 0 && s.archive != Memory {
    True ->
      Some(domain.Cursor(
        s.stream.incarnation,
        s.stream.epoch,
        s.stream.persisted,
      ))
    False -> None
  }
  runtime.set_account(
    s.gate,
    s.stream.bytes,
    healthy
      && age_ok
      && s.stream.bytes < limits.stream_bytes
      && dict.size(s.stream.times) < limits.max_records,
    progress,
  )
}

pub fn stats(o: Owner) -> #(Int, Bool, Option(Cursor)) {
  runtime.account(o.gate)
}

pub fn append(
  owner: Owner,
  id: String,
  data: json.Json,
  now: Int,
) -> Result(Event, Error) {
  use permit <- result.try(admit(owner))
  use _ <- result.try(
    runtime.reserve_bytes(owner.gate, stream.event_bytes(id, data))
    |> result.replace_error(domain.Unavailable),
  )
  runtime.call(owner.subject, 1000, fn(reply) {
    Request(permit, Append(id, data, now, reply))
  })
  |> result.map_error(log_call_error)
  |> result.unwrap(Error(domain.Unavailable))
}

pub fn watermark(owner: Owner) -> Result(Cursor, Error) {
  use permit <- result.try(admit(owner))
  runtime.call(owner.subject, 1000, fn(reply) {
    Request(permit, Watermark(reply))
  })
  |> result.map_error(log_call_error)
  |> result.unwrap(Error(domain.Unavailable))
}

pub fn read(owner: Owner, after: Cursor) -> Result(Option(Event), Error) {
  use permit <- result.try(admit(owner))
  runtime.call(owner.subject, 1000, fn(reply) {
    Request(permit, Read(after, reply))
  })
  |> result.map_error(log_call_error)
  |> result.unwrap(Error(domain.Unavailable))
}

pub fn alive(owner: Owner) -> Bool {
  process.is_alive(pid(owner))
}

pub fn pid(owner: Owner) -> process.Pid {
  let assert Ok(pid) = process.subject_owner(owner.subject)
  pid
}

fn admit(owner: Owner) -> Result(runtime.Permit, Error) {
  runtime.permit(owner.gate)
  |> result.map_error(fn(_) {
    case alive(owner) {
      True -> domain.RateLimited
      False -> domain.Unavailable
    }
  })
}

fn log_call_error(reason: String) -> Error {
  logging.log(logging.Warning, "Stream owner request failed: " <> reason)
  domain.Unavailable
}

pub fn stop(owner: Owner) -> Result(Nil, Error) {
  use permit <- result.try(admit(owner))
  runtime.call(owner.subject, 1000, fn(reply) { Request(permit, Stop(reply)) })
  |> result.map_error(log_call_error)
  |> result.unwrap(Error(domain.Unavailable))
}

pub fn missing(o: Owner) -> Bool {
  runtime.is_missing(o.gate)
}

fn clean_leases(s: State) -> State {
  State(
    ..s,
    leases: dict.filter(s.leases, fn(_, lease) {
      process.is_alive(lease.connection)
    }),
  )
}

fn capture(s: State, from: protocol.From) -> Result(view.View, Error) {
  use _ <- result.try(case s.draining {
    True -> Error(domain.Unavailable)
    False -> Ok(Nil)
  })
  use _ <- result.try(case s.archive {
    Starting | Opening | Fenced -> Error(domain.Unavailable)
    Missing -> Error(domain.StreamNotFound)
    _ -> Ok(Nil)
  })
  let high = stream.watermark(s.stream)
  let floor = case from {
    protocol.Latest -> high
    protocol.Earliest -> domain.Cursor(high.incarnation, 1, 0)
    protocol.After(c) -> c
  }
  case
    floor.incarnation != high.incarnation
    || { s.archive == Memory && floor.epoch != high.epoch }
  {
    True -> Error(domain.CursorInvalid)
    False ->
      case domain.after(floor, high) {
        True -> Error(domain.CursorAhead)
        False ->
          Ok(view.View(
            runtime.incarnation(),
            high,
            floor,
            from != protocol.Latest,
          ))
      }
  }
}

fn pull_state(s: State, id: String, after: Cursor) -> Result(view.Pull, Error) {
  use lease <- result.try(
    dict.get(s.leases, id) |> result.replace_error(domain.Forbidden),
  )
  use _ <- result.try(
    case
      process.is_alive(lease.connection)
      && domain.at_or_before(lease.view.floor, after)
    {
      True -> Ok(Nil)
      False -> Error(domain.Forbidden)
    },
  )
  use _ <- result.try(case s.archive {
    Fenced -> Error(domain.Unavailable)
    _ -> Ok(Nil)
  })
  let tail = stream.watermark(s.stream)
  case domain.after(after, tail) {
    True -> Error(domain.CursorAhead)
    False ->
      case after.epoch != tail.epoch {
        True -> Ok(view.Pull(tail, view.Archived))
        False ->
          case stream.next(s.stream, after) {
            Ok(Some(e)) -> Ok(view.Pull(tail, view.Available(e)))
            Ok(None) -> Ok(view.Pull(tail, view.AtTail))
            Error(domain.SlowConsumer) -> Ok(view.Pull(tail, view.Archived))
            Error(e) -> Error(e)
          }
      }
  }
}

pub fn capture_view(
  o: Owner,
  from: protocol.From,
  connection: process.Pid,
) -> Result(view.View, Error) {
  use permit <- result.try(admit(o))
  runtime.call(o.subject, 1000, fn(reply) {
    Request(permit, Capture(from, connection, reply))
  })
  |> result.map_error(log_call_error)
  |> result.unwrap(Error(domain.Unavailable))
}

pub fn pull(o: Owner, v: view.View, after: Cursor) -> Result(view.Pull, Error) {
  use permit <- result.try(admit(o))
  runtime.call(o.subject, 1000, fn(reply) {
    Request(permit, Pull(v.id, after, reply))
  })
  |> result.map_error(log_call_error)
  |> result.unwrap(Error(domain.Unavailable))
}

pub fn release(o: Owner, v: view.View) -> Nil {
  case admit(o) {
    Error(_) -> {
      // Admission stays bounded. Dead connection leases are also reaped by the
      // owner timer; never block socket cancellation on a saturated mailbox.
      case alive(o) {
        True -> logging.log(logging.Warning, "Replay lease cleanup deferred")
        False -> Nil
      }
    }
    Ok(permit) -> process.send(o.subject, Request(permit, Release(v.id)))
  }
}

fn drain_step(state: State) -> State {
  case state.archive {
    Active(session) ->
      case state.stream.bytes == 0 {
        True -> send_close(state, archive.closure(session))
        False -> flush(state)
      }
    _ -> state
  }
}

fn send_close(state: State, plan: archive.Closure) -> State {
  case state.writer {
    Some(writer) -> {
      process.send(writer, worker.Close(plan, state.replies))
      State(..state, archive: Closing(plan))
    }
    None -> state
  }
}

/// Asynchronous mailbox barrier, sent by the registry after admission closes.
/// Earlier appends from that registry precede this operation even after timeout.
pub fn begin_drain(o: Owner) -> Result(Nil, Error) {
  use permit <- result.try(admit(o))
  process.send(o.subject, Request(permit, Drain))
  Ok(Nil)
}

pub fn drained(o: Owner) -> Bool {
  alive(o) && runtime.is_drained(o.gate)
}
