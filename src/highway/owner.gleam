import gleam/dict
import gleam/erlang/process.{type Subject}
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string
import highway/cluster
import highway/config.{type Limits}
import highway/domain.{
  type Cursor, type Error, type Event, type Stream, type Tenant,
}
import highway/observability as logging
import highway/operations
import highway/protocol
import highway/replay/view
import highway/runtime
import highway/storage.{type Store}
import highway/storage/archive
import highway/storage/format
import highway/storage/worker
import highway/stream

pub opaque type Owner {
  Owner(subject: Subject(Message), gate: runtime.Slots)
}

type Message {
  Request(runtime.Permit, Operation)
  Tick
  Storage(worker.Reply)
  Upstream(Feed)
  Located(
    node: String,
    result: Result(Result(#(Owner, process.Pid), Error), Nil),
  )
  Down(process.Down)
}

/// Ordered relay feed from the primary owner's agent to a mirror owner.
/// `Begin` marks the exclusive start (everything before it is archived);
/// `Persisted` never exceeds the last cursor already sent in `Events`.
pub type Feed {
  Begin(Cursor)
  Events(List(Event))
  Persisted(Cursor)
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
  /// Clustered: another node's lease is current. Locate it or wait it out.
  Elsewhere(node: String, until: Int)
  Locating(node: String, until: Int)
  /// Clustered: a local read replica of a remote primary. Publishes bypass
  /// this actor and go straight to the primary; `ready` follows `Begin`.
  Mirror(node: String, ready: Bool)
  Renewing(archive.Closure)
  RetryRenew(archive.Closure)
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
    cluster: config.Cluster,
    key: #(Tenant, Stream),
    feed: Subject(Feed),
    agent: Option(process.Pid),
    lease_until: Int,
    activity_ms: Int,
    sleeping: Bool,
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
  start_cluster(
    limits,
    store,
    scope,
    create,
    slots,
    metrics,
    config.Standalone,
    #(domain.Tenant(""), domain.Stream("")),
  )
}

pub fn start_cluster(
  limits: Limits,
  store: Option(Store),
  scope: String,
  create: Bool,
  slots: runtime.Slots,
  metrics: Option(operations.Context),
  cluster: config.Cluster,
  key: #(Tenant, Stream),
) -> actor.StartResult(Owner) {
  actor.new_with_initialiser(1000, fn(subject) {
    let gate = runtime.slots()
    let replies = process.new_subject()
    let feed = process.new_subject()
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
        cluster,
        key,
        feed,
        None,
        0,
        runtime.monotonic_ms(),
        False,
      )
    let initial = case store {
      None -> Ok(initial)
      Some(store) -> {
        use writer <- result.try(
          worker.start(store, slots, process.self())
          |> result.map_error(fn(_) { "storage worker startup failed" }),
        )
        process.send(
          writer.data,
          worker.Open(scope, create, claimant(cluster), replies),
        )
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
      |> process.select_map(replies, Storage)
      |> process.select_map(feed, Upstream)
      |> process.select_monitors(Down),
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
                        activity_ms: runtime.monotonic_ms(),
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
                  Opening
                  | Fenced
                  | Elsewhere(_, _)
                  | Locating(_, _)
                  | Mirror(_, _)
                  | Closing(_)
                  | RetryClose(_)
                  | Closed -> {
                    runtime.release_bytes(
                      state.gate,
                      stream.event_bytes(id, data),
                    )
                    process.send(reply, Error(domain.Unavailable))
                    actor.continue(state)
                  }
                  _ ->
                    case
                      lease_fenced(state),
                      stream.append(state.stream, id, data, limits, now)
                    {
                      True, _ -> {
                        // The confirmed lease is about to lapse: another node
                        // may claim the stream, so stop admitting first.
                        runtime.release_bytes(
                          state.gate,
                          stream.event_bytes(id, data),
                        )
                        process.send(reply, Error(domain.Unavailable))
                        actor.continue(state)
                      }
                      False, Error(e) -> {
                        runtime.release_bytes(
                          state.gate,
                          stream.event_bytes(id, data),
                        )
                        process.send(reply, Error(e))
                        actor.continue(state)
                      }
                      False, Ok(#(next, event)) -> {
                        let next =
                          State(
                            ..state,
                            stream: next,
                            activity_ms: runtime.monotonic_ms(),
                          )
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
              Starting
              | Opening
              | Fenced
              | Elsewhere(_, _)
              | Locating(_, _)
              | Mirror(_, False) -> Error(domain.Unavailable)
              Missing -> Error(domain.StreamNotFound)
              _ -> Ok(stream.watermark(state.stream))
            }
            process.send(reply, result)
            actor.continue(state)
          }
          Read(c, reply) -> {
            process.send(reply, case state.archive {
              Fenced | Elsewhere(_, _) | Locating(_, _) | Mirror(_, False) ->
                Error(domain.Unavailable)
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
          RetryRenew(plan) ->
            case runtime.monotonic_ms() >= state.retry_ms {
              True -> send_renew(state, plan)
              False -> state
            }
          Elsewhere(node, until) -> {
            let wall = runtime.wall_ms()
            case state.cluster {
              config.Clustered(skew_ms: skew, ..) if wall >= until + skew ->
                send_open(state, state.create)
              _ ->
                case runtime.monotonic_ms() >= state.retry_ms {
                  True -> locate(state, node, until)
                  False -> state
                }
            }
          }
          Active(_) ->
            case state.draining {
              True -> drain_step(state)
              False -> flush(state) |> renew |> sleep
            }
          Mirror(_, True) -> sleep(state)
          _ -> state
        }
        finish(next, limits)
      }
      Upstream(feed) ->
        case state.archive, feed {
          // The agent's first frame can overtake the erpc reply that names it:
          // only an agent given this owner's feed subject can send it.
          Mirror(node, False), Begin(start) | Locating(node, _), Begin(start) -> {
            let model =
              stream.State(
                ..stream.new(start.incarnation),
                epoch: start.epoch,
                offset: start.offset,
                persisted: start.offset,
              )
            finish(
              State(..state, stream: model, archive: Mirror(node, True)),
              limits,
            )
          }
          Mirror(_, True), _ ->
            case mirror(state.stream, feed) {
              Ok(model) -> finish(State(..state, stream: model), limits)
              Error(reason) -> {
                logging.log(logging.Warning, "Mirror feed rejected: " <> reason)
                actor.stop_abnormal("mirror feed out of order")
              }
            }
          _, _ -> actor.continue(state)
        }
      Located(node, outcome) ->
        case state.archive, outcome {
          Locating(expected, _), Ok(Ok(#(primary, agent)))
          | Mirror(expected, _), Ok(Ok(#(primary, agent)))
            if expected == node && state.agent == None
          -> {
            let _ = process.monitor(agent)
            set_upstream(state.gate, primary)
            let archive = case state.archive {
              Mirror(_, ready) -> Mirror(node, ready)
              _ -> Mirror(node, False)
            }
            finish(State(..state, archive: archive, agent: Some(agent)), limits)
          }
          // A feed already started but its agent handle was lost: without a
          // monitor this mirror could outlive its primary, so start over.
          Mirror(expected, _), _ if expected == node && state.agent == None ->
            actor.stop_abnormal("mirror agent unconfirmed")
          Locating(expected, until), failed if expected == node -> {
            logging.log(
              logging.Warning,
              "Stream owner locate on "
                <> node
                <> " failed: "
                <> case failed {
                Ok(Error(e)) -> domain.code(e)
                _ -> "transport"
              },
            )
            finish(
              State(
                ..state,
                archive: Elsewhere(node, until),
                retry_ms: runtime.monotonic_ms() + 1000,
              ),
              limits,
            )
          }
          _, _ -> {
            case outcome {
              Ok(Ok(#(_, agent))) -> process.kill(agent)
              _ -> Nil
            }
            actor.continue(state)
          }
        }
      Down(process.ProcessDown(pid: pid, ..)) ->
        case state.agent == Some(pid) {
          True -> actor.stop_abnormal("mirror upstream lost")
          False -> actor.continue(state)
        }
      Down(_) -> actor.continue(state)
      Storage(worker.Closed(epoch, id, outcome)) -> {
        let next = case state.archive {
          Renewing(plan)
            if plan.next.epoch == epoch && plan.next.commit_id == id
          ->
            case outcome {
              Ok(session) ->
                State(
                  ..state,
                  archive: Active(session),
                  lease_until: lease_of(session),
                )
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
                  archive: RetryRenew(plan),
                  retry_ms: runtime.monotonic_ms() + 1000,
                )
              }
            }
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
        finish(next, limits)
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
                  lease_until: lease_of(session),
                  activity_ms: runtime.monotonic_ms(),
                )
              Error(storage.NotFound) -> State(..state, archive: Missing)
              Error(storage.Leased(node, _, until)) ->
                locate(state, node, until)
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
        finish(next, limits)
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
                      State(
                        ..state,
                        stream: model,
                        archive: Active(session),
                        lease_until: lease_of(session),
                      )
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
        finish(next, limits)
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
      process.send(
        writer,
        worker.Open(state.scope, create, claimant(state.cluster), state.replies),
      )
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
          case
            archive.plan_leased(session, events, next_lease(state, session))
          {
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
  let mirroring = case s.archive {
    Mirror(_, _) -> True
    _ -> False
  }
  runtime.set_drained(
    s.gate,
    s.archive == Closed
      || s.archive == Missing
      || { s.archive == Memory && s.draining }
      || { mirroring && s.draining },
  )
  runtime.set_missing(s.gate, s.archive == Missing)
  let healthy = case s.archive {
    Active(_) | Writing(_) | Renewing(_) | Mirror(_, True) -> True
    Elsewhere(_, _)
    | Locating(_, _)
    | Mirror(_, False)
    | RetryRenew(_)
    | Memory
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
  // A mirror buffers a remote primary's events; it holds no local pending.
  let pending = case mirroring {
    True -> 0
    False -> s.stream.bytes
  }
  runtime.set_account(
    s.gate,
    pending,
    healthy
      && { mirroring || age_ok && !lease_fenced(s) }
      && pending < limits.stream_bytes
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
  case get_upstream(owner.gate) {
    // Mirrors never order events: the remote primary assigns the cursor.
    Ok(primary) ->
      case remote_append(cluster.pid_node(pid(primary)), primary, id, data) {
        Ok(outcome) -> outcome
        Error(_) -> Error(domain.Unavailable)
      }
    Error(_) -> append_local(owner, id, data, now)
  }
}

/// Remote entry point for a mirror's forwarded publish, executed on the
/// primary's node with that node's monotonic clock. It never forwards again.
pub fn append_here(
  owner: Owner,
  id: String,
  data: json.Json,
) -> Result(Event, Error) {
  case forwarding(owner) {
    True -> Error(domain.Unavailable)
    False -> append_local(owner, id, data, runtime.monotonic_ms())
  }
}

/// True for a mirror whose publishes go to a remote primary.
pub fn forwarding(owner: Owner) -> Bool {
  result.is_ok(get_upstream(owner.gate))
}

fn append_local(
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

/// True after an owner exited deliberately (slept or idle mirror) rather than
/// failing. Connections drop its progress watches without closing.
pub fn retired(o: Owner) -> Bool {
  !alive(o) && cluster.retired(pid(o))
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
    Starting
    | Opening
    | Fenced
    | Elsewhere(_, _)
    | Locating(_, _)
    | Mirror(_, False) -> Error(domain.Unavailable)
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
    Fenced | Elsewhere(_, _) | Locating(_, _) | Mirror(_, False) ->
      Error(domain.Unavailable)
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

@external(erlang, "highway_cluster_ffi", "put_upstream")
fn set_upstream(gate: runtime.Slots, primary: Owner) -> Nil

@external(erlang, "highway_cluster_ffi", "get_upstream")
fn get_upstream(gate: runtime.Slots) -> Result(Owner, Nil)

@external(erlang, "highway_cluster_ffi", "append")
fn remote_append(
  node: String,
  primary: Owner,
  id: String,
  data: json.Json,
) -> Result(Result(Event, Error), Nil)

@external(erlang, "highway_cluster_ffi", "attach")
fn remote_attach(
  node: String,
  tenant: Tenant,
  stream: Stream,
  feed: Subject(Feed),
) -> Result(Result(#(Owner, process.Pid), Error), Nil)

fn claimant(cluster: config.Cluster) -> Option(archive.Claimant) {
  case cluster {
    config.Standalone -> None
    config.Clustered(node, region, lease, skew, _) ->
      Some(archive.Claimant(node, region, 0, lease, skew))
  }
}

fn lease_of(session: archive.Session) -> Int {
  case session.head.owner {
    Some(lease) -> lease.until
    None -> 0
  }
}

fn next_lease(state: State, session: archive.Session) -> Option(format.Lease) {
  case state.cluster {
    config.Standalone -> session.head.owner
    config.Clustered(node, region, lease, _, _) ->
      Some(format.Lease(node, region, runtime.wall_ms() + lease))
  }
}

/// Live admission fence: a clustered owner stops admitting `skew` before its
/// confirmed lease ends; claimants wait until `skew` after it.
fn lease_fenced(state: State) -> Bool {
  case state.cluster, state.archive {
    config.Standalone, _ | _, Memory -> False
    config.Clustered(skew_ms: skew, ..), _ ->
      runtime.wall_ms() >= state.lease_until - skew
  }
}

/// Renew an idle clustered lease halfway through its term. Data commits renew
/// too, so this only runs while nothing is pending.
fn renew(state: State) -> State {
  case state.cluster, state.archive {
    config.Clustered(node, region, lease, _, _), Active(session) ->
      case runtime.wall_ms() >= state.lease_until - lease / 2 {
        True ->
          send_renew(
            state,
            archive.renewal(
              session,
              format.Lease(node, region, runtime.wall_ms() + lease),
            ),
          )
        False -> state
      }
    _, _ -> state
  }
}

fn send_renew(state: State, plan: archive.Closure) -> State {
  case state.writer {
    Some(writer) -> {
      process.send(writer, worker.Close(plan, state.replies))
      State(..state, archive: Renewing(plan))
    }
    None -> state
  }
}

/// Sleep: an idle clustered stream with nothing pending and no readers closes
/// its head so the next client can wake it in the nearest region.
fn sleep(state: State) -> State {
  case state.cluster, state.archive {
    config.Clustered(sleep_ms: idle, ..), Active(_)
      if idle > 0 && state.stream.bytes == 0 && !state.draining
    ->
      case
        dict.is_empty(state.leases)
        && runtime.monotonic_ms() - state.activity_ms >= idle
      {
        True -> drain_step(State(..state, draining: True, sleeping: True))
        False -> state
      }
    // An idle mirror simply exits, releasing its view on the primary so the
    // primary can sleep too. The next local request attaches a fresh mirror.
    config.Clustered(sleep_ms: idle, ..), Mirror(_, True) if idle > 0 ->
      case
        dict.is_empty(state.leases)
        && runtime.monotonic_ms() - state.activity_ms >= idle
      {
        True -> State(..state, sleeping: True)
        False -> state
      }
    _, _ -> state
  }
}

/// Ask the lease holder for its primary owner and start a relay agent there.
/// Distribution setup can block, so it runs outside the owner actor.
fn locate(state: State, node: String, until: Int) -> State {
  case state.cluster {
    config.Standalone -> State(..state, archive: Fenced)
    config.Clustered(..) -> {
      let subject = state.subject
      let #(tenant, stream_id) = state.key
      let feed = state.feed
      process.spawn_unlinked(fn() {
        process.send(
          subject,
          Located(node, remote_attach(node, tenant, stream_id, feed)),
        )
      })
      State(..state, archive: Locating(node, until))
    }
  }
}

fn finish(state: State, limits: Limits) -> actor.Next(State, Message) {
  account(state, limits)
  case state.cluster, state.archive {
    // A clustered owner that lost its head (or finished sleeping) exits so the
    // registry re-resolves the stream on the next request.
    config.Clustered(..), Fenced -> actor.stop_abnormal("ownership fenced")
    config.Clustered(..), Mirror(_, _) if state.sleeping -> {
      cluster.retire(process.self())
      actor.stop()
    }
    config.Clustered(..), Closed if state.sleeping -> {
      cluster.retire(process.self())
      case state.writer {
        Some(writer) ->
          case process.subject_owner(writer) {
            Ok(pid) -> {
              process.unlink(pid)
              process.kill(pid)
            }
            Error(_) -> Nil
          }
        None -> Nil
      }
      actor.stop()
    }
    _, _ -> actor.continue(state)
  }
}

/// Apply one relay feed message to mirror state. Gaps or foreign cursors are
/// errors: a mirror must never invent or skip upstream events.
fn mirror(s: stream.State, feed: Feed) -> Result(stream.State, String) {
  case feed {
    Begin(start) ->
      case
        start.incarnation == s.incarnation
        && start.epoch == s.epoch
        && start.offset >= s.offset
      {
        True ->
          Ok(
            stream.State(
              ..s,
              offset: start.offset,
              persisted: start.offset,
              events: dict.new(),
              bytes: 0,
            ),
          )
        False -> Error("restart outside mirrored epoch")
      }
    Events(events) ->
      list.try_fold(events, s, fn(m, e) {
        case e.cursor == domain.Cursor(m.incarnation, m.epoch, m.offset + 1) {
          True ->
            Ok(
              stream.State(
                ..m,
                offset: m.offset + 1,
                bytes: m.bytes + e.bytes,
                events: dict.insert(m.events, e.cursor.offset, e),
              ),
            )
          False -> Error("noncontiguous mirrored event")
        }
      })
    Persisted(through) ->
      case through.offset <= s.persisted {
        True -> Ok(s)
        False ->
          stream.committed(s, through)
          |> result.replace_error("mirrored persistence ahead of feed")
      }
  }
}
