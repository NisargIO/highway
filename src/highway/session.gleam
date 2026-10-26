import gleam/dict.{type Dict}
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import highway/auth.{type Claims}
import highway/config.{type Config}
import highway/domain.{type Error, type Stream}
import highway/flow
import highway/owner.{type Owner}
import highway/protocol.{type Command, type Response}
import highway/registry.{type Registry}
import highway/replay
import highway/replay/read
import highway/replay/view
import highway/runtime

pub type Subscription {
  Subscription(
    stream: Stream,
    owner: Owner,
    cursor: domain.Cursor,
    credit: flow.Credit,
    view: view.View,
    launcher: replay.Launcher,
    follow: Bool,
    caught_up: Bool,
    started_ms: Int,
    scan: Option(read.Scan),
    buffer: List(read.Item),
    job: Option(replay.Job),
    retry_ms: Int,
    held_bytes: Int,
    target: domain.Cursor,
    scoped_claims: Option(Claims),
  )
}

pub type Watch {
  Watch(
    owner: Owner,
    through: Option(domain.Cursor),
    publisher: Bool,
    scoped_claims: Option(Claims),
  )
}

pub type Receipt {
  Receipt(
    stream: Stream,
    credit: flow.Credit,
    historical: Bool,
    scoped_claims: Option(Claims),
  )
}

type AckTarget {
  Active(Subscription)
  Completed(Receipt)
}

pub type State {
  State(
    claims: Option(Claims),
    identity: Option(#(String, domain.Tenant)),
    subscriptions: Dict(String, Subscription),
    started_ms: Int,
    last_input_ms: Int,
    rotation: Int,
    watches: Dict(Stream, Watch),
    replies: process.Subject(replay.Reply),
    completed: List(#(String, Receipt)),
  )
}

pub type Step {
  Step(state: State, responses: List(Response), close: Option(Int))
}

pub fn new(now: Int) -> State {
  State(
    None,
    None,
    dict.new(),
    now,
    now,
    0,
    dict.new(),
    process.new_subject(),
    [],
  )
}

fn success(state: State, responses: List(Response)) -> Step {
  Step(state, responses, None)
}

fn failure(state: State, id: String, sub: String, error: Error) -> Step {
  success(state, [protocol.Failure(id, sub, error)])
}

pub fn disconnect(state: State, error: Error, code: Int) -> Step {
  cleanup(state)
  Step(
    State(..state, subscriptions: dict.new(), completed: []),
    [protocol.Failure("", "", error)],
    Some(code),
  )
}

pub fn deadline(
  state: State,
  config: Config,
  now: Int,
  ms: Int,
) -> Option(Error) {
  case state.claims {
    None
      if state.identity == None && ms - state.started_ms >= config.limits.auth_ms
    -> Some(domain.Unauthenticated)
    Some(c) if now >= c.expires_at -> Some(domain.TokenExpired)
    _ if ms - state.last_input_ms >= config.limits.idle_ms ->
      Some(domain.Unavailable)
    _ -> None
  }
}

pub fn handle(
  state: State,
  command: Command,
  config: Config,
  registry: Registry,
  now: Int,
  ms: Int,
) -> Step {
  case deadline(state, config, now, ms) {
    Some(e) -> disconnect(state, e, 1008)
    None ->
      command_step(
        State(..state, last_input_ms: ms),
        command,
        config,
        registry,
        now,
      )
  }
}

fn command_step(
  state: State,
  cmd: Command,
  config: Config,
  registry: Registry,
  now: Int,
) -> Step {
  case cmd {
    protocol.ScopedSubscribe(id, sub, stream, from, follow, window, token) ->
      scoped_command(
        state,
        protocol.Subscribe(id, sub, stream, from, follow, window),
        token,
        config,
        registry,
        now,
      )
    protocol.ScopedPublish(id, stream, message, data, token) ->
      scoped_command(
        state,
        protocol.Publish(id, stream, message, data),
        token,
        config,
        registry,
        now,
      )
    protocol.SubscriptionAuth(id, sub, token) ->
      refresh_subscription(state, id, sub, token, config, now)
    protocol.Unsubscribe(id, sub) if state.identity != None -> {
      case dict.get(state.subscriptions, sub) {
        Ok(s) -> close_sub(s)
        Error(_) -> Nil
      }
      let state =
        State(
          ..state,
          subscriptions: dict.delete(state.subscriptions, sub),
          completed: list.filter(state.completed, fn(pair) { pair.0 != sub }),
        )
      success(prune_watches(state, now), [protocol.Unsubscribed(id, sub)])
    }
    protocol.Auth(id, _) if state.identity != None ->
      failure(state, id, "", domain.Forbidden)
    protocol.Auth(id, token) ->
      case auth.verify(config.auth, token, now) {
        Error(e) ->
          case state.claims {
            None -> Step(state, [protocol.Failure(id, "", e)], Some(1008))
            Some(_) -> failure(state, id, "", e)
          }
        Ok(claims) -> {
          let same_identity = case state.claims {
            None -> True
            Some(old) ->
              old.subject == claims.subject && old.tenant == claims.tenant
          }
          case same_identity {
            False -> failure(state, id, "", domain.Forbidden)
            True -> {
              let #(subs, cancelled) =
                dict.fold(
                  state.subscriptions,
                  #(dict.new(), []),
                  fn(acc, id, s) {
                    case allowed(claims, s, now) {
                      Ok(_) -> {
                        cancel_job(s)
                        #(
                          dict.insert(
                            acc.0,
                            id,
                            Subscription(
                              ..s,
                              job: None,
                              buffer: [],
                              scan: None,
                              held_bytes: 0,
                            ),
                          ),
                          acc.1,
                        )
                      }
                      Error(e) -> {
                        close_sub(s)
                        #(acc.0, [protocol.Failure("", id, e), ..acc.1])
                      }
                    }
                  },
                )
              let domain.Tenant(tenant) = claims.tenant
              success(
                prune_watches(
                  State(
                    ..state,
                    claims: Some(claims),
                    subscriptions: subs,
                    completed: list.filter(state.completed, fn(pair) {
                      receipt_allowed(claims, pair.1, now) == Ok(Nil)
                    }),
                  ),
                  now,
                ),
                [
                  protocol.Authenticated(
                    id,
                    claims.subject,
                    tenant,
                    claims.expires_at,
                  ),
                  ..cancelled
                ],
              )
            }
          }
        }
      }
    _ ->
      case state.claims {
        None -> scoped_control(state, cmd, config, registry, now)
        Some(claims) ->
          authorized_command(state, cmd, claims, config, registry, now, None)
      }
  }
}

fn authorized_command(
  state: State,
  cmd: Command,
  claims: Claims,
  config: Config,
  registry: Registry,
  now: Int,
  scoped_claims: Option(Claims),
) -> Step {
  case cmd {
    protocol.Publish(id, stream, message, data) -> {
      let outcome = {
        use _ <- result.try(auth.authorize(
          claims,
          claims.tenant,
          stream,
          domain.Publish,
          now,
        ))
        use _ <- result.try(
          case string.byte_size(json.to_string(data)) <= 65_536 {
            True -> Ok(Nil)
            False -> Error(domain.FrameTooLarge)
          },
        )
        use _ <- result.try(watch_capacity(state, stream, config))
        registry.publish(registry, claims.tenant, stream, message, data)
      }
      case outcome {
        Ok(event) -> {
          let state = case config.storage {
            config.DevelopmentMemory -> state
            config.S3(_) ->
              case registry.find(registry, claims.tenant, stream) {
                Ok(#(o, _)) -> watch(state, stream, o, True, scoped_claims)
                Error(_) -> state
              }
          }
          let frame = case scoped_claims {
            None -> protocol.Accepted(id, stream, event)
            Some(c) -> {
              let domain.Tenant(tenant) = c.tenant
              protocol.ScopedAccepted(
                id,
                stream,
                event,
                c.subject,
                tenant,
                c.expires_at,
              )
            }
          }
          success(state, [frame])
        }
        Error(e) -> failure(state, id, "", e)
      }
    }
    protocol.Subscribe(id, sub, stream, from, follow, window) -> {
      let outcome = {
        use _ <- result.try(auth.authorize(
          claims,
          claims.tenant,
          stream,
          domain.Subscribe,
          now,
        ))
        use _ <- result.try(case from {
          protocol.Latest -> Ok(Nil)
          _ ->
            auth.authorize(claims, claims.tenant, stream, domain.History, now)
        })
        use _ <- result.try(case dict.has_key(state.subscriptions, sub) {
          True -> Error(domain.InvalidFrame)
          False -> Ok(Nil)
        })
        use _ <- result.try(
          case dict.size(state.subscriptions) >= config.limits.subscriptions {
            True -> Error(domain.RateLimited)
            False -> Ok(Nil)
          },
        )
        use _ <- result.try(watch_capacity(state, stream, config))
        use found <- result.try(registry.find(registry, claims.tenant, stream))
        use launcher <- result.try(registry.replayer(registry))
        use captured <- result.try(owner.capture_view(
          found.0,
          from,
          process.self(),
        ))
        Ok(#(found.0, captured, launcher))
      }
      case outcome {
        Error(e) -> failure(state, id, sub, e)
        Ok(#(o, captured, launcher)) -> {
          let s =
            Subscription(
              stream,
              o,
              captured.floor,
              flow.new(window),
              captured,
              launcher,
              follow,
              captured.floor == captured.high,
              state.last_input_ms,
              None,
              [],
              None,
              state.last_input_ms,
              0,
              captured.high,
              scoped_claims,
            )
          let state =
            State(
              ..state,
              completed: list.filter(state.completed, fn(pair) { pair.0 != sub }),
            )
          let state = case config.storage {
            config.S3(_) if follow -> watch(state, stream, o, False, None)
            _ -> state
          }
          let frames = [
            case scoped_claims {
              None -> protocol.Subscribed(id, sub, captured.high)
              Some(c) -> {
                let domain.Tenant(tenant) = c.tenant
                protocol.ScopedSubscribed(
                  id,
                  sub,
                  captured.high,
                  c.subject,
                  tenant,
                  c.expires_at,
                )
              }
            },
          ]
          let frames = case s.caught_up {
            True -> list.append(frames, [protocol.CaughtUp(sub, captured.high)])
            False -> frames
          }
          case s.caught_up && !follow {
            True -> {
              close_sub(s)
              success(state, list.append(frames, [protocol.Complete(sub)]))
            }
            False ->
              success(
                State(
                  ..state,
                  subscriptions: dict.insert(state.subscriptions, sub, s),
                ),
                frames,
              )
          }
        }
      }
    }
    protocol.Ack(sub, through) -> {
      let outcome = {
        use target <- result.try(case dict.get(state.subscriptions, sub) {
          Ok(s) -> Ok(Active(s))
          Error(_) ->
            list.find(state.completed, fn(pair) { pair.0 == sub })
            |> result.map(fn(pair) { Completed(pair.1) })
            |> result.replace_error(domain.InvalidFrame)
        })
        let stream = case target {
          Active(s) -> s.stream
          Completed(r) -> r.stream
        }
        use _ <- result.try(auth.authorize(
          claims,
          claims.tenant,
          stream,
          domain.Subscribe,
          now,
        ))
        let credit = case target {
          Active(s) -> s.credit
          Completed(r) -> r.credit
        }
        use credit <- result.try(flow.ack(credit, through))
        Ok(case target {
          Active(s) ->
            State(
              ..state,
              subscriptions: dict.insert(
                state.subscriptions,
                sub,
                Subscription(..s, credit: credit),
              ),
            )
          Completed(_) ->
            State(
              ..state,
              completed: list.map(state.completed, fn(pair) {
                case pair.0 == sub {
                  True -> #(sub, Receipt(..pair.1, credit: credit))
                  False -> pair
                }
              }),
            )
        })
      }
      case outcome {
        Ok(next) -> success(next, [])
        Error(e) -> failure(state, "", sub, e)
      }
    }
    protocol.Unsubscribe(id, sub) ->
      case dict.get(state.subscriptions, sub) {
        Error(_) -> failure(state, id, sub, domain.InvalidFrame)
        Ok(s) ->
          case
            auth.authorize(
              claims,
              claims.tenant,
              s.stream,
              domain.Subscribe,
              now,
            )
          {
            Error(e) -> failure(state, id, sub, e)
            Ok(_) -> {
              close_sub(s)
              success(
                State(
                  ..state,
                  subscriptions: dict.delete(state.subscriptions, sub),
                ),
                [protocol.Unsubscribed(id, sub)],
              )
            }
          }
      }
    protocol.Auth(_, _)
    | protocol.ScopedPublish(_, _, _, _, _)
    | protocol.ScopedSubscribe(_, _, _, _, _, _, _)
    | protocol.SubscriptionAuth(_, _, _) ->
      failure(state, "", "", domain.Internal)
  }
}

fn command_id(cmd: Command) -> String {
  case cmd {
    protocol.Auth(id, _)
    | protocol.ScopedPublish(id, _, _, _, _)
    | protocol.ScopedSubscribe(id, _, _, _, _, _, _)
    | protocol.SubscriptionAuth(id, _, _)
    | protocol.Publish(id, _, _, _)
    | protocol.Subscribe(id, _, _, _, _, _)
    | protocol.Unsubscribe(id, _) -> id
    protocol.Ack(_, _) -> ""
  }
}

pub fn tick(state: State, config: Config, now: Int, ms: Int) -> Step {
  case deadline(state, config, now, ms) {
    Some(e) -> disconnect(state, e, 1008)
    None -> {
      // One turn produces at most 16 events. No unbounded mailbox fanout,
      // per-subscriber payload backlog, or per-tick output list.
      let state =
        State(
          ..state,
          completed: list.filter(state.completed, fn(pair) {
            case subscription_claims(state, pair.1.scoped_claims) {
              Ok(claims) -> receipt_allowed(claims, pair.1, now) == Ok(Nil)
              Error(_) -> False
            }
          }),
        )
      let entries = dict.to_list(state.subscriptions)
      let start = case list.length(entries) {
        0 -> 0
        n -> state.rotation % n
      }
      let #(front, back) = list.split(entries, start)
      let entries = list.append(back, front)
      let initial = #(State(..state, rotation: start + 1), [], 16, None)
      let #(state, frames, _, close) =
        list.fold(entries, initial, fn(acc, entry) {
          let #(id, sub) = entry
          let #(state, frames, budget, close) = acc
          let authorized = {
            use claims <- result.try(subscription_claims(
              state,
              sub.scoped_claims,
            ))
            use _ <- result.try(allowed(claims, sub, now))
            Ok(claims)
          }
          case authorized {
            Error(e) -> {
              close_sub(sub)
              #(
                State(
                  ..state,
                  subscriptions: dict.delete(state.subscriptions, id),
                ),
                [protocol.Failure("", id, e), ..frames],
                budget,
                close,
              )
            }
            Ok(claims) -> {
              let #(updated, produced, remaining, fatal) =
                pump(
                  sub,
                  id,
                  config,
                  ms,
                  budget,
                  connection_bytes(state),
                  [],
                  claims,
                  now,
                  state.replies,
                  !dict.fold(state.subscriptions, False, fn(busy, _, other) {
                    busy || other.job != None
                  }),
                )
              let complete =
                list.any(produced, fn(frame) { frame == protocol.Complete(id) })
              let #(subs, receipts) = case updated {
                Some(s) if complete -> {
                  close_sub(s)
                  let receipts =
                    list.append(
                      list.filter(state.completed, fn(pair) { pair.0 != id }),
                      [
                        #(
                          id,
                          Receipt(
                            s.stream,
                            s.credit,
                            s.view.historical,
                            s.scoped_claims,
                          ),
                        ),
                      ],
                    )
                  #(
                    dict.delete(state.subscriptions, id),
                    bound_receipts(receipts),
                  )
                }
                Some(s) -> #(
                  dict.insert(state.subscriptions, id, s),
                  state.completed,
                )
                None -> {
                  close_sub(sub)
                  #(dict.delete(state.subscriptions, id), state.completed)
                }
              }
              #(
                State(..state, subscriptions: subs, completed: receipts),
                list.append(produced, frames),
                remaining,
                option.or(fatal, close),
              )
            }
          }
        })
      let step = progress(state, now)
      Step(
        step.state,
        list.append(list.reverse(frames), step.responses),
        option.or(close, step.close),
      )
    }
  }
}

fn connection_bytes(state: State) -> Int {
  dict.fold(state.subscriptions, 0, fn(n, _, s) {
    n + s.credit.bytes + s.held_bytes
  })
}

fn allowed(claims: Claims, s: Subscription, now: Int) -> Result(Nil, Error) {
  use _ <- result.try(auth.authorize(
    claims,
    claims.tenant,
    s.stream,
    domain.Subscribe,
    now,
  ))
  case s.view.historical {
    True -> auth.authorize(claims, claims.tenant, s.stream, domain.History, now)
    False -> Ok(Nil)
  }
}

fn cancel_job(s: Subscription) -> Nil {
  case s.job {
    Some(job) -> replay.cancel(job)
    None -> Nil
  }
}

fn close_sub(s: Subscription) -> Nil {
  cancel_job(s)
  owner.release(s.owner, s.view)
}

pub fn cleanup(state: State) -> Nil {
  dict.each(state.subscriptions, fn(_, s) { close_sub(s) })
}

fn pump_error(
  id: String,
  e: Error,
  budget: Int,
  frames: List(Response),
) -> #(Option(Subscription), List(Response), Int, Option(Int)) {
  #(None, [protocol.Failure("", id, e), ..frames], budget, case e {
    domain.Unavailable -> Some(1011)
    _ -> None
  })
}

fn ordered(after: domain.Cursor, item: read.Item) -> Bool {
  case item {
    read.Record(e) ->
      e.cursor
      == domain.Cursor(after.incarnation, after.epoch, after.offset + 1)
    read.Boundary(c, before) ->
      c.incarnation == after.incarnation
      && before == after.epoch + 1
      && domain.at_or_before(c, after)
  }
}

fn pump(
  s: Subscription,
  id: String,
  config: Config,
  ms: Int,
  budget: Int,
  bytes: Int,
  frames: List(Response),
  claims: Claims,
  now: Int,
  replies: process.Subject(replay.Reply),
  can_launch: Bool,
) -> #(Option(Subscription), List(Response), Int, Option(Int)) {
  case owner.alive(s.owner) {
    False -> pump_error(id, domain.Unavailable, budget, frames)
    True ->
      case flow.timed_out(s.credit, ms, config.limits.slow_ms) {
        True -> pump_error(id, domain.SlowConsumer, budget, frames)
        False ->
          case !s.caught_up && s.cursor == s.view.high {
            True -> {
              let frames = [protocol.CaughtUp(id, s.view.high), ..frames]
              case s.follow {
                False -> #(
                  Some(Subscription(..s, caught_up: True)),
                  [protocol.Complete(id), ..frames],
                  budget,
                  None,
                )
                True ->
                  pump(
                    Subscription(..s, caught_up: True),
                    id,
                    config,
                    ms,
                    budget,
                    bytes,
                    frames,
                    claims,
                    now,
                    replies,
                    can_launch,
                  )
              }
            }
            False ->
              case
                { !s.caught_up || s.job != None || s.scan != None }
                && ms - s.started_ms >= config.limits.replay_timeout_ms
              {
                True -> pump_error(id, domain.ReplayLimit, budget, frames)
                False ->
                  case budget > 0 && flow.available(s.credit) {
                    False -> #(Some(s), frames, budget, None)
                    True ->
                      case s.buffer {
                        [item, ..rest] -> {
                          let frame = read.response(item, id, s.stream)
                          let size = string.byte_size(protocol.encode(frame))
                          case
                            ordered(s.cursor, item)
                            && {
                              s.caught_up
                              || domain.at_or_before(
                                read.position(item),
                                s.view.high,
                              )
                            }
                          {
                            False ->
                              pump_error(id, domain.Unavailable, budget, frames)
                            True -> {
                              let credit = case item {
                                read.Record(_) ->
                                  flow.sent(
                                    s.credit,
                                    read.position(item),
                                    size,
                                    ms,
                                  )
                                read.Boundary(_, _) -> s.credit
                              }
                              let remaining_bytes = case item {
                                read.Record(_) -> bytes
                                _ -> bytes - size
                              }
                              pump(
                                Subscription(
                                  ..s,
                                  cursor: read.position(item),
                                  credit: credit,
                                  buffer: rest,
                                  held_bytes: s.held_bytes - size,
                                ),
                                id,
                                config,
                                ms,
                                budget - 1,
                                remaining_bytes,
                                [frame, ..frames],
                                claims,
                                now,
                                replies,
                                can_launch,
                              )
                            }
                          }
                        }
                        [] ->
                          case s.job {
                            Some(_) -> #(Some(s), frames, budget, None)
                            None ->
                              case s.scan {
                                Some(_) ->
                                  schedule(
                                    s,
                                    id,
                                    config,
                                    ms,
                                    budget,
                                    bytes,
                                    frames,
                                    claims,
                                    now,
                                    replies,
                                    can_launch,
                                  )
                                None ->
                                  case owner.pull(s.owner, s.view, s.cursor) {
                                    Error(e) ->
                                      pump_error(id, e, budget, frames)
                                    Ok(view.Pull(_, view.AtTail)) ->
                                      case s.caught_up {
                                        True -> #(Some(s), frames, budget, None)
                                        False ->
                                          pump_error(
                                            id,
                                            domain.Unavailable,
                                            budget,
                                            frames,
                                          )
                                      }
                                    Ok(view.Pull(_, view.Available(event))) -> {
                                      let item = read.Record(event)
                                      let size =
                                        string.byte_size(
                                          protocol.encode(read.response(
                                            item,
                                            id,
                                            s.stream,
                                          )),
                                        )
                                      case
                                        size + bytes
                                        > config.limits.output_bytes
                                      {
                                        True ->
                                          pump_error(
                                            id,
                                            domain.SlowConsumer,
                                            budget,
                                            frames,
                                          )
                                        False ->
                                          pump(
                                            Subscription(
                                              ..s,
                                              buffer: [item],
                                              held_bytes: size,
                                            ),
                                            id,
                                            config,
                                            ms,
                                            budget,
                                            bytes + size,
                                            frames,
                                            claims,
                                            now,
                                            replies,
                                            can_launch,
                                          )
                                      }
                                    }
                                    Ok(view.Pull(tail, view.Archived)) ->
                                      schedule(
                                        Subscription(
                                          ..s,
                                          target: case s.caught_up {
                                            True -> tail
                                            False -> s.view.high
                                          },
                                          started_ms: case s.caught_up {
                                            True -> ms
                                            False -> s.started_ms
                                          },
                                        ),
                                        id,
                                        config,
                                        ms,
                                        budget,
                                        bytes,
                                        frames,
                                        claims,
                                        now,
                                        replies,
                                        can_launch,
                                      )
                                  }
                              }
                          }
                      }
                  }
              }
          }
      }
  }
}

fn schedule(
  s: Subscription,
  id: String,
  config: Config,
  ms: Int,
  budget: Int,
  bytes: Int,
  frames: List(Response),
  claims: Claims,
  now: Int,
  replies: process.Subject(replay.Reply),
  can_launch: Bool,
) -> #(Option(Subscription), List(Response), Int, Option(Int)) {
  case can_launch && ms >= s.retry_ms {
    False -> #(Some(s), frames, budget, None)
    True -> {
      let reserved = int.min(131_072, config.limits.output_bytes - bytes)
      case reserved <= 0 {
        True -> pump_error(id, domain.SlowConsumer, budget, frames)
        False -> {
          let input =
            replay.Input(
              id,
              runtime.incarnation(),
              s.owner,
              s.view,
              claims,
              s.stream,
              s.cursor,
              s.target,
              s.scan,
              int.min(16, s.credit.window - list.length(s.credit.pending)),
              reserved,
              now,
              process.self(),
              replies,
            )
          case replay.launch(s.launcher, input) {
            Error(domain.RateLimited) -> #(
              Some(Subscription(..s, retry_ms: ms + 100)),
              frames,
              budget,
              None,
            )
            Error(e) -> pump_error(id, e, budget, frames)
            Ok(job) -> #(
              Some(Subscription(..s, job: Some(job), held_bytes: reserved)),
              frames,
              budget,
              None,
            )
          }
        }
      }
    }
  }
}

/// A late result can only affect the exact still-active generation. Authorization
/// is repeated with the connection's current token before retaining any payload.
pub fn replayed(
  state: State,
  reply: replay.Reply,
  config: Config,
  now: Int,
  ms: Int,
) -> Step {
  case deadline(state, config, now, ms) {
    Some(e) -> disconnect(state, e, 1008)
    None ->
      case dict.get(state.subscriptions, reply.subscription) {
        Ok(s) ->
          case s.job {
            Some(job) ->
              case replay.generation(job) == reply.generation {
                False -> success(state, [])
                True -> {
                  let outcome = {
                    use claims <- result.try(subscription_claims(
                      state,
                      s.scoped_claims,
                    ))
                    use _ <- result.try(allowed(claims, s, now))
                    use step <- result.try(reply.result)
                    case step {
                      read.Continue(scan) ->
                        Ok(
                          Subscription(
                            ..s,
                            job: None,
                            held_bytes: 0,
                            scan: Some(scan),
                          ),
                        )
                      read.Missing -> Error(domain.Unavailable)
                      read.Ready(batch) -> {
                        let bytes =
                          list.fold(batch.items, 0, fn(n, i) {
                            n
                            + string.byte_size(
                              protocol.encode(read.response(
                                i,
                                reply.subscription,
                                s.stream,
                              )),
                            )
                          })
                        case
                          bytes <= s.held_bytes
                          && list.length(batch.items) <= 16
                          && batch.items != []
                        {
                          True ->
                            Ok(
                              Subscription(
                                ..s,
                                job: None,
                                held_bytes: bytes,
                                scan: None,
                                buffer: batch.items,
                              ),
                            )
                          False -> Error(domain.Unavailable)
                        }
                      }
                    }
                  }
                  case outcome {
                    Ok(updated) ->
                      success(
                        State(
                          ..state,
                          subscriptions: dict.insert(
                            state.subscriptions,
                            reply.subscription,
                            updated,
                          ),
                        ),
                        [],
                      )
                    Error(domain.RateLimited) ->
                      success(
                        State(
                          ..state,
                          subscriptions: dict.insert(
                            state.subscriptions,
                            reply.subscription,
                            Subscription(
                              ..s,
                              job: None,
                              held_bytes: 0,
                              retry_ms: ms + 100,
                            ),
                          ),
                        ),
                        [],
                      )
                    Error(e) -> {
                      close_sub(s)
                      Step(
                        State(
                          ..state,
                          subscriptions: dict.delete(
                            state.subscriptions,
                            reply.subscription,
                          ),
                        ),
                        [protocol.Failure("", reply.subscription, e)],
                        case e {
                          domain.Unavailable -> Some(1011)
                          _ -> None
                        },
                      )
                    }
                  }
                }
              }
            None -> success(state, [])
          }
        Error(_) -> success(state, [])
      }
  }
}

fn watch_capacity(
  state: State,
  stream: Stream,
  config: Config,
) -> Result(Nil, Error) {
  case config.storage {
    config.S3(_) ->
      case
        !dict.has_key(state.watches, stream)
        && dict.size(state.watches) >= config.limits.subscriptions
      {
        True -> Error(domain.RateLimited)
        False -> Ok(Nil)
      }
    _ -> Ok(Nil)
  }
}

fn watch(
  state: State,
  stream: Stream,
  o: Owner,
  publisher: Bool,
  scoped_claims: Option(Claims),
) -> State {
  let previous =
    dict.get(state.watches, stream)
    |> result.unwrap(Watch(o, None, False, None))
  State(
    ..state,
    watches: dict.insert(
      state.watches,
      stream,
      Watch(
        o,
        previous.through,
        previous.publisher || publisher,
        case publisher {
          True -> scoped_claims
          False -> previous.scoped_claims
        },
      ),
    ),
  )
}

fn watch_allowed(state: State, stream: Stream, w: Watch, now: Int) -> Bool {
  let publishing = case subscription_claims(state, w.scoped_claims) {
    Ok(claims) ->
      w.publisher
      && auth.authorize(claims, claims.tenant, stream, domain.Publish, now)
      == Ok(Nil)
    Error(_) -> False
  }
  let subscribed =
    dict.fold(state.subscriptions, False, fn(yes, _, s) {
      yes
      || s.stream == stream
      && case subscription_claims(state, s.scoped_claims) {
        Ok(claims) -> allowed(claims, s, now) == Ok(Nil)
        Error(_) -> False
      }
    })
  publishing || subscribed
}

fn prune_watches(state: State, now: Int) -> State {
  State(
    ..state,
    watches: dict.filter(state.watches, fn(stream, w) {
      watch_allowed(state, stream, w, now)
    }),
  )
}

fn progress(state: State, now: Int) -> Step {
  let state = prune_watches(state, now)
  dict.fold(state.watches, success(state, []), fn(step, stream, w) {
    case owner.alive(w.owner) {
      // A slept owner already confirmed everything it accepted; the next
      // publish re-resolves the stream and watches its new owner.
      False ->
        case owner.retired(w.owner) {
          True ->
            Step(
              State(
                ..step.state,
                watches: dict.delete(step.state.watches, stream),
              ),
              step.responses,
              step.close,
            )
          False -> Step(step.state, step.responses, Some(1011))
        }
      True ->
        case owner.stats(w.owner).2 {
          Some(c) if w.through != Some(c) ->
            Step(
              State(
                ..step.state,
                watches: dict.insert(
                  step.state.watches,
                  stream,
                  Watch(..w, through: Some(c)),
                ),
              ),
              list.append(step.responses, [protocol.Persisted(stream, c)]),
              step.close,
            )
          _ -> step
        }
    }
  })
}

/// Completed subscriptions release owners/payloads immediately. Up to 64 final
/// credit receipts tolerate already-in-flight ACKs, with the same delivered-
/// cursor checks. Refresh/reuse/disconnect invalidate the relevant receipts.
fn receipt_allowed(
  claims: Claims,
  receipt: Receipt,
  now: Int,
) -> Result(Nil, Error) {
  let claims = option.unwrap(receipt.scoped_claims, claims)
  use _ <- result.try(auth.authorize(
    claims,
    claims.tenant,
    receipt.stream,
    domain.Subscribe,
    now,
  ))
  case receipt.historical {
    True ->
      auth.authorize(claims, claims.tenant, receipt.stream, domain.History, now)
    False -> Ok(Nil)
  }
}

fn bound_receipts(
  receipts: List(#(String, Receipt)),
) -> List(#(String, Receipt)) {
  let pending =
    list.fold(receipts, 0, fn(n, pair) {
      n + list.length(pair.1.credit.pending)
    })
  case list.length(receipts) > 64 || pending > 1024 {
    True -> bound_receipts(list.drop(receipts, 1))
    False -> receipts
  }
}

/// Explicit scope claims win; only v1 sessions have connection claims to use.
fn subscription_claims(
  state: State,
  scoped: Option(Claims),
) -> Result(Claims, Error) {
  case scoped, state.claims {
    Some(claims), _ -> Ok(claims)
    None, Some(claims) -> Ok(claims)
    _, _ -> Error(domain.Unauthenticated)
  }
}

fn verify_scoped(
  state: State,
  token: String,
  cfg: Config,
  now: Int,
) -> Result(Claims, Error) {
  use _ <- result.try(case state.claims {
    Some(_) -> Error(domain.Forbidden)
    None -> Ok(Nil)
  })
  use claims <- result.try(auth.verify(cfg.auth, token, now))
  let verified_identity = #(claims.subject, claims.tenant)
  case state.identity {
    Some(identity) if identity != verified_identity -> Error(domain.Forbidden)
    _ -> Ok(claims)
  }
}

fn scoped_command(
  state: State,
  cmd: Command,
  token: String,
  cfg: Config,
  reg: Registry,
  now: Int,
) -> Step {
  let id = command_id(cmd)
  let sub = case cmd {
    protocol.Subscribe(_, sub, _, _, _, _) -> sub
    _ -> ""
  }
  let request = case cmd {
    protocol.Publish(_, stream, _, _) -> Ok(#(stream, domain.Publish, False))
    protocol.Subscribe(_, _, stream, from, _, _) ->
      Ok(#(stream, domain.Subscribe, from != protocol.Latest))
    _ -> Error(domain.Internal)
  }
  let outcome = {
    use #(stream, action, historical) <- result.try(request)
    use claims <- result.try(verify_scoped(state, token, cfg, now))
    use _ <- result.try(auth.authorize(
      claims,
      claims.tenant,
      stream,
      action,
      now,
    ))
    use _ <- result.try(case historical {
      True -> auth.authorize(claims, claims.tenant, stream, domain.History, now)
      False -> Ok(Nil)
    })
    Ok(claims)
  }
  case outcome {
    Error(e) -> failure(state, id, sub, e)
    Ok(claims) ->
      authorized_command(
        State(..state, identity: Some(#(claims.subject, claims.tenant))),
        cmd,
        claims,
        cfg,
        reg,
        now,
        Some(claims),
      )
  }
}

fn scoped_control(
  state: State,
  cmd: Command,
  cfg: Config,
  reg: Registry,
  now: Int,
) -> Step {
  let sub = case cmd {
    protocol.Ack(sub, _) | protocol.Unsubscribe(_, sub) -> sub
    _ -> ""
  }
  let credentials = case dict.get(state.subscriptions, sub) {
    Ok(s) -> subscription_claims(state, s.scoped_claims)
    Error(_) -> {
      use receipt <- result.try(
        list.find(state.completed, fn(pair) { pair.0 == sub })
        |> result.replace_error(domain.Unauthenticated),
      )
      subscription_claims(state, receipt.1.scoped_claims)
    }
  }
  case credentials {
    Ok(claims) -> authorized_command(state, cmd, claims, cfg, reg, now, None)
    Error(e) -> failure(state, command_id(cmd), sub, e)
  }
}

fn refresh_subscription(
  state: State,
  id: String,
  sub: String,
  token: String,
  cfg: Config,
  now: Int,
) -> Step {
  let outcome = {
    use s <- result.try(
      dict.get(state.subscriptions, sub)
      |> result.replace_error(domain.InvalidFrame),
    )
    use claims <- result.try(verify_scoped(state, token, cfg, now))
    // An already expired lease cannot be resurrected by racing its idle tick.
    use old <- result.try(subscription_claims(state, s.scoped_claims))
    use _ <- result.try(allowed(old, s, now))
    Ok(#(s, claims))
  }
  case outcome {
    Error(e) -> failure(state, id, sub, e)
    Ok(#(s, claims)) ->
      case allowed(claims, s, now) {
        Error(e) -> {
          close_sub(s)
          let state =
            State(
              ..state,
              subscriptions: dict.delete(state.subscriptions, sub),
              completed: list.filter(state.completed, fn(pair) { pair.0 != sub }),
            )
          failure(prune_watches(state, now), id, sub, e)
        }
        Ok(_) -> {
          cancel_job(s)
          let updated =
            Subscription(
              ..s,
              scoped_claims: Some(claims),
              job: None,
              buffer: [],
              scan: None,
              held_bytes: 0,
            )
          let domain.Tenant(tenant) = claims.tenant
          success(
            State(
              ..state,
              subscriptions: dict.insert(state.subscriptions, sub, updated),
            ),
            [
              protocol.SubscriptionAuthenticated(
                id,
                sub,
                claims.subject,
                tenant,
                claims.expires_at,
              ),
            ],
          )
        }
      }
  }
}
