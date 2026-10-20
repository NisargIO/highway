import gleam/erlang/process
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import highway/config.{type Config}
import highway/domain
import highway/observability as logging
import highway/operations
import highway/protocol
import highway/registry.{type Registry}
import highway/replay
import highway/runtime
import highway/session
import mist

pub type Tick {
  Tick
  ReplayResult(replay.Reply)
}

pub type State {
  State(
    session: session.State,
    subject: process.Subject(Tick),
    timer: process.Timer,
    admitted: Bool,
    context: operations.Context,
  )
}

pub fn init(
  connection: mist.WebsocketConnection,
  config: Config,
  slots: runtime.Slots,
  registry: Registry,
) -> #(State, Option(process.Selector(Tick))) {
  let admitted =
    !registry.draining(registry)
    && runtime.acquire(slots, config.limits.max_connections)
  case admitted {
    True -> {
      operations.activity(
        registry.context(registry),
        operations.Connection,
        True,
      )
      operations.add(registry.context(registry), operations.ConnectionsTotal, 1)
    }
    False -> Nil
  }
  let subject = process.new_subject()
  let timer = process.send_after(subject, config.limits.tick_ms, Tick)
  let configured = case runtime.socket_limits(connection) {
    Error(reason) -> {
      logging.log(logging.Error, "Socket configuration failed: " <> reason)
      False
    }
    Ok(_) ->
      case
        send(connection, [
          case config.storage {
            config.DevelopmentMemory -> protocol.Hello(config.limits.auth_ms)
            config.S3(_) -> protocol.S3Hello(config.limits.auth_ms)
          },
        ])
      {
        Error(_) -> False
        Ok(_) -> True
      }
  }
  let state =
    State(
      session.new(runtime.monotonic_ms()),
      subject,
      timer,
      admitted && configured,
      registry.context(registry),
    )
  #(
    state,
    Some(
      process.new_selector()
      |> process.select(subject)
      |> process.select_map(state.session.replies, ReplayResult),
    ),
  )
}

pub fn handle(
  state: State,
  message: mist.WebsocketMessage(Tick),
  connection: mist.WebsocketConnection,
  config: Config,
  registry: Registry,
) -> mist.Next(State, Tick) {
  case operations.closing(registry.context(registry)) {
    True ->
      apply(
        state,
        session.disconnect(state.session, domain.Unavailable, 1012),
        connection,
      )
    False ->
      case state.admitted {
        False ->
          apply(
            state,
            session.disconnect(state.session, domain.RateLimited, 1008),
            connection,
          )
        True ->
          case message {
            mist.Custom(Tick) -> {
              let step =
                session.tick(
                  state.session,
                  config,
                  runtime.now(),
                  runtime.monotonic_ms(),
                )
              let timer =
                process.send_after(state.subject, config.limits.tick_ms, Tick)
              apply(State(..state, timer: timer), step, connection)
            }
            mist.Custom(ReplayResult(reply)) ->
              apply(
                state,
                session.replayed(
                  state.session,
                  reply,
                  config,
                  runtime.now(),
                  runtime.monotonic_ms(),
                ),
                connection,
              )
            mist.Text(frame) -> {
              let limit = case state.session.claims {
                None -> 16_384
                Some(_) -> 262_144
              }
              let step = case string.byte_size(frame) > limit {
                True ->
                  session.disconnect(state.session, domain.FrameTooLarge, 1009)
                False ->
                  case protocol.decode(frame) {
                    Error(e) -> session.disconnect(state.session, e, 1008)
                    Ok(protocol.Auth(id, token)) ->
                      case string.byte_size(frame) > 16_384 {
                        True ->
                          session.disconnect(
                            state.session,
                            domain.FrameTooLarge,
                            1009,
                          )
                        False ->
                          session.handle(
                            state.session,
                            protocol.Auth(id, token),
                            config,
                            registry,
                            runtime.now(),
                            runtime.monotonic_ms(),
                          )
                      }
                    Ok(cmd) ->
                      session.handle(
                        state.session,
                        cmd,
                        config,
                        registry,
                        runtime.now(),
                        runtime.monotonic_ms(),
                      )
                  }
              }
              apply(state, step, connection)
            }
            mist.Binary(_) ->
              apply(
                state,
                session.disconnect(state.session, domain.InvalidFrame, 1008),
                connection,
              )
            mist.Closed | mist.Shutdown -> mist.stop()
          }
      }
  }
}

fn send(
  connection: mist.WebsocketConnection,
  responses: List(protocol.Response),
) -> Result(Nil, Nil) {
  list.try_each(responses, fn(frame) {
    mist.send_text_frame(connection, protocol.encode(frame))
    |> result.map_error(fn(reason) {
      logging.log(
        logging.Warning,
        "WebSocket write failed: " <> string.inspect(reason),
      )
    })
  })
}

fn apply(
  state: State,
  step: session.Step,
  connection: mist.WebsocketConnection,
) -> mist.Next(State, Tick) {
  list.each(step.responses, fn(frame) {
    case frame {
      protocol.Failure(_, _, domain.TokenExpired)
      | protocol.Failure(_, _, domain.Forbidden)
      | protocol.Failure(_, _, domain.Unauthenticated) ->
        operations.add(state.context, operations.AuthFailures, 1)
      protocol.Failure(_, _, domain.SlowConsumer) ->
        operations.add(state.context, operations.SlowConsumers, 1)
      _ -> Nil
    }
  })
  case send(connection, step.responses) {
    Error(_) -> mist.stop()
    Ok(_) ->
      case step.close {
        None -> mist.continue(State(..state, session: step.state))
        Some(code) -> {
          case runtime.close(connection, code) {
            Ok(_) -> Nil
            Error(reason) ->
              logging.log(logging.Warning, "WebSocket close failed: " <> reason)
          }
          mist.stop()
        }
      }
  }
}

pub fn cleanup(state: State, slots: runtime.Slots, registry: Registry) -> Nil {
  operations.activity(registry.context(registry), operations.Connection, False)
  session.cleanup(state.session)
  let _ = process.cancel_timer(state.timer)
  runtime.release(slots, process.self())
}
