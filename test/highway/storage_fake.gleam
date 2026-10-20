/// Deterministic object-store model. Test-only: atomic conditions and failpoints.
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/otp/actor
import gleam/result
import highway/storage

pub type Failure {
  Before(storage.Error)
  After(storage.Error)
}

pub type Fake {
  Fake(subject: Subject(Message), store: storage.Store, pid: process.Pid)
}

pub type Message {
  Stop(Subject(Nil))
  Get(String, Subject(Result(storage.Object, storage.Error)))
  Put(String, String, storage.Condition, Subject(Result(String, storage.Error)))
  Fail(String, Failure, Subject(Nil))
  FailRead(String, storage.Error, Subject(Nil))
  Objects(Subject(Dict(String, storage.Object)))
}

type State {
  State(
    objects: Dict(String, storage.Object),
    failures: Dict(String, Failure),
    read_failures: Dict(String, storage.Error),
    version: Int,
  )
}

pub fn new() -> Fake {
  let assert Ok(started) =
    actor.new(State(dict.new(), dict.new(), dict.new(), 0))
    |> actor.on_message(fn(s, message) {
      case message {
        Stop(reply) -> {
          process.send(reply, Nil)
          actor.stop()
        }
        Fail(key, failure, reply) -> {
          process.send(reply, Nil)
          actor.continue(
            State(..s, failures: dict.insert(s.failures, key, failure)),
          )
        }
        FailRead(key, cause, reply) -> {
          process.send(reply, Nil)
          actor.continue(
            State(..s, read_failures: dict.insert(s.read_failures, key, cause)),
          )
        }
        Objects(reply) -> {
          process.send(reply, s.objects)
          actor.continue(s)
        }
        Get(key, reply) -> {
          let outcome = case dict.get(s.read_failures, key) {
            Ok(e) -> Error(e)
            Error(_) ->
              dict.get(s.objects, key) |> result.replace_error(storage.NotFound)
          }
          process.send(reply, outcome)
          actor.continue(
            State(..s, read_failures: dict.delete(s.read_failures, key)),
          )
        }
        Put(key, body, condition, reply) -> {
          let allowed = case condition, dict.get(s.objects, key) {
            storage.Absent, Error(_) -> True
            storage.Match(expected), Ok(o) -> expected == o.etag
            _, _ -> False
          }
          let failure = dict.get(s.failures, key)
          let s = State(..s, failures: dict.delete(s.failures, key))
          case allowed, failure {
            False, _ -> {
              process.send(reply, Error(storage.Precondition))
              actor.continue(s)
            }
            True, Ok(Before(cause)) -> {
              process.send(reply, Error(cause))
              actor.continue(s)
            }
            True, _ -> {
              let tag = "\"version-" <> int.to_string(s.version + 1) <> "\""
              let outcome = case failure {
                Ok(After(cause)) -> Error(cause)
                _ -> Ok(tag)
              }
              process.send(reply, outcome)
              actor.continue(
                State(
                  ..s,
                  version: s.version + 1,
                  objects: dict.insert(
                    s.objects,
                    key,
                    storage.Object(body, tag),
                  ),
                ),
              )
            }
          }
        }
      }
    })
    |> actor.start
  let subject = started.data
  Fake(
    subject,
    storage.Store(
      fn(key) { process.call(subject, 1000, Get(key, _)) },
      fn(key, body, condition) {
        process.call(subject, 1000, Put(key, body, condition, _))
      },
    ),
    started.pid,
  )
}

pub fn fail(fake: Fake, key: String, failure: Failure) -> Nil {
  process.call(fake.subject, 1000, Fail(key, failure, _))
}

pub fn fail_read(fake: Fake, key: String, cause: storage.Error) -> Nil {
  process.call(fake.subject, 1000, FailRead(key, cause, _))
}

pub fn objects(fake: Fake) -> Dict(String, storage.Object) {
  process.call(fake.subject, 1000, Objects)
}

pub fn stop(fake: Fake) -> Nil {
  process.call(fake.subject, 1000, Stop)
}
