import exception
import gleam/erlang/process.{type Subject}
import gleam/otp/actor
import gleam/result
import highway/runtime
import highway/storage.{type Error, type Store}
import highway/storage/archive

pub type Request {
  Open(scope: String, create: Bool, reply: Subject(Reply))
  Commit(plan: archive.Plan, reply: Subject(Reply))
  OwnerDown
}

pub type Reply {
  Opened(Result(archive.Session, Error))
  Committed(
    epoch: Int,
    batch_id: String,
    result: Result(archive.Session, Error),
  )
}

/// Worker is linked and monitors its owner, including normal owner shutdown.
/// It has one outstanding operation; owner never waits on its network I/O.
pub fn start(
  store: Store,
  slots: runtime.Slots,
  parent: process.Pid,
) -> actor.StartResult(Subject(Request)) {
  actor.new_with_initialiser(1000, fn(subject) {
    let monitor = process.monitor(parent)
    actor.initialised(Nil)
    |> actor.selecting(
      process.new_selector()
      |> process.select(subject)
      |> process.select_specific_monitor(monitor, fn(_) { OwnerDown }),
    )
    |> actor.returning(subject)
    |> Ok
  })
  |> actor.on_message(fn(_, request) {
    case request {
      OwnerDown -> actor.stop()
      Open(scope, create, reply) -> {
        let outcome =
          bounded(slots, fn() { archive.open(store, scope, create) })
        process.send(reply, Opened(outcome))
        actor.continue(Nil)
      }
      Commit(plan, reply) -> {
        let outcome = bounded(slots, fn() { archive.commit(store, plan) })
        process.send(
          reply,
          Committed(plan.next.epoch, plan.next.commit_id, outcome),
        )
        actor.continue(Nil)
      }
    }
  })
  |> actor.start
}

fn bounded(
  slots: runtime.Slots,
  operation: fn() -> Result(a, Error),
) -> Result(a, Error) {
  case runtime.acquire(slots, 16) {
    False -> Error(storage.Busy)
    True -> {
      let outcome =
        exception.rescue(fn() {
          use <- exception.defer(fn() { runtime.release(slots, process.self()) })
          operation()
        })
      outcome
      |> result.unwrap(Error(storage.Transport("storage worker exception")))
    }
  }
}
