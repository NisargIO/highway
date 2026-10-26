import gleam/erlang/process.{type Pid, type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import highway/cluster
import highway/domain.{
  type Cursor, type Error, type Event, type Stream, type Tenant,
}
import highway/owner.{type Feed, type Owner}
import highway/protocol
import highway/registry
import highway/replay/view

const batch_limit = 256

const idle_poll_ms = 10

/// Remote entry point, executed on the lease holder's node through erpc.
/// Returns this node's primary owner and a relay agent feeding `feed`. A
/// mirror is never returned: relays always attach to the ordering owner.
pub fn attach(
  tenant: Tenant,
  stream: Stream,
  feed: Subject(Feed),
) -> Result(#(Owner, Pid), Error) {
  use registry <- result.try(
    cluster.get_registry() |> result.replace_error(domain.Unavailable),
  )
  use found <- result.try(registry.find(registry, tenant, stream))
  let primary = found.0
  case owner.forwarding(primary) {
    True -> Error(domain.Unavailable)
    False -> {
      let agent = process.spawn_unlinked(fn() { agent(primary, feed) })
      Ok(#(primary, agent))
    }
  }
}

type Signal {
  Gone
}

/// Pulls committed-or-pending events from the local primary in order and
/// pushes them to one remote mirror in bounded batches. It exits when either
/// side goes down; the mirror treats that as an owner failure.
fn agent(primary: Owner, feed: Subject(Feed)) -> Nil {
  let downs =
    process.new_selector()
    |> process.select_specific_monitor(
      process.monitor(owner.pid(primary)),
      fn(_) { Gone },
    )
  let downs = case process.subject_owner(feed) {
    Ok(pid) ->
      process.select_specific_monitor(downs, process.monitor(pid), fn(_) {
        Gone
      })
    Error(_) -> downs
  }
  case start(primary) {
    Error(_) -> Nil
    Ok(#(view, at)) -> {
      process.send(feed, owner.Begin(at))
      run(primary, feed, view, downs, at, None)
    }
  }
}

/// Mirror floor: the primary's persisted cursor. Everything after it is still
/// in the primary's memory (pending is never discarded before a head commit).
fn start(primary: Owner) -> Result(#(view.View, Cursor), Error) {
  use tail <- result.try(owner.watermark(primary))
  let at = case owner.stats(primary).2 {
    Some(c) if c.incarnation == tail.incarnation && c.epoch == tail.epoch -> c
    _ -> domain.Cursor(tail.incarnation, tail.epoch, 0)
  }
  use v <- result.try(owner.capture_view(
    primary,
    protocol.After(at),
    process.self(),
  ))
  Ok(#(v, at))
}

fn run(
  primary: Owner,
  feed: Subject(Feed),
  v: view.View,
  downs: process.Selector(Signal),
  at: Cursor,
  sent: Option(Cursor),
) -> Nil {
  case collect(primary, v, at, []) {
    Error(_) -> owner.release(primary, v)
    Ok(#(events, restart)) -> {
      let at = case list.last(events) {
        Ok(e) -> {
          process.send(feed, owner.Events(events))
          e.cursor
        }
        Error(_) -> at
      }
      // Fell behind the primary's live cache: those events are archived now.
      let at = case restart {
        Some(floor) -> {
          process.send(feed, owner.Begin(floor))
          floor
        }
        None -> at
      }
      let sent = case owner.stats(primary).2 {
        Some(c) if c.offset <= at.offset && Some(c) != sent -> {
          process.send(feed, owner.Persisted(c))
          Some(c)
        }
        _ -> sent
      }
      let wait = case events {
        [] -> idle_poll_ms
        _ -> 0
      }
      case process.selector_receive(downs, wait) {
        Ok(Gone) -> owner.release(primary, v)
        Error(_) -> run(primary, feed, v, downs, at, sent)
      }
    }
  }
}

fn collect(
  primary: Owner,
  v: view.View,
  at: Cursor,
  acc: List(Event),
) -> Result(#(List(Event), Option(Cursor)), Error) {
  case list.length(acc) >= batch_limit {
    True -> Ok(#(list.reverse(acc), None))
    False -> {
      use pulled <- result.try(owner.pull(primary, v, at))
      case pulled.next {
        view.Available(e) -> collect(primary, v, e.cursor, [e, ..acc])
        view.AtTail -> Ok(#(list.reverse(acc), None))
        view.Archived ->
          case acc, owner.stats(primary).2 {
            [], Some(c) if c.offset > at.offset -> Ok(#([], Some(c)))
            [], _ -> Error(domain.Unavailable)
            _, _ -> Ok(#(list.reverse(acc), None))
          }
      }
    }
  }
}
