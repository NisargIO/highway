import gleam/erlang/process
import highway/domain.{type Cursor, type Event}

/// A server-created lease. Its exclusive floor never changes when data moves
/// between pending memory and the archive. Latest access cannot read before H.
pub type View {
  View(id: String, high: Cursor, floor: Cursor, historical: Bool)
}

pub type Lease {
  Lease(view: View, connection: process.Pid)
}

pub type Next {
  Available(Event)
  AtTail
  Archived
}

pub type Pull {
  Pull(tail: Cursor, next: Next)
}
