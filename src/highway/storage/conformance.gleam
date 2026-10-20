/// Opt-in real-endpoint contract check. Writes only below a fresh test prefix;
/// no production stream head is touched and no objects/buckets are deleted.
import envoy
import gleam/erlang/process
import gleam/io
import gleam/list
import gleam/result
import highway/runtime
import highway/storage
import highway/storage/s3

pub fn check(
  store: storage.Store,
  prefix: String,
) -> Result(Nil, storage.Error) {
  let key = prefix <> "/conformance/" <> runtime.incarnation()
  use first <- result.try(store.put(key, "one", storage.Absent))
  use read <- result.try(store.get(key))
  use _ <- result.try(case read.body == "one" && read.etag == first {
    True -> Ok(Nil)
    False -> Error(storage.Corrupt("read visibility/ETag"))
  })
  use _ <- result.try(
    case store.put(key, "forbidden-overwrite", storage.Absent) {
      Error(storage.Precondition) -> Ok(Nil)
      _ -> Error(storage.Corrupt("If-None-Match not enforced"))
    },
  )
  use second <- result.try(store.put(key, "two", storage.Match(first)))
  use _ <- result.try(
    case store.put(key, "stale-overwrite", storage.Match(first)) {
      Error(storage.Precondition) -> Ok(Nil)
      _ -> Error(storage.Corrupt("If-Match not enforced"))
    },
  )
  use read <- result.try(store.get(key))
  use _ <- result.try(case read.body == "two" && read.etag == second {
    True -> Ok(Nil)
    False -> Error(storage.Corrupt("conditional write visibility"))
  })
  let race_key = key <> "-race"
  let replies = process.new_subject()
  list.each(["a", "b"], fn(body) {
    process.spawn(fn() {
      process.send(replies, store.put(race_key, body, storage.Absent))
    })
  })
  use one <- result.try(
    process.receive(replies, 15_000)
    |> result.replace_error(storage.Transport("race request deadline")),
  )
  use two <- result.try(
    process.receive(replies, 15_000)
    |> result.replace_error(storage.Transport("race request deadline")),
  )
  case one, two {
    Ok(_), Error(storage.Precondition) | Error(storage.Precondition), Ok(_) ->
      Ok(Nil)
    _, _ -> Error(storage.Corrupt("concurrent immutable write contract"))
  }
}

pub fn main() -> Nil {
  case envoy.get("HIGHWAY_S3_CONFORMANCE") {
    Ok("1") -> Nil
    _ ->
      panic as "Set HIGHWAY_S3_CONFORMANCE=1 to explicitly opt into writes under the configured test prefix"
  }
  let settings = case s3.load() {
    Ok(s) -> s
    Error(reason) -> panic as reason
  }
  case check(s3.store(settings), settings.prefix) {
    Ok(_) ->
      io.println(
        "S3 conditional writes, read visibility, opaque ETags, and concurrent immutable creation passed",
      )
    Error(reason) -> {
      let message = "S3 conformance failed: " <> storage_error(reason)
      panic as message
    }
  }
}

import gleam/string

fn storage_error(reason: storage.Error) -> String {
  string.inspect(reason)
}
