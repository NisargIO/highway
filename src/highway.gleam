import gleam/erlang/process
import gleam/string
import highway/http
import logging
import mist

pub fn main() -> Nil {
  logging.configure()
  case
    mist.new(http.handle)
    |> mist.bind("127.0.0.1")
    |> mist.port(4000)
    |> mist.start
  {
    Ok(_server) -> process.sleep_forever()
    Error(reason) -> {
      let message = "HTTP server failed to start: " <> string.inspect(reason)
      panic as message
    }
  }
}
