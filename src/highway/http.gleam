import gleam/bytes_tree
import gleam/http
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}
import mist.{type ResponseData}

/// Bootstrap routes. Stream readiness stays false until storage and auth exist.
pub fn handle(req: Request(body)) -> Response(ResponseData) {
  case req.method {
    http.Get -> get(request.path_segments(req))
    _ ->
      json(405, "{\"error\":\"method_not_allowed\"}")
      |> response.set_header("allow", "GET")
  }
}

fn get(path: List(String)) -> Response(ResponseData) {
  case path {
    ["healthz"] -> json(200, "{\"status\":\"ok\"}")
    ["readyz"] -> json(503, "{\"status\":\"not_implemented\"}")
    ["v1", "ws"] -> json(501, "{\"error\":\"not_implemented\"}")
    _ -> json(404, "{\"error\":\"not_found\"}")
  }
}

fn json(status: Int, body: String) -> Response(ResponseData) {
  response.new(status)
  |> response.set_header("content-type", "application/json")
  |> response.set_header("cache-control", "no-store")
  |> response.set_body(mist.Bytes(bytes_tree.from_string(body)))
}
