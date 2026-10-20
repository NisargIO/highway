import gleam/bytes_tree
import gleam/http
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}
import gleam/list
import highway/config.{type Config}
import highway/registry.{type Registry}
import highway/runtime
import highway/websocket
import mist.{type ResponseData}

pub fn handle(
  req: Request(mist.Connection),
  config: Config,
  registry: Registry,
  slots: runtime.Slots,
) -> Response(ResponseData) {
  case req.method, request.path_segments(req) {
    http.Get, ["metrics"] ->
      response.new(200)
      |> response.set_header(
        "content-type",
        "text/plain; version=0.0.4; charset=utf-8",
      )
      |> response.set_header("cache-control", "no-store")
      |> response.set_body(
        mist.Bytes(bytes_tree.from_string(registry.metrics(registry))),
      )
    http.Get, ["v1", "ws"] ->
      case registry.draining(registry) {
        True -> json(503, "{\"error\":\"draining\"}")
        False ->
          case allowed_origin(req, config.origins) && req.query == option.None {
            False -> json(403, "{\"error\":\"forbidden\"}")
            True -> {
              // Disable compression: bounded input must also bound decoded bytes.
              let req = request.set_header(req, "sec-websocket-extensions", "")
              mist.websocket(
                req,
                fn(s, m, c) { websocket.handle(s, m, c, config, registry) },
                websocket.init(_, config, slots, registry),
                websocket.cleanup(_, slots, registry),
              )
            }
          }
      }
    http.Get, ["readyz"] ->
      case config.storage {
        config.DevelopmentMemory -> basic(req)
        config.S3(_) ->
          case registry.ready(registry) {
            True ->
              json(
                200,
                "{\"status\":\"s3_async\",\"acceptance_is_durable\":false}",
              )
            False ->
              json(503, "{\"status\":\"storage_unavailable_or_backlogged\"}")
          }
      }
    _, _ -> basic(req)
  }
}

import gleam/option

pub fn allowed_origin(req: Request(body), origins: List(String)) -> Bool {
  case request.get_header(req, "origin") {
    Ok(origin) -> list.contains(origins, origin)
    Error(_) -> True
  }
}

/// Native clients still require JWTs; the TLS proxy must keep admin routes private.
pub fn basic(req: Request(body)) -> Response(ResponseData) {
  case req.method {
    http.Get ->
      case request.path_segments(req) {
        ["healthz"] -> json(200, "{\"status\":\"ok\"}")
        ["readyz"] ->
          json(503, "{\"status\":\"development_memory\",\"durable\":false}")
        _ -> json(404, "{\"error\":\"not_found\"}")
      }
    _ ->
      json(405, "{\"error\":\"method_not_allowed\"}")
      |> response.set_header("allow", "GET")
  }
}

fn json(status: Int, body: String) -> Response(ResponseData) {
  response.new(status)
  |> response.set_header("content-type", "application/json")
  |> response.set_header("cache-control", "no-store")
  |> response.set_body(mist.Bytes(bytes_tree.from_string(body)))
}
