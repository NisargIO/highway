import gleam/http
import gleam/http/request
import highway/http as server

pub fn bootstrap_cannot_claim_stream_readiness_test() {
  let response = request.new() |> request.set_path("/readyz") |> server.handle
  assert response.status == 503
}

pub fn bootstrap_does_not_upgrade_unauthenticated_websocket_test() {
  let response = request.new() |> request.set_path("/v1/ws") |> server.handle
  assert response.status == 501
}

pub fn health_endpoint_rejects_mutation_test() {
  let response =
    request.new()
    |> request.set_path("/healthz")
    |> request.set_method(http.Post)
    |> server.handle
  assert response.status == 405
}
