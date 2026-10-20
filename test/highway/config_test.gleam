import highway/config

pub fn runtime_limits_are_explicit_positive_and_bounded_test() {
  assert config.decode_limits("{}") == Ok(config.defaults())
  let assert Ok(limits) =
    config.decode_limits(
      "{\"max_connections\":2,\"stream_bytes\":1024,\"node_bytes\":2048}",
    )
  assert limits.max_connections == 2
  assert limits.stream_bytes == 1024
  let assert Ok(replay) =
    config.decode_limits(
      "{\"replay_workers\":2,\"replay_pages_per_step\":1,\"replay_max_pages\":20,\"replay_timeout_ms\":1000}",
    )
  assert replay.replay_workers == 2 && replay.replay_pages_per_step == 1
  assert replay.replay_max_pages == 20 && replay.replay_timeout_ms == 1000
  let assert Error(_) = config.decode_limits("{\"replay_workers\":17}")
  let assert Error(_) = config.decode_limits("{\"replay_pages_per_step\":0}")
  let assert Error(_) = config.decode_limits("{\"replay_max_pages\":-1}")
  let assert Error(_) = config.decode_limits("{\"replay_timeout_ms\":300001}")
  let assert Error(_) = config.decode_limits("{\"tick_ms\":0}")
  let assert Error(_) = config.decode_limits("{\"subscriptions\":65}")
  let assert Error(_) = config.decode_limits("{\"max_connections\":99999}")
  let assert Error(_) = config.decode_limits("{\"node_bytes\":1}")
  let assert Error(_) = config.decode_limits("{\"auth_ms\":\"5000\"}")
}

pub fn s3_settings_require_trusted_canonical_endpoint_and_credentials_test() {
  let settings =
    s3.Settings(
      "https://s3.us-east-1.amazonaws.com",
      "bucket",
      "highway/test",
      "us-east-1",
      "test",
      "test",
      "session",
      2000,
    )
  assert s3.validate(settings) == Ok(settings)
  let assert Error(_) =
    s3.validate(s3.Settings(..settings, endpoint: "http://example.com"))
  let assert Error(_) =
    s3.validate(
      s3.Settings(..settings, endpoint: "https://user:secret@example.com"),
    )
  let assert Error(_) =
    s3.validate(
      s3.Settings(..settings, endpoint: "https://example.com?keys=remote"),
    )
  let assert Error(_) = s3.validate(s3.Settings(..settings, prefix: "../bad"))
  let assert Error(_) = s3.validate(s3.Settings(..settings, secret_key: ""))
  let assert Error(_) = s3.validate(s3.Settings(..settings, timeout_ms: 0))
}

import highway/storage/s3
