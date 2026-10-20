import highway/domain

/// Storage errors retain a safe operation cause; never include request headers,
/// credentials, response bodies, or message data in their context.
pub type Error {
  NotFound
  Precondition
  Conflict
  Ambiguous(String)
  Transport(String)
  Http(Int)
  Corrupt(String)
  Busy
  Limit
  Access(domain.Error)
}

pub type Condition {
  Absent
  Match(String)
}

pub type Object {
  Object(body: String, etag: String)
}

pub type Store {
  Store(
    get: fn(String) -> Result(Object, Error),
    put: fn(String, String, Condition) -> Result(String, Error),
  )
}

@external(erlang, "highway_s3_ffi", "sha256")
pub fn sha256(body: String) -> String

@external(erlang, "highway_s3_ffi", "hex")
pub fn hex(value: String) -> String
