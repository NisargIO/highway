import gleam/erlang/process

/// Distribution primitives for clustered ownership. Typed wrappers live in
/// `owner` and `relay`; these externals are deliberately generic and narrow.
@external(erlang, "highway_cluster_ffi", "self_node")
pub fn self_node() -> String

@external(erlang, "highway_cluster_ffi", "pid_node")
pub fn pid_node(pid: process.Pid) -> String

/// Start (or confirm) distribution under `node` with the shared cookie.
@external(erlang, "highway_cluster_ffi", "start")
pub fn start(node: String, cookie: String) -> Result(Nil, String)

/// Make this node's registry reachable from remote relay calls.
@external(erlang, "highway_cluster_ffi", "put_registry")
pub fn put_registry(registry: registry) -> Nil

@external(erlang, "highway_cluster_ffi", "get_registry")
pub fn get_registry() -> Result(registry, Nil)

@external(erlang, "highway_cluster_ffi", "retired_table")
pub fn retired_table() -> Nil

@external(erlang, "highway_cluster_ffi", "retire")
pub fn retire(pid: process.Pid) -> Nil

@external(erlang, "highway_cluster_ffi", "retired")
pub fn retired(pid: process.Pid) -> Bool

/// Forget retirements recorded before this monotonic millisecond.
@external(erlang, "highway_cluster_ffi", "prune_retired")
pub fn prune_retired(before_ms: Int) -> Nil
