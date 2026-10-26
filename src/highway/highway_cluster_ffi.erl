%% Erlang distribution boundary for clustered ownership. Remote entry points
%% are fixed MFAs (never closures) so nodes need only the same module exports.
%% Node names reaching this module were validated by config:node_name/1, so
%% the atom table grows only with the bounded set of cluster node names.
-module(highway_cluster_ffi).
-export([self_node/0, pid_node/1, start/2, attach/4, append/4,
         put_registry/1, get_registry/0, put_upstream/2, get_upstream/1,
         retired_table/0, retire/1, retired/1, prune_retired/1]).

self_node() -> atom_to_binary(node()).
pid_node(Pid) -> atom_to_binary(node(Pid)).

%% Start distribution unless the VM was booted with -name already. The cookie
%% is applied before any connection can be attempted.
start(Name, Cookie) ->
    Node = binary_to_atom(Name),
    case node() of
        Node -> erlang:set_cookie(binary_to_atom(Cookie)), {ok, nil};
        nonode@nohost ->
            _ = os:cmd("epmd -daemon"),
            case net_kernel:start(Node, #{name_domain => longnames, hidden => false}) of
                {ok, _} -> erlang:set_cookie(binary_to_atom(Cookie)), {ok, nil};
                {error, _} -> {error, <<"distribution failed to start">>}
            end;
        _ -> {error, <<"VM already distributed under a different node name">>}
    end.

%% Result(Result(#(Owner, Pid), Error), Nil): the outer error means transport.
attach(Node, Tenant, Stream, Feed) ->
    remote(Node, 'highway@relay', attach, [Tenant, Stream, Feed], 3000).
append(Node, Owner, Id, Data) ->
    remote(Node, 'highway@owner', append_here, [Owner, Id, Data], 2000).
remote(Node, M, F, A, Timeout) ->
    try erpc:call(binary_to_atom(Node), M, F, A, Timeout) of R -> {ok, R}
    catch _:_ -> {error, nil} end.

put_registry(R) -> persistent_term:put({highway, registry}, R), nil.
get_registry() ->
    try {ok, persistent_term:get({highway, registry})}
    catch error:badarg -> {error, nil} end.

put_upstream(Table, Owner) -> ets:insert(Table, {upstream, Owner}), nil.
get_upstream(Table) ->
    try ets:lookup(Table, upstream) of [{upstream, O}] -> {ok, O}; [] -> {error, nil}
    catch error:badarg -> {error, nil} end.

%% Owners that exit on purpose (slept, idle mirror) are recorded for a grace
%% period so connections drop their progress watches instead of treating the
%% exit as a failure. The named table belongs to the long-lived registry.
retired_table() ->
    case ets:info(highway_retired) of
        undefined -> ets:new(highway_retired, [named_table, public, set]), nil;
        _ -> nil
    end.
retire(Pid) ->
    try ets:insert(highway_retired, {Pid, erlang:monotonic_time(millisecond)}), nil
    catch error:badarg -> nil end.
retired(Pid) ->
    try ets:member(highway_retired, Pid) catch error:badarg -> false end.
prune_retired(Before) ->
    try ets:select_delete(highway_retired, [{{'_', '$1'}, [{'<', '$1', Before}], [true]}]), nil
    catch error:badarg -> nil end.
