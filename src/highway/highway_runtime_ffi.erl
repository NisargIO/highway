%% Slots is a public ETS set owned by its creating process. Connection/IO slots
%% reclaim dead owners atomically; mailbox gates use unique reference permits.
%% Mist 6 WebsocketConnection = {websocket_connection, Socket, Transport, Deflate}.
%% This slice only binds loopback TCP. Socket calls return safe OS reasons.
-module(highway_runtime_ffi).
-export([now/0, monotonic_ms/0, incarnation/0, slots/0, acquire/2, release/2,
         socket_limits/1, close/2, permit/1, release_permit/2, reset/1, set_account/4, account/1, set_missing/2, is_missing/1, reserve_bytes/2, release_bytes/2, call/3, shutdown/1, set_drained/2, is_drained/1, memory_bytes/0]).
now() -> erlang:system_time(second).
monotonic_ms() -> erlang:monotonic_time(millisecond).
incarnation() -> binary:encode_hex(crypto:strong_rand_bytes(16), lowercase).
slots() -> ets:new(highway_connections, [set, public, {write_concurrency, true}]).
acquire(Table, Limit) -> acquire_slot(Table, 1, Limit).
acquire_slot(_, N, Limit) when N > Limit -> false;
acquire_slot(Table, N, Limit) ->
    case ets:lookup(Table, N) of
        [{N, Pid}] ->
            case is_process_alive(Pid) of
                true -> acquire_slot(Table, N + 1, Limit);
                false -> ets:delete_object(Table, {N, Pid}), acquire_slot(Table, N, Limit)
            end;
        [] ->
            case ets:insert_new(Table, {N, self()}) of
                true -> true;
                false -> acquire_slot(Table, N + 1, Limit)
            end
    end.
release(Table, Pid) ->
    try ets:match_delete(Table, {'_', Pid}) catch error:badarg -> ok end, nil.
socket_limits({websocket_connection, Socket, _, _}) ->
    safe(inet:setopts(Socket, [{send_timeout, 100}, {send_timeout_close, true},
                             {sndbuf, 65536}, {buffer, 16384},
                             {high_watermark, 65536}, {low_watermark, 32768}])).
close({websocket_connection, Socket, _, _}, Code) ->
    %% RFC6455 close frame via the same encoder as Mist; compression disabled.
    Frame = gramps@websocket:encode_close_frame({custom_close_reason, Code, <<>>}, none),
    safe(gen_tcp:send(Socket, Frame)).
safe(ok) -> {ok, nil};
safe({error, Reason}) -> {error, atom_to_binary(Reason)}.
permit(Table) ->
    try permit_slot(Table, 1) catch error:badarg -> {error, nil} end.
permit_slot(_, N) when N > 128 -> {error, nil};
permit_slot(Table, N) ->
    Token = {N, make_ref()},
    case ets:insert_new(Table, Token) of
        true -> {ok, Token};
        false -> permit_slot(Table, N + 1)
    end.
release_permit(Table, Token) -> ets:delete_object(Table, Token), nil.
reset(Table) -> ets:delete_all_objects(Table), nil.

set_account(Table, Bytes, Healthy, Persisted) ->
    case ets:insert_new(Table, {account, Bytes, Healthy, Persisted, 0}) of
        true -> nil;
        false -> ets:update_element(Table, account, [{2, Bytes}, {3, Healthy}, {4, Persisted}]), nil
    end.
account(Table) ->
    try ets:lookup(Table, account) of
        [{account, Bytes, Healthy, Persisted, Queued}] -> {Bytes + Queued, Healthy, Persisted};
        [] -> {0, false, none}
    catch error:badarg -> {0, false, none} end.
set_missing(Table, Missing) -> ets:insert(Table, {missing, Missing}), nil.
is_missing(Table) ->
    try ets:lookup(Table, missing) =:= [{missing, true}]
    catch error:badarg -> false end.

reserve_bytes(Table, Bytes) ->
    try ets:update_counter(Table, account, {5, Bytes}), {ok, nil}
    catch error:badarg -> {error, nil} end.
release_bytes(Table, Bytes) -> ets:update_counter(Table, account, {5, -Bytes}), nil.
%% Gleam 1.3 Subject ABI: {subject, send_destination, tag}, named_subject atom.
%% Reply subjects are send-only aliases. OTP drops replies after unalias; they
%% cannot leak into actor unexpected-message logs after a timeout. Do not use
%% subject_owner on these private reply destinations.
call(Subject, Timeout, MakeRequest) ->
    case gleam@erlang@process:subject_owner(Subject) of
        {error, _} -> {error, <<"callee_unavailable">>};
        {ok, Pid} ->
            Alias = alias(), Tag = make_ref(), Monitor = monitor(process, Pid),
            Target = case Subject of
                {subject, _, OriginalTag} -> {subject, Pid, OriginalTag};
                {named_subject, Name} -> {subject, Pid, Name}
            end,
            try
                gleam@erlang@process:send(Target, MakeRequest({subject, Alias, Tag})),
                receive
                    {Tag, Reply} -> {ok, Reply};
                    {'DOWN', Monitor, process, _, _} -> {error, <<"callee_exited">>}
                after Timeout -> {error, <<"call_timeout">>} end
            catch _:_ -> {error, <<"call_failed">>}
            after unalias(Alias), demonitor(Monitor, [flush]), flush_reply(Tag) end
    end.
flush_reply(Tag) -> receive {Tag, _} -> flush_reply(Tag) after 0 -> ok end.
shutdown(Pid) -> exit(Pid, shutdown), nil.

set_drained(T, B) -> ets:insert(T, {drained,B}), nil.
is_drained(T) -> try ets:lookup(T, drained) of [{_,B}] -> B; [] -> false catch error:badarg -> false end.

memory_bytes() -> erlang:memory(total).
