%% OTP gen_event signal bridge only. All drain policy and deadlines are Gleam.
%% The OS handler emits once, so repeated SIGTERM cannot grow a mailbox.
-module(highway_lifecycle_ffi).
-behaviour(gen_event).
-export([install/1, exit/1, init/1, handle_event/2, handle_info/2,
         handle_call/2, terminate/2, code_change/3]).
install(Subject) ->
    try
        ok = gen_event:swap_handler(erl_signal_server,
            {erl_signal_handler, []}, {?MODULE, Subject}),
        ok = os:set_signal(sigterm, handle),
        {ok, nil}
    catch _:_ -> {error, <<"signal handler installation failed">>} end.
init({Subject, _OldState}) -> {ok, {Subject,false}}.
handle_event(sigterm, {Subject,false}) ->
    gleam@erlang@process:send(Subject, stop), {ok, {Subject,true}};
handle_event(_, State) -> {ok, State}.
handle_info(_, State) -> {ok, State}.
handle_call(_, State) -> {ok, ok, State}.
terminate(_, _) -> ok.
code_change(_, State, _) -> {ok, State}.
exit(Code) -> init:stop(Code), receive after infinity -> nil end.
