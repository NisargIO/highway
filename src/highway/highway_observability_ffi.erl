%% Only explicitly safe service messages may contain free text. OTP/Mist crash
%% reports can contain JWT/session/event state, so never serialize their terms.
-module(highway_observability_ffi).
-export([configure/0, write/2, format/2]).
configure() ->
    ok = logger:set_primary_config(level, info),
    ok = logger:set_handler_config(default, formatter, {?MODULE, #{}}), nil.
write(Level, Json) -> logger:log(Level, Json, #{highway_safe => true}), nil.
format(#{msg := {string, Text}, meta := #{highway_safe := true}}, _) -> [Text, $\n];
format(#{level := Level}, _) ->
    [<<"{\"event\":\"runtime_report\",\"level\":\"">>, atom_to_binary(Level), <<"\"}\n">>].
