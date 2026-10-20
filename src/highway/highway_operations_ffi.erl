%% Typed ETS boundary: keys are finite Gleam enums; activity rows are scoped to
%% live PIDs and reaped on every count. Phase is monotonic, including restarts.
-module(highway_operations_ffi).
-export([new/0, add/3, counter/2, set_phase/2, phase/1, health_set/3, health/1,
         track/3, active/2]).
new() -> ets:new(highway_operations, [set, public, {write_concurrency, true}]).
add(T, M, N) -> ets:update_counter(T, {counter,M}, {2,N}, {{counter,M},0}), nil.
counter(T, M) -> case ets:lookup(T, {counter,M}) of [{_,N}] -> N; [] -> 0 end.
set_phase(T, P) -> ets:insert_new(T, {phase,0}), advance(T,P), nil.
advance(T, P) ->
    Old = phase(T),
    case Old >= P of true -> ok; false ->
        case ets:select_replace(T, [{{phase,Old},[],[{{phase,P}}]}]) of
            1 -> ok; 0 -> advance(T,P)
        end
    end.
phase(T) -> case ets:lookup(T, phase) of [{_,P}] -> P; [] -> 0 end.
health_set(T, H, Now) -> ets:insert(T, {health,H,Now}), nil.
health(T) -> case ets:lookup(T, health) of [{_,H,N}] -> {H,N}; [] -> {false,0} end.
track(T, K, true) -> ets:insert(T, {{activity,K,self()},true}), nil;
track(T, K, false) -> ets:delete(T, {activity,K,self()}), nil.
active(T, K) ->
    Rows = ets:match_object(T, {{activity,K,'_'},true}),
    lists:foldl(fun({{activity,_,P}=Key,_},N) ->
        case is_process_alive(P) of true -> N+1; false -> ets:delete(T,Key), N end
    end, 0, Rows).
