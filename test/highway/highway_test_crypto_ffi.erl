%% Test-only ephemeral signing. No credentials or private fixtures are stored.
-module(highway_test_crypto_ffi).
-export([key/0, public/1, sign/3, suspend/1, resume/1, mailbox_length/1, owner_replies/1, redacted_report/1]).
key() -> jose_jwk:generate_key({okp, 'Ed25519'}).
public(Key) -> {_, Json} = jose_jwk:to_binary(jose_jwk:to_public(Key)), Json.
sign(Key, Header, Payload) ->
    {_, Signed} = jose_jws:compact(jose_jwt:sign(Key, Header, Payload)), Signed.
suspend(Pid) -> true = erlang:suspend_process(Pid), nil.
resume(Pid) -> true = erlang:resume_process(Pid), nil.
mailbox_length(Pid) -> {message_queue_len, N} = process_info(Pid, message_queue_len), N.
%% Test-only observation of the Gleam owner State tuple: replies is field 4.
%% sys:get_state is the actor's OTP introspection boundary, not a public API.
owner_replies(Pid) -> element(5, sys:get_state(Pid)).
%% A crash report may contain arbitrary session state. The production formatter
%% must discard it instead of rendering terms containing secrets or payloads.
redacted_report(Secret) -> iolist_to_binary(highway_observability_ffi:format(
    #{level => error, msg => {report, #{state => Secret}}, meta => #{}}, #{})).
