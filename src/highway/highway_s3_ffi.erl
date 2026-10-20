%% Typed Settings tuple -> bounded HTTP response or safe storage.Error.
%% SigV4 is delegated to maintained aws_signature; OTP owns HTTP/TLS/SHA256.
%% All object keys are already canonical ASCII (Gleam encodes tenant/stream IDs).
-module(highway_s3_ffi).
-export([request/5, sha256/1, hex/1]).
sha256(Body) -> hex(crypto:hash(sha256, Body)).
hex(Value) -> binary:encode_hex(Value, lowercase).
request(Settings = {settings, _, _, _, _, _, _, _, Timeout}, Method, Key, Body, Headers) ->
    %% Isolate httpc's receives so late replies cannot enter the owner's worker
    %% mailbox or be printed as unexpected messages by the Gleam actor.
    Parent = self(), Ref = make_ref(),
    Child = spawn_link(fun() -> Parent ! {Ref, do_request(Settings, Method, Key, Body, Headers)} end),
    receive {Ref, Result} -> Result
    after Timeout + 2000 -> unlink(Child), exit(Child, kill), error_for(Method, <<"request_deadline">>) end.
do_request({settings, Endpoint, Bucket, _Prefix, Region, Access, Secret, Token, Timeout}, Method, Key, Body, Headers) ->
    try
        {ok, _} = application:ensure_all_started(inets),
        {ok, _} = application:ensure_all_started(ssl),
        URL = <<Endpoint/binary, "/", Bucket/binary, "/", Key/binary>>,
        Parsed = uri_string:parse(URL),
        #{host := Host0} = Parsed,
        Host = case Parsed of
            #{port := Port} -> <<Host0/binary, ":", (integer_to_binary(Port))/binary>>;
            _ -> Host0
        end,
        Extra = case Token of <<>> -> []; _ -> [{<<"x-amz-security-token">>, Token}] end,
        Signed = aws_signature:sign_v4(Access, Secret, Region, <<"s3">>, calendar:universal_time(),
                    Method, URL, [{<<"host">>, Host} | Extra ++ Headers], Body, [{uri_encode_path, false}]),
        H = [{binary_to_list(K), binary_to_list(V)} || {K,V} <- Signed],
        Req = case Method of
            <<"GET">> -> {binary_to_list(URL), H};
            <<"PUT">> -> {binary_to_list(URL), H, "application/json", Body}
        end,
        Verb = case Method of <<"GET">> -> get; <<"PUT">> -> put end,
        HttpOpts = [{timeout, Timeout}, {connect_timeout, Timeout}, {autoredirect, false},
                    {ssl, [{verify, verify_peer}, {cacerts, public_key:cacerts_get()},
                           {customize_hostname_check, [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]}]}],
        %% OTP 29 httpc source documents both limits; they apply to error bodies
        %% too, before the entire response is accumulated.
        case httpc:request(Verb, Req, HttpOpts, [{body_format, binary},
                           {max_body_size, 2097152}, {max_header_size, 16384}]) of
            {ok, {{_, Code, _}, ResponseHeaders, ResponseBody}} ->
                case unicode:characters_to_binary(ResponseBody, utf8, utf8) of
                    Utf8 when is_binary(Utf8) -> {ok, {Code, normalize(ResponseHeaders), Utf8}};
                    _ -> {error, {corrupt, <<"non_utf8_object">>}}
                end;
            {error, _} -> error_for(Method, <<"httpc_request_failed">>)
        end
    catch _:_ -> error_for(Method, <<"httpc_or_signer_failed">>) end.
error_for(<<"PUT">>, Reason) -> {error, {ambiguous, Reason}};
error_for(_, Reason) -> {error, {transport, Reason}}.
normalize(Headers) -> [{list_to_binary(string:lowercase(K)), iolist_to_binary(V)} || {K,V} <- Headers].
