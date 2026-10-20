%% Contract: trusted public Ed25519 JWK JSON + compact JWT -> Result(payload JSON,
%% safe reason). JOSE owns parsing/signature verification; never implement crypto.
-module(highway_crypto_ffi).
-export([header/1, verify/2, valid_key/1]).
header(Token) ->
    try {ok, jose_jws:peek_protected(Token)}
    catch _:_ -> {error, <<"malformed_jws">>} end.
valid_key(Json) ->
    try
        Key = jose_jwk:from_binary(Json),
        {_, Map} = jose_jwk:to_map(Key),
        #{<<"kty">> := <<"OKP">>, <<"crv">> := <<"Ed25519">>, <<"x">> := _} = Map,
        false = maps:is_key(<<"d">>, Map),
        true = maps:get(<<"alg">>, Map, <<"EdDSA">>) =:= <<"EdDSA">>,
        true = maps:get(<<"use">>, Map, <<"sig">>) =:= <<"sig">>,
        true = lists:member(<<"verify">>, maps:get(<<"key_ops">>, Map, [<<"verify">>])),
        {ok, nil}
    catch _:_ -> {error, <<"invalid_public_ed25519_key">>} end.
verify(KeyJson, Token) ->
    try
        Key = jose_jwk:from_binary(KeyJson),
        case jose_jwt:verify_strict(Key, [<<"EdDSA">>], Token) of
            {true, Jwt, _} -> {_, Payload} = jose_jwt:to_binary(Jwt), {ok, Payload};
            {false, _, _} -> {error, <<"invalid_signature">>}
        end
    catch _:_ -> {error, <<"invalid_jws">>} end.
