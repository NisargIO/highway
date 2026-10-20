# Tested native shipments use OTP 29. Build this image on Linux CI before release.
FROM ghcr.io/gleam-lang/gleam:v1.19.2-erlang-alpine@sha256:64b1df35a9611332d51f77c97f16f50ec97863162ba8824400ed4c8dff6378ac AS build
WORKDIR /app
RUN apk add --no-cache python3 patch make
ENV ERL_COMPILER_OPTIONS="[nowarn_deprecated_catch]"
COPY gleam.toml manifest.toml ./
RUN gleam deps download
COPY patches ./patches
COPY scripts/patch-mist.py ./scripts/patch-mist.py
RUN python3 scripts/patch-mist.py
COPY src ./src
RUN test "$(erl -noshell -eval 'io:format("~s",[erlang:system_info(otp_release)]),halt().')" = 29 \
    && gleam export erlang-shipment

FROM erlang:29.1.2-alpine@sha256:cd43a9f16a766e71440ee802a857a3536eb63ac1d8488814d8e5dcebd9c3e7d7 AS runtime
RUN apk add --no-cache ca-certificates libstdc++ \
    && addgroup -g 10001 highway && adduser -D -H -u 10001 -G highway highway
WORKDIR /app
COPY --from=build --chown=10001:10001 /app/build/erlang-shipment/ ./
USER 10001:10001
ENV HIGHWAY_BIND=0.0.0.0 HIGHWAY_PORT=4000 HIGHWAY_STORAGE_MODE=s3_async ERL_CRASH_DUMP=/dev/null
EXPOSE 4000
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:4000/healthz || exit 1
ENTRYPOINT ["/app/entrypoint.sh", "run"]
