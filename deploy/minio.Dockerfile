# Local development only. Upstream community images were withdrawn; build the
# unmodified archived sources instead. Keep source hashes and AGPL licenses.
FROM golang:1.26-alpine@sha256:9ce231b28998b32e116927c61aa5c406183061d4f3f2d61b292c88e7f6bdf7ca AS minio-build
WORKDIR /src
ADD --checksum=sha256:71794c2df26aad0cc99e8421c58b7aa2dd55969f979b0e7d1e931042e9fabcd6 https://codeload.github.com/minio/minio/tar.gz/7aac2a2c5b7c882e68c1ce017d8256be2feea27f /tmp/source.tar.gz
RUN tar -xzf /tmp/source.tar.gz --strip-components=1
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -o /out/minio .

FROM golang:1.26-alpine@sha256:9ce231b28998b32e116927c61aa5c406183061d4f3f2d61b292c88e7f6bdf7ca AS mc-build
WORKDIR /src
ADD --checksum=sha256:167415edd21bc29f5360943dac64272aa5cda0a39f3070b15cfeca671c43d975 https://codeload.github.com/minio/mc/tar.gz/77f82e18b5401a65958f1619df6ebb994634bd88 /tmp/source.tar.gz
RUN tar -xzf /tmp/source.tar.gz --strip-components=1
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -o /out/mc .

FROM alpine:3.23@sha256:85fe1e81d6758c208f3e1eed4338a1997e19d4be002d4dd32d3100c9a8c010a0 AS base
RUN apk add --no-cache ca-certificates \
    && addgroup -g 10001 minio && adduser -D -H -u 10001 -G minio minio \
    && mkdir /data && chown 10001:10001 /data
USER 10001:10001

FROM base AS minio
COPY --from=minio-build /out/minio /usr/local/bin/minio
COPY --from=minio-build /src/LICENSE /licenses/minio-LICENSE
ENTRYPOINT ["minio"]

FROM base AS mc
COPY --from=mc-build /out/mc /usr/local/bin/mc
COPY --from=mc-build /src/LICENSE /licenses/mc-LICENSE
COPY deploy/minio-client.sh /usr/local/bin/minio-client
ENTRYPOINT ["/bin/sh", "/usr/local/bin/minio-client"]
