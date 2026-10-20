#!/bin/sh
set -eu
export MC_CONFIG_DIR=/tmp/mc
# Generated hex credentials need no URL escaping. Keep them out of argv/logs.
export MC_HOST_local="http://$(cat /run/secrets/minio-user):$(cat /run/secrets/minio-password)@minio:9000"
if [ "${1:-}" = initialize ]; then
  mc mb --ignore-existing local/highway >/dev/null
  mc anonymous set none local/highway >/dev/null
  echo 'Private local bucket initialized.'
else
  exec mc "$@"
fi
