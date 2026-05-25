#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: bash run-backup-download.sh <output-dir> [rpc-port] [max-concurrent-files]" >&2
  exit 1
fi

DIR=$1
RPC_PORT=${2:-6800}
MAX_CONCURRENT_FILES=${3:-16}

SHARDS_DIR="${DIR}/shards"
SESSION_FILE="${DIR}/aria2.session"

if ! command -v aria2c >/dev/null 2>&1; then
  echo "Error: aria2c is not installed or not in PATH" >&2
  exit 1
fi

if ! [[ "${MAX_CONCURRENT_FILES}" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: max-concurrent-files must be a positive integer, got: ${MAX_CONCURRENT_FILES}" >&2
  exit 1
fi

if ! [[ "${RPC_PORT}" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: rpc-port must be a positive integer, got: ${RPC_PORT}" >&2
  exit 1
fi

mkdir -p "${SHARDS_DIR}"

#
# ARCHITECTURAL DECISIONS FOR DOWNLOAD OPTIMIZATION
#
# - This script starts one private aria2 RPC daemon per `backup-helper download`
#   invocation. The Node controller owns queueing, retries, and shutdown.
#
# - Download policy that may vary by shard size stays in RPC `addUri` options
#   inside `download.mjs` (for example `split` and `max-connection-per-server`).
#   This launcher keeps only daemon-level defaults and lifecycle settings.
#
# - `--follow-metalink=false` and `--no-want-digest-header=true` stay enabled
#   because shard URLs are plain file downloads and we want to avoid extra
#   protocol behavior that previously caused incompatibilities with R2-backed
#   endpoints.
#
# - `--save-session` is kept as extra aria2 recovery state, but `tracking.db`
#   remains the source of truth for job-level resume and retry behavior.
#
# - `--stop-with-process="${PPID}"` ensures the daemon exits if the controlling
#   `backup-helper download` process dies unexpectedly, avoiding orphan workers.
#

exec aria2c \
  --enable-rpc=true \
  --rpc-listen-all=false \
  --rpc-listen-port="${RPC_PORT}" \
  --dir="${SHARDS_DIR}" \
  --allow-overwrite=false \
  --auto-file-renaming=false \
  --follow-metalink=false \
  --no-want-digest-header=true \
  --save-session="${SESSION_FILE}" \
  --save-session-interval=60 \
  --max-concurrent-downloads="${MAX_CONCURRENT_FILES}" \
  --enable-http-pipelining=true \
  --stop-with-process="${PPID}"
