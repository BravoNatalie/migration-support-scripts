#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: bash run-cars-download.sh <download-dir> [max-concurrent-files]" >&2
  exit 1
fi

DOWNLOAD_DIR=$1
MAX_CONCURRENT_FILES=${2:-16}
ARIA2_INPUT="${DOWNLOAD_DIR}/download.aria2"
SESSION_FILE="${DOWNLOAD_DIR}/aria2.session"
CARS_DIR="${DOWNLOAD_DIR}/cars"

if ! command -v aria2c >/dev/null 2>&1; then
  echo "Error: aria2c is not installed or not in PATH" >&2
  exit 1
fi

if [[ ! -f "${ARIA2_INPUT}" ]]; then
  echo "Error: missing aria2 input file: ${ARIA2_INPUT}" >&2
  exit 1
fi

if ! [[ "${MAX_CONCURRENT_FILES}" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: max-concurrent-files must be a positive integer, got: ${MAX_CONCURRENT_FILES}" >&2
  exit 1
fi

mkdir -p "${CARS_DIR}"

# ==============================================================================
# ARCHITECTURAL DECISIONS FOR DOWNLOAD OPTIMIZATION
# 
# Context: 
# Storacha splits uploaded content into maximum 134 MB shards, 
# which are hosted in a Cloudflare R2 bucket.
# 
# Optimization Strategy:
# 1. Disable Intra-file Multi-threading (--split=1, --max-connection-per-server=1)
#    Standard tools optimize speed by slicing a single file via HTTP Range headers. 
#    Because these shards are small-to-medium sized, we force a 1:1 ratio.
# 
# 2. Horizontal Bandwidth Saturation (--max-concurrent-downloads)
#    Instead of downloading one file with multiple threads, maximize bandwidth 
#    by downloading multiple completely distinct shards simultaneously.
# 
# 3. Latency Mitigation via Pipelining (--enable-http-pipelining=true)
#    Eliminates network latency between sequential requests by batching subsequent 
#    shard requests over Cloudflare’s native HTTP/2 architecture, without waiting 
#    for previous transfers to finish.
# ==============================================================================

exec aria2c \
  --dir="${CARS_DIR}" \
  --input-file="${ARIA2_INPUT}" \
  --continue=true \
  --allow-overwrite=false \
  --auto-file-renaming=false \
  --save-session="${SESSION_FILE}" \
  --save-session-interval=60 \
  --max-concurrent-downloads="${MAX_CONCURRENT_FILES}" \
  --split=1\ 
  --max-connection-per-server=1\ 
  --enable-http-pipelining=true

