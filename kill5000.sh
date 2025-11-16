#!/usr/bin/env bash

# Kill any process listening on TCP port 5000.
# Works with either lsof or fuser. Requires no interactive confirmation.
set -euo pipefail

PORT=5000

echo "Checking for processes using TCP port ${PORT}..."

kill_with_lsof() {
  if command -v lsof >/dev/null 2>&1; then
    PIDS=$(lsof -t -i TCP:${PORT} -sTCP:LISTEN || true)
    if [ -n "${PIDS}" ]; then
      echo "Found PIDs via lsof: ${PIDS}"
      # shellcheck disable=SC2086
      kill -9 ${PIDS} || true
      echo "Killed processes on port ${PORT} using lsof."
      return 0
    fi
  fi
  return 1
}

kill_with_fuser() {
  if command -v fuser >/dev/null 2>&1; then
    # -k: kill, -n tcp: TCP namespace, -9: SIGKILL
    if fuser -n tcp ${PORT} >/dev/null 2>&1; then
      fuser -k -n tcp -9 ${PORT} || true
      echo "Killed processes on port ${PORT} using fuser."
      return 0
    fi
  fi
  return 1
}

if kill_with_lsof; then
  exit 0
fi

if kill_with_fuser; then
  exit 0
fi

echo "No process found listening on TCP port ${PORT}."
exit 0


