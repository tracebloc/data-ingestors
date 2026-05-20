#!/bin/sh
# Wait for MySQL, then hand off to the YAML-driven entrypoint registered by
# setup.py (`tracebloc-ingest`, defined in #44). The Helm subchart (client#86)
# mounts ingest.yaml and sets INGEST_CONFIG to its path; this script does not
# need to know about that — it just exec's the console script.

set -e

if [ -z "$MYSQL_HOST" ]; then
  echo "ERROR: MYSQL_HOST must be set (the tracebloc client provides this)" >&2
  exit 64
fi

# Bound the MySQL wait so a misconfigured client surfaces a clear failure
# instead of hanging the Job indefinitely.
WAIT_SECONDS="${MYSQL_WAIT_SECONDS:-120}"
echo "Waiting up to ${WAIT_SECONDS}s for MySQL at ${MYSQL_HOST}:3306..."
i=0
until nc -z "$MYSQL_HOST" 3306; do
  i=$((i + 1))
  if [ "$i" -ge "$WAIT_SECONDS" ]; then
    echo "ERROR: MySQL not reachable at ${MYSQL_HOST}:3306 after ${WAIT_SECONDS}s" >&2
    exit 65
  fi
  sleep 1
done
echo "MySQL is ready."

# exec so the Python process becomes PID 1 and Kubernetes signal handling
# (SIGTERM on Pod deletion, etc.) reaches the application directly.
exec tracebloc-ingest "$@"
