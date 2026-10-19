#!/usr/bin/env bash
# Starts / stops an isolated InfluxDB 3 Core for the integration tests.
#
#   scripts/influx3-test-server.sh start   → prints the environment to export
#   scripts/influx3-test-server.sh stop
#
# The server keeps its data in memory only and listens on 127.0.0.1:${INFLUX3_PORT:-18282}.
# A second container without authentication (port +1) is started for the ioBroker harness tests.
set -euo pipefail

IMAGE="${INFLUX3_IMAGE:-influxdb:3-core}"
PORT="${INFLUX3_PORT:-18282}"
NOAUTH_PORT=$((PORT + 1))
NAME="iob-influx3-test"
NOAUTH_NAME="iob-influx3-test-noauth"

case "${1:-}" in
start)
    docker rm -f "$NAME" "$NOAUTH_NAME" >/dev/null 2>&1 || true
    docker run -d --name "$NAME" -p "127.0.0.1:${PORT}:8181" "$IMAGE" \
        influxdb3 serve --node-id test --object-store memory >/dev/null
    docker run -d --name "$NOAUTH_NAME" -p "127.0.0.1:${NOAUTH_PORT}:8181" "$IMAGE" \
        influxdb3 serve --node-id test --object-store memory --without-auth >/dev/null
    TOKEN=""
    for _ in $(seq 1 30); do
        OUT=$(docker exec "$NAME" influxdb3 create token --admin 2>&1 || true)
        TOKEN=$(printf '%s\n' "$OUT" | grep -oE 'apiv3_[A-Za-z0-9_-]+' | head -1 || true)
        [ -n "$TOKEN" ] && break
        sleep 1
    done
    if [ -z "$TOKEN" ]; then
        echo "could not create a token" >&2
        exit 1
    fi
    for _ in $(seq 1 30); do
        curl -fsS "http://127.0.0.1:${NOAUTH_PORT}/health" >/dev/null 2>&1 && break
        sleep 1
    done
    echo "export INFLUX3_URL=http://127.0.0.1:${PORT}"
    echo "export INFLUX3_TOKEN=${TOKEN}"
    echo "export INFLUX3_CONTAINER=${NAME}"
    echo "export INFLUX3_NOAUTH_URL=http://127.0.0.1:${NOAUTH_PORT}"
    ;;
stop)
    docker rm -f "$NAME" "$NOAUTH_NAME" >/dev/null 2>&1 || true
    ;;
*)
    echo "usage: $0 start|stop" >&2
    exit 2
    ;;
esac
