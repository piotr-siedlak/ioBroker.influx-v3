#!/usr/bin/env bash
# Starts / stops InfluxDB 1.8 and 2.7 for the tests of the data import (old databases of ioBroker.influxdb).
#
#   eval "$(scripts/legacy-influx-test-server.sh start)"   → exports INFLUX1_* and INFLUX2_*
#   scripts/legacy-influx-test-server.sh stop
#
# Both servers have authentication enabled; the credentials are random test values of this run.
# InfluxDB 1.8 gets a read-only user, so the tests prove that the import never needs write access.
set -euo pipefail

V1_IMAGE="${INFLUX1_IMAGE:-influxdb:1.8}"
V2_IMAGE="${INFLUX2_IMAGE:-influxdb:2.7}"
V1_PORT="${INFLUX1_PORT:-18286}"
V2_PORT="${INFLUX2_PORT:-18287}"
V1_NAME="iob-influx1-test"
V2_NAME="iob-influx2-test"

rand() { openssl rand -hex 16; }

case "${1:-}" in
start)
    docker rm -f "$V1_NAME" "$V2_NAME" >/dev/null 2>&1 || true
    ADMIN_PW=$(rand)
    READ_PW=$(rand)
    V2_PW=$(rand)
    V2_TOKEN=$(rand)
    docker run -d --name "$V1_NAME" -p "127.0.0.1:${V1_PORT}:8086" \
        -e INFLUXDB_HTTP_AUTH_ENABLED=true \
        -e INFLUXDB_DB=iobroker \
        -e INFLUXDB_ADMIN_USER=admin -e INFLUXDB_ADMIN_PASSWORD="$ADMIN_PW" \
        -e INFLUXDB_READ_USER=reader -e INFLUXDB_READ_USER_PASSWORD="$READ_PW" \
        "$V1_IMAGE" >/dev/null
    docker run -d --name "$V2_NAME" -p "127.0.0.1:${V2_PORT}:8086" \
        -e DOCKER_INFLUXDB_INIT_MODE=setup \
        -e DOCKER_INFLUXDB_INIT_USERNAME=admin -e DOCKER_INFLUXDB_INIT_PASSWORD="$V2_PW" \
        -e DOCKER_INFLUXDB_INIT_ORG=iob -e DOCKER_INFLUXDB_INIT_BUCKET=iobroker \
        -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN="$V2_TOKEN" \
        "$V2_IMAGE" >/dev/null
    for _ in $(seq 1 60); do
        if curl -fsS "http://127.0.0.1:${V1_PORT}/ping" >/dev/null 2>&1 &&
            curl -fsS -H "Authorization: Token ${V2_TOKEN}" "http://127.0.0.1:${V2_PORT}/api/v2/buckets?name=iobroker" 2>/dev/null | grep -q '"name": *"iobroker"'; then
            break
        fi
        sleep 1
    done
    # the 1.8 image creates the users in an init step after the first start: wait until the reader can query
    for _ in $(seq 1 60); do
        curl -fsS -u "reader:${READ_PW}" "http://127.0.0.1:${V1_PORT}/query?db=iobroker" --data-urlencode "q=SHOW MEASUREMENTS" >/dev/null 2>&1 && break
        sleep 1
    done
    echo "export INFLUX1_URL=http://127.0.0.1:${V1_PORT}"
    echo "export INFLUX1_DB=iobroker"
    echo "export INFLUX1_ADMIN_USER=admin"
    echo "export INFLUX1_ADMIN_PASSWORD=${ADMIN_PW}"
    echo "export INFLUX1_READ_USER=reader"
    echo "export INFLUX1_READ_PASSWORD=${READ_PW}"
    echo "export INFLUX2_URL=http://127.0.0.1:${V2_PORT}"
    echo "export INFLUX2_ORG=iob"
    echo "export INFLUX2_BUCKET=iobroker"
    echo "export INFLUX2_TOKEN=${V2_TOKEN}"
    ;;
stop)
    docker rm -f "$V1_NAME" "$V2_NAME" >/dev/null 2>&1 || true
    ;;
*)
    echo "usage: $0 start|stop" >&2
    exit 2
    ;;
esac
