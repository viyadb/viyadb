#!/bin/bash

if ! which docker >/dev/null 2>&1; then
  echo "ERROR: Docker is not installed"
  exit 1
fi

start_consul() {
  docker run --rm -d --name viyadb-consul -p 8500:8500 consul >/dev/null
  cat consul-backup.json | docker exec -i viyadb-consul consul kv import - >/dev/null
}

stop_consul() {
  docker exec -i viyadb-consul consul kv export > consul-backup.json
  docker stop viyadb-consul >/dev/null
}

trap stop_consul EXIT
start_consul

while true; do
  sleep 10
done

