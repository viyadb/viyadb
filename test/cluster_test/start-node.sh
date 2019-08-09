#!/bin/bash -eu

while ! curl -m1 -sSL http://setup:8080/ >/dev/null 2>&1; do
  echo "Configuration is not set ... will retry in 1s"
  sleep 1
done

rack_id=$1

cat >/tmp/store.json <<EOF
{
  "supervise": true,
  "workers": 2,
  "cluster_id": "cluster001",
  "consul_url": "http://consul:8500",
  "rack_id": "$rack_id",
  "state_dir": "/tmp/viyadb",
  "http_port": 5000
}
EOF

exec ./src/server/viyad /tmp/store.json
