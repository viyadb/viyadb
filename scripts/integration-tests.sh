#!/bin/bash -eu

if [ ! -f CODE_OF_CONDUCT.md ]; then
  echo "This script is supposed to run from the root directory!"
  exit 1
fi

cleanup() {
  docker-compose -p viyadb-integration-test --project-directory . \
    -f scripts/cluster_test/docker-compose.yml down
}

trap cleanup EXIT

docker-compose -p viyadb-integration-test --project-directory . \
  -f scripts/cluster_test/docker-compose.yml up --force-recreate \
  --abort-on-container-exit --exit-code-from test
