#!/bin/bash

set -ex
test -z $1 && exit 1

docker images --filter "dangling=true" -q | xargs --no-run-if-empty docker rmi -f

docker build \
  --build-arg tarball=$1 \
  -t viyadb/viyadb:latest \
  -t viyadb/viyadb:$(cat ../VERSION) \
  .

