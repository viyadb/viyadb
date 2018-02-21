#!/bin/bash

set -ex
test -z $1 && exit 1

docker build \
  --squash \
  --build-arg tarball=$1 \
  -t viyadb/viyadb:latest \
  -t viyadb/viyadb:$(cat ../VERSION) \
  .

