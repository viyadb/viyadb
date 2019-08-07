#!/bin/bash -eu

if [ ! -f CODE_OF_CONDUCT.md ]; then
  echo "This script is supposed to run from the root directory!"
  exit 1
fi

docker run --rm -v $(pwd):/viyadb \
  -w /viyadb viyadb/devenv \
  /viyadb/scripts/build-and-test.sh

./scripts/integration-tests.sh

docker run --rm -e CODECOV_TOKEN=${CODECOV_TOKEN} \
  -w /viyadb/build -v $(pwd):/viyadb viyadb/devenv \
  /viyadb/scripts/collect-coverage.sh
