#!/bin/bash -eu

cd /viyadb/

echo "Checking C++ code style"
./scripts/check-formatting.sh

echo "Building ViyaDB"
[ -d build ] || mkdir build
cd build
cmake ..
make -j2

echo "Running unit tests"
./test/unit_tests

