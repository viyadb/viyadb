#!/bin/bash -eu

if [ ! -f CODE_OF_CONDUCT.md ]; then
  echo "This script is supposed to run from the root directory!"
  exit 1
fi

./scripts/check-formatting.sh

echo
echo "==========================="
echo "Building ViyaDB for release"
echo "==========================="
echo
rm -rf build
mkdir build
cd build
cmake ..
make -j2

echo
echo "============================"
echo "Running unit tests (release)"
echo "============================"
echo
rm -rf /tmp/viyadb
./test/unit_tests
cd ..

echo
echo "============================"
echo "Building ViyaDB for coverage"
echo "============================"
echo
rm -rf build
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Debug -DCODE_COVERAGE=ON ..
make -j2

echo
echo "===================================="
echo "Running unit tests (debug, coverage)"
echo "===================================="
echo
rm -rf /tmp/viyadb
lcov --capture --initial --directory . --output-file baseline.info
./test/unit_tests

