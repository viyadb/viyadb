#!/bin/bash -e

# Configure and build
rm -rf release
mkdir release
cd release
cmake ..
make -j2

# Run unit tests
./test/unit_tests
