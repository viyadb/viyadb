#!/bin/bash -e

# Configure and build
rm -rf debug
mkdir debug
cd debug
cmake -DCMAKE_BUILD_TYPE=Debug -DCODE_COVERAGE=ON ..
make -j2

# Collect total file and line number information to base coverage on:
lcov --capture --initial --directory . --output-file baseline.info

# Run unit tests
./test/unit_tests

# Capture tests coverage:
lcov --capture --directory . --output-file test.info

# Combine baseline and tests coverage:
lcov --add-tracefile baseline.info --add-tracefile test.info --output-file total.info

# Remove unneeded sources from coverage:
lcov --remove total.info '/usr/*' '/tmp/*' '*/third_party/*' '*/test/*' --output-file total.info
