#!/bin/bash -eu

cd /viyadb/

echo
echo "======================="
echo "Checking C++ code style"
echo "======================="
echo
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
./test/unit_tests
lcov --capture --directory . --output-file coverage.info
lcov --remove coverage.info '/usr/*' '/tmp/*' '*/third_party/*' '*/test/*' --output-file coverage.info
bash <(curl -s https://codecov.io/bash) -f coverage.info
