#!/bin/bash -eu

echo
echo "======================================"
echo "Collecting and reporting coverage info"
echo "======================================"
echo

lcov --capture --directory . --output-file test.info
lcov --add-tracefile baseline.info --add-tracefile test.info --output-file total.info
lcov --remove total.info '/usr/*' '/tmp/*' '*/third_party/*' '*/test/*' --output-file total.info

bash <(curl -s https://codecov.io/bash) -f total.info
