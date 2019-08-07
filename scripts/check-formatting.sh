#!/bin/bash -e

echo
echo "======================="
echo "Checking C++ code style"
echo "======================="
echo

if [ ! -f CODE_OF_CONDUCT.md ]; then
  echo "This script is supposed to run from the root directory!"
  exit 1
fi

if ! which clang-format >/dev/null 2>&1; then
  echo "Please install clang-format version 3.8 or greater!"
  exit 1
fi

cmd="clang-format -style=LLVM"

source_dirs="src/ test/"
if [ $# -gt 0 ]; then
  source_dirs="$@"
fi

for file in $(find $source_dirs -name '*.cc' -o -name '*.h'); do
  if $cmd -output-replacements-xml $file | grep -c "<replacement " >/dev/null; then
    echo "Please run clang-format on $file:"
    $cmd $file | diff - $file
    format_error=true
  fi
done

if [ ! -z "${format_error}" ]; then
  echo "Some files require formatting"
  exit 1
fi

exit 0
