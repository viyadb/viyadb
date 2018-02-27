#!/bin/bash

set -ex

for arg in "$@"; do
  case "$arg" in
    "-cache") cached="true"
      ;;
  esac
done

if [ $(uname -s) != "Linux" ]; then
  cd ../devenv/docker
  exec ./run.sh bash -c "cd /viyadb/package && ./tarball.sh"
fi

version=$(cat ../VERSION)
package_name=$(echo viyadb-${version}-$(uname -s)-x86_64 | tr "[:upper:]" "[:lower:]")

if [ "${cached}" != "true" ]; then
  rm -rf build
  trap "rm -rf build" EXIT
fi
[ ! -d build ] && mkdir build
cd build

cmake --build-target viyad ../.. \
  -DCMAKE_INSTALL_PREFIX=${package_name} \
  -DVIYA_IS_RELEASE=ON

make -j4 install

rm -rf \
  ${package_name}/lib/cmake \
  ${package_name}/lib/pkgconfig \
  ${package_name}/lib/libevent*.a \
  ${package_name}/lib/libcppkafka*.a \
  ${package_name}/lib/libglog*.a \
  ${package_name}/include/curl \
  ${package_name}/bin/curl-config

cp ../vsql ${package_name}/bin/

tar -zcf ${package_name}.tgz ${package_name}
mv ${package_name}.tgz ..
cd ..

