#!/bin/bash

set -ex

if [ $(uname -s) != "Linux" ]; then
  cd ../devenv/docker
  exec ./run.sh bash -c "cd /viyadb/package && ./tarball.sh"
fi

version=$(cat ../VERSION)
package_name=$(echo viyadb-${version}-$(uname -s)-x86_64 | tr "[:upper:]" "[:lower:]")

rm -rf build
mkdir build
cd build

cmake --build-target viyad ../.. \
  -DCMAKE_INSTALL_PREFIX=${package_name} \
  -DVIYA_IS_RELEASE=ON

make -j8 install

rm -rf \
  ${package_name}/lib/cmake \
  ${package_name}/lib/pkgconfig \
  ${package_name}/include/curl \
  ${package_name}/bin/curl-config

tar -zcf ${package_name}.tgz ${package_name}
mv ${package_name}.tgz ..
cd ..
rm -rf build

