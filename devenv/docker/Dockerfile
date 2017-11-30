from ubuntu:zesty

MAINTAINER Michael Spector <spektom@gmail.com>
LABEL Description="Build environment for ViyaDB"

RUN apt-get update && apt-get install -y --no-install-recommends \
    autoconf \
    automake \
    binutils-dev \
    bison \
    build-essential \
    git \
    flex \
    libfl-dev \
    libbz2-dev \
    libevent-dev \
    liblz4-dev \
    libssl-dev \
    libtool \
    pkg-config \
    python \
    software-properties-common \
    valgrind \
    wget \
    zlib1g-dev \
  && add-apt-repository ppa:ubuntu-toolchain-r/test \
  && apt-get update && apt-get install -y \
    g++-7 \
  && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-7 60 --slave /usr/bin/g++ g++ /usr/bin/g++-7 \
  && wget -q https://dl.bintray.com/boostorg/release/1.65.1/source/boost_1_65_1.tar.gz \
    && tar xfz boost_1_65_1.tar.gz \
    && rm boost_1_65_1.tar.gz \
    && cd boost_1_65_1 \
    && ./bootstrap.sh \
    && ./b2 --without-python -j 4 link=static runtime-link=shared install \
    && cd .. \
    && rm -rf boost_1_65_1 \
  && wget -q https://cmake.org/files/v3.10/cmake-3.10.0-Linux-x86_64.sh -O cmake-install.sh \
    && sh ./cmake-install.sh --prefix=/usr/local --skip-license \
  && ldconfig \
  && apt-get autoremove -y \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /var/tmp/* \
    /tmp/*

RUN mkdir /viyadb
VOLUME /viyadb

CMD bash

