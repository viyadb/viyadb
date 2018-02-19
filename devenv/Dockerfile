from ubuntu:artful

MAINTAINER Michael Spector <spektom@gmail.com>
LABEL Description="Build environment for ViyaDB"

ENV TERM=xterm

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
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
    ca-certificates \
    zlib1g-dev \
    clang-format \
    g++-7 \
  && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-7 60 --slave /usr/bin/g++ g++ /usr/bin/g++-7 \
  && curl -sS -L https://cmake.org/files/v3.10/cmake-3.10.2-Linux-x86_64.sh -o cmake-install.sh \
    && sh ./cmake-install.sh --prefix=/usr/local --skip-license \
    && rm -f ./cmake-install.sh \
  && curl -sS -L https://dl.bintray.com/boostorg/release/1.65.1/source/boost_1_65_1.tar.gz | tar -zxf - \
    && cd boost_1_65_1 \
    && ./bootstrap.sh \
    && ./b2 --without-python -j 4 link=static runtime-link=shared install \
    && cd .. \
    && rm -rf boost_1_65_1 \
  && ldconfig \
  && apt-get purge -y \
    manpages-dev \
  && apt-get autoremove -y \
  && apt-get clean \
  && rm -rf \
    /usr/local/doc \
    /usr/local/man \
    /usr/local/share/man \
    /usr/local/bin/cmake-gui \
    /usr/local/bin/cpack \
    /usr/local/bin/ctest \
    /usr/local/bin/ccmake \
    /usr/share/doc* \
    /usr/share/man \
    /usr/share/info \
    /var/lib/apt/lists/* \
    /var/tmp/* \
    /tmp/*

RUN mkdir /viyadb
VOLUME /viyadb

CMD bash
