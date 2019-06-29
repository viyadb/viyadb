<img src="http://viyadb.com/img/logo.svg" height="100px" />

ViyaDB
=======

[![Build Status](https://travis-ci.org/viyadb/viyadb.svg?branch=master)](https://travis-ci.org/viyadb/viyadb)

ViyaDB is in-memory columnar analytical data store, featuring:

 * Fast ad-hoc analytical queries
 * Random access update pattern
 * Built-in cardinality protection
 * Real-time query compilation to machine code
 * Dynamic period based rollup
 * REST API interface with intuitive JSON-based language
 * Basic SQL (DML) support
 
## Quickstart

 * Learn about ViyaDB through [Mobile Attribution Tracking Sample](http://viyadb.com/samples/#mobile-attribution-tracking).
 * Read more on ViyaDB [Usage](http://viyadb.com/usage).
 * Deep dive into ViyaDB [Clustering Architecture](http://viyadb.com/clustering) and [Real-Time Analytics Infrastructure](http://viyadb.com/realtime) based on ViyaDB.
 
For more information please visit the official Website: http://viyadb.com

## Building

In order to pull all third party dependencies, either clone ViyaDB sources using `--recursive` flag, or run this command afterwards:

    git submodule update --init --recursive

The easiest way to build ViyaDB is using `viyadb/devenv` Docker image:

    docker run --rm -v $(pwd):/viyadb viyadb/devenv:latest /viyadb/scripts/travis-build.sh

If for some reason you'd like to use your own system tools, please read on.

### Prerequisites

The following components are required for building ViyaDB:

 * CMake >= 3.10
 * Boost >= 1.65.1
 * Flex >= 2.6.1
 * Bison >= 2:3.0.4
 * g++ >= 8.3

Additional third party dependencies are included into the project as [Git submodules](https://git-scm.com/book/en/v2/Git-Tools-Submodules).

### Building

To build the project, run:

    mkdir build/
    cd build/
    cmake ..
    make -j4

### Testing

Unit tests are built as part of the main build process. To invoke all unit tests, run:

    GLOG_logtostderr=1 ./test/unit_tests

