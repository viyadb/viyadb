ViyaDB
=======

ViyaDB is in-memory columnar analytical data store, featuring:

 * Fast ad-hoc analytical queries
 * Random access update pattern
 * Built-in cardinality protection
 * Real-time query compilation to machine code
 * Dynamic period based rollup
 * REST API interface with intuitive JSON-based language

For more information please visit the Website: http://viyadb.com

## Building from source

[![Build Status](https://travis-ci.org/viyadb/viyadb.png)](https://travis-ci.org/viyadb/viyadb)

If your development machine is not Linux, unfortunately, refer to [this](devenv) document for instructions.

### Prerequisites

You must have the following prerequisites installed:

 * CMake >= 3.2
 * Boost >= 1.64.0
 * g++ >= 7.1

Additional third party dependencies are included into the project as Git submodules.

### Building

To fetch third party dependencies for the first time, run:

    git submodule update --init --recursive

To update third party dependencies when needed, run:

    git submodule update --recursive --remote

To build the project, run:

    mkdir build/
    cd build/
    cmake ..
    make -j8

### Testing

Unit tests are built as part of the main build process. To invoke all unit tests, run:

    GLOG_logtostderr=1 ./test/unit_tests

