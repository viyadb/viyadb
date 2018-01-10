ViyaDB
=======

[![Build Status](https://travis-ci.org/viyadb/viyadb.png)](https://travis-ci.org/viyadb/viyadb)

ViyaDB is in-memory columnar analytical data store, featuring:

 * Fast ad-hoc analytical queries
 * Random access update pattern
 * Built-in cardinality protection
 * Real-time query compilation to machine code
 * Dynamic period based rollup
 * REST API interface with intuitive JSON-based language
 * Basic SQL (DML) support
 
## Quickstart

 * Learn about ViyaDB through [Mobile Attribution Tracking Sample](http://viyadb.com/samples/#mobile-attribution-tracking)
 * Read more on ViyaDB [Usage](http://viyadb.com/usage)
 
For more information please visit the official Website: http://viyadb.com

## Building

If your development machine is not Linux, please refer to [this](devenv) document for instructions.

### Prerequisites

You must have the following prerequisites installed:

 * CMake >= 3.9
 * Boost >= 1.65.1
 * Flex >= 2.6.1
 * Bison >= 2:3.0.4
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

