
Development environment vased on Vagrant.

## Prerequisites

 * [Docker](https://www.docker.com)

## Preparing development environment

    ./prepare.sh

After running this command, docker image containing everything needed for compiling and running ViyaDB
will be created.

## Entering Docker container shell

    ./run.sh

## Building viya

    cd /viyadb
    mkdir build
    cd build
    cmake ..
    make -j8

