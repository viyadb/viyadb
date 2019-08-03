
Development environment vased on Docker.

## Prerequisites

 * [Docker](https://www.docker.com)

## Preparing development environment

You can either build your own Docker image, or pull a ready one from Docker Hub repository.

To pull the latest Docker image containing the build environment, run:

    docker pull viyadb/devenv:latest

After running this command, docker image containing everything needed for compiling and running ViyaDB
will be created.

## Entering Docker container shell

    ./run.sh

## Building ViyaDB

    cd /viyadb
    mkdir build
    cd build
    cmake ..
    make -j8

