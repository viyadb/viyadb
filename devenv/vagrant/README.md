
Development environment vased on Vagrant.

## Prerequisites

 * [VirtualBox](https://www.virtualbox.org)
 * [Vagrant](https://www.vagrantup.com)

## Preparing development environment

    vagrant up

After running this command, a virtual machine containing everything needed for compiling and running ViyaDB
will be created.

## Entering VM shell

    vagrant ssh

## Building ViyaDB

    cd /viyadb
    mkdir build
    cd build
    cmake ..
    make -j8

