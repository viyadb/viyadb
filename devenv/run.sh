#!/bin/bash
exec docker run --rm --security-opt seccomp=unconfined -v $(pwd)/../../:/viyadb -p 5000:5000 -ti viyadb/devenv:latest "$@"
