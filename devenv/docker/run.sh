#!/bin/bash
docker ps -a | grep viya-devenv | awk '{print $1}' | xargs docker rm
docker run --security-opt seccomp=unconfined -v $(pwd)/../../:/viyadb -p 5000:5000 -ti viya-devenv "$@"
