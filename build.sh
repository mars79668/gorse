#!/bin/sh

build() {
    MODULENAME=$1
    docker build -f cmd/gorse-$MODULENAME/Dockerfile -t gorse-$MODULENAME:latest .
}

dpush() {
    MODULENAME=$1

    awkCmd="{if (\$1 == \"gorse-${MODULENAME}\") print \$3}"
    newImageID=$(docker images | awk "${awkCmd}" | head -1)

    docker tag $newImageID registry.cn-beijing.aliyuncs.com/nbaweb/gorse-$MODULENAME:latest
    docker tag $newImageID registry.cn-beijing.aliyuncs.com/nbaweb/gorse-$MODULENAME:v0.4.13st
    docker push registry.cn-beijing.aliyuncs.com/nbaweb/gorse-$MODULENAME:latest
    docker push registry.cn-beijing.aliyuncs.com/nbaweb/gorse-$MODULENAME:v0.4.13st

    docker images | grep gorse-$MODULENAME

    rmi=$(docker images | grep "gorse-" | grep "<none>" | awk "{print \$3}")
    docker rmi $rmi
}


case "$1" in
build)
    build $2
;;
pbuild)
    build $2
    dpush $2
;;
dpush)
    dpush $2
;;
*)
    echo "./build.sh build|docker|dtest|push"
    exit 2
    ;;
esac