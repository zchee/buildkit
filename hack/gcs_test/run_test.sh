#!/bin/sh -ex

cd "$(dirname "$0")"

docker buildx bake --load
docker run --rm --privileged -p 4443:4443 moby/buildkit:gcstest /test/test.sh
docker rmi moby/buildkit:gcstest
