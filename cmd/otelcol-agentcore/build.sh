#!/bin/bash
# Build otelcol-agentcore via Docker. Usage: ./build.sh
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
IMAGE="otelcol-agentcore-builder"
CONTAINER="otelcol-agentcore-build-tmp"

rm -rf "$SCRIPT_DIR/output"

docker build --build-arg GOOS="${GOOS:-linux}" --build-arg GOARCH="${GOARCH:-arm64}" \
  -f "$SCRIPT_DIR/Dockerfile.build" -t $IMAGE "$REPO_ROOT"

docker rm -f $CONTAINER 2>/dev/null || true
docker run --name $CONTAINER -d $IMAGE
mkdir -p "$SCRIPT_DIR/output"
docker cp $CONTAINER:/output/otelcol-agentcore "$SCRIPT_DIR/output/otelcol-agentcore"
docker rm -f $CONTAINER

ls -lh "$SCRIPT_DIR/output/otelcol-agentcore"
