#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

REGISTRY="199658938451.dkr.ecr.us-east-2.amazonaws.com"
REPOSITORY="${REGISTRY}/olake-worker"
GIT_SHA="$(git rev-parse HEAD)"
GIT_TAG="git-${GIT_SHA:0:12}"

aws ecr get-login-password --region us-east-2 |
    docker login --username AWS --password-stdin "${REGISTRY}"

docker build --platform linux/amd64 --file Dockerfile \
    --tag "${REPOSITORY}:latest" \
    --tag "${REPOSITORY}:${GIT_TAG}" .

docker push "${REPOSITORY}:latest"
docker push "${REPOSITORY}:${GIT_TAG}"
