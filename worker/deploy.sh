#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

aws ecr get-login-password --region us-east-2 |
  docker login --username AWS --password-stdin 199658938451.dkr.ecr.us-east-2.amazonaws.com

docker build --platform linux/amd64 --file Dockerfile \
  --label "org.opencontainers.image.revision=$(git rev-parse HEAD)" \
  --tag 199658938451.dkr.ecr.us-east-2.amazonaws.com/olakego/ui-worker:latest .

docker push 199658938451.dkr.ecr.us-east-2.amazonaws.com/olakego/ui-worker:latest
