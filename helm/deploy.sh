#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

temp_dir=$(mktemp -d)
trap 'rm -rf "$temp_dir"' EXIT

aws ecr get-login-password --region us-east-2 |
  helm registry login --username AWS --password-stdin 199658938451.dkr.ecr.us-east-2.amazonaws.com

helm package olake --destination "$temp_dir"
helm push "$temp_dir"/olake-*.tgz oci://199658938451.dkr.ecr.us-east-2.amazonaws.com/helm
