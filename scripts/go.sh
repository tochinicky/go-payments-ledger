#!/usr/bin/env bash
# Runs the Go toolchain in Docker: Go isn't installed on the host. The repo is mounted at /src, and the module and
# build caches live in named volumes so repeated runs are fast.
#   scripts/go.sh go test -race ./...
#   scripts/go.sh golangci-lint run          (uses the golangci-lint image instead)
set -euo pipefail
cd "$(dirname "$0")/.."
GO_IMAGE=golang:1.27.1
LINT_IMAGE=golangci/golangci-lint:v2.14.0
image=$GO_IMAGE
[[ "${1:-}" == golangci-lint ]] && image=$LINT_IMAGE
exec docker run --rm -v "$PWD":/src -w /src \
  -v gpl-gomod:/go/pkg/mod -v gpl-gocache:/root/.cache \
  "$image" "$@"
