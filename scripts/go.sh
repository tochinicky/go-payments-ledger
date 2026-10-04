#!/usr/bin/env bash
# Runs the Go toolchain in Docker: Go isn't installed on the host. The repo is mounted at /src, and the module and
# build caches live in named volumes so repeated runs are fast.
#   scripts/go.sh go test -race ./...
#   scripts/go.sh golangci-lint run          (uses the golangci-lint image instead)
#   scripts/go.sh sqlc generate              (uses the sqlc image)
set -euo pipefail
cd "$(dirname "$0")/.."
GO_IMAGE=golang:1.27.1
LINT_IMAGE=golangci/golangci-lint:v2.14.0
image=$GO_IMAGE
SQLC_IMAGE=sqlc/sqlc:1.31.1
[[ "${1:-}" == golangci-lint ]] && image=$LINT_IMAGE
if [[ "${1:-}" == sqlc ]]; then image=$SQLC_IMAGE; shift; fi
# The Docker socket lets Testcontainers start Postgres and Redpanda next to this container; their mapped ports
# are reached through host.docker.internal.
exec docker run --rm -v "$PWD":/src -w /src \
  -v gpl-gomod:/go/pkg/mod -v gpl-gocache:/root/.cache \
  -v /var/run/docker.sock:/var/run/docker.sock -e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal \
  "$image" "$@"
