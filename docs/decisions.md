# Decisions

Each design choice, why it was made, and what it costs.

## Slice 0: toolchain and skeleton, 2026-10-03

- **Go 1.27.1**, the latest stable release on 2026-10-03 (`go.mod`: `go 1.27.1`). Pinning the exact version makes local, Docker and CI builds identical.
- **The toolchain runs in Docker** (`scripts/go.sh`), so the only prerequisite is Docker and every machine uses the same Go version. The image is pinned to `golang:1.27.1`. Module and build caches live in named volumes (`gpl-gomod`, `gpl-gocache`), so repeat runs take seconds. The race detector needs cgo, and the `golang` image has gcc, so `-race` works in it.
- **Module path `github.com/tochinicky/go-payments-ledger`**: the repo the code will live at, so imports don't change when it's pushed.
- **Lint: golangci-lint v2.14.0**, also run from its Docker image. Beyond the standard set (errcheck, govet, staticcheck, unused, ineffassign) it enables linters that matter for this service: `errorlint` (wrap with `%w`, compare with `errors.Is/As`), `gosec`, `bodyclose`, `noctx`, `sqlclosecheck`, `rowserrcheck`, and `revive` for idiomatic naming and doc comments. `gofmt` and `goimports` are enforced as formatters. Checked that it really fails: an ignored `os.Stdout.Write` error was reported by errcheck.
- **CI** (`.github/workflows/ci.yml`, checked with actionlint): lint, `go test -race -count=1 ./...`, and build with the commit stamped in. `-count=1` turns off Go's test cache, so CI always really runs the tests. No deploy, no registry push.
- **Version stamping:** `internal/version` holds `Version` and `Commit`, set at build time with `-ldflags -X`. It's the Go way of stamping a build without generated files.
