# One image with every binary (ledger-api, relay, notifier, reconcile, ledgerctl); the command picks which runs.
# Static binaries on distroless: no shell, no package manager, a non-root user.
FROM golang:1.27.1 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations
ARG COMMIT=dev
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X github.com/tochinicky/go-payments-ledger/internal/version.Commit=${COMMIT}" \
      -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER 65532:65532   # distroless "nonroot", numeric so Kubernetes can verify runAsNonRoot
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/ledger-api"]
