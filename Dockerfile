# --- Stage 1: Build the Go binary -------------------------------------------
FROM golang:1.25-alpine AS builder

WORKDIR /build

# Copy dependency manifests first so that `go mod download` is cached
# independently of source code changes.
COPY go.mod go.sum ./
RUN go mod download

# Copy the full source tree and compile a statically-linked binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /server ./cmd/server/main.go

# --- Stage 2: Minimal runtime image ----------------------------------------
FROM alpine:3.19

# Create a non-root user to run the binary.
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

COPY --from=builder /server /server

# The migrations directory is included so that it can be volume-mounted or
# copied into init containers if needed.
COPY --from=builder /build/migrations /migrations

EXPOSE 8080

USER appuser

ENTRYPOINT ["/server"]
