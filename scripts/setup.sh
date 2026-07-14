#!/usr/bin/env bash
# ==============================================================================
# Highload Core Packet Network Engine - Automated Setup & Build Script
# ==============================================================================
set -euo pipefail

echo "==> 1. Cleaning up legacy artifacts..."
rm -f go.work go.work.sum
rm -rf bin

echo "==> 2. Initializing Go Workspace (Multi-Module Monorepo)..."
go work init

echo "==> 3. Linking pkg components down to Workspace context..."
go work use ./pkg/config
go work use ./pkg/fec
go work use ./pkg/lru
go work use ./pkg/network
go work use ./pkg/order
go work use ./pkg/packet
go work use ./pkg/pregen
go work use ./pkg/shutdown
go work use ./pkg/workerpool
go work use ./pkg/zeroalloc

echo "==> 4. Linking internal variants and common tools..."
go work use ./internal/common
go work use ./internal/variants

echo "==> 5. Linking console application entrypoints (cmd)..."
go work use ./cmd/udp-server
go work use ./cmd/udp-client
go work use ./cmd/quic-server
go work use ./cmd/quic-client
go work use ./cmd/udp-fec-server
go work use ./cmd/udp-fec-client
go work use ./cmd/sctp-server
go work use ./cmd/sctp-client

echo "==> 6. Generating TLS Certificates for QUIC execution layer..."
mkdir -p certs
if [[ "\((uname -s)" == MINGW* ]] \vert{}\vert{} [[ "\)(uname -s)" == MSYS* ]]; then
    export MSYS_NO_PATHCONV=1
fi

openssl req -x509 -newkey rsa:2048 -keyout certs/key.pem -out certs/cert.pem -days 365 -nodes \
    -subj "/CN=localhost" \
    -addext "subjectAltName = DNS:localhost, IP:127.0.0.1" 2>/dev/null

echo "==> 7. Running all core infrastructure unit-tests (pkg/)..."
go test ./pkg/...

echo "==> 8. Compiling high-performance binaries..."
mkdir -p bin
go build -o bin/udp-server ./cmd/udp-server/main.go
go build -o bin/udp-client ./cmd/udp-client/main.go
go build -o bin/quic-server ./cmd/quic-server/main.go
go build -o bin/quic-client ./cmd/quic-client/main.go
go build -o bin/udp-fec-server ./cmd/udp-fec-server/main.go
go build -o bin/udp-fec-client ./cmd/udp-fec-client/main.go
go build -o bin/sctp-server ./cmd/sctp-server/main.go
go build -o bin/sctp-client ./cmd/sctp-client/main.go

echo "=============================================================================="
echo "🎯 СБОРКА ЗАВЕРШЕНА УСПЕШНО! Все бинарники находятся в папке ./bin/"
echo "=============================================================================="