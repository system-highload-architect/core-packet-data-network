# Корень проекта
ROOT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Go commands
GOCMD := go
GOBUILD := $(GOCMD) build
GORUN := $(GOCMD) run
GOTEST := $(GOCMD) test
BENCHFLAGS := -bench=. -benchmem -timeout 30s

# Build output
BIN_DIR := bin

# TLS
CERT_SCRIPT := scripts/generate-certs.sh

.PHONY: all build clean certs run-udp run-udp-fec run-quic run-sctp bench test

all: build

build: certs
	@echo "Building all clients and servers..."
	@mkdir -p $(BIN_DIR)
	$(GOBUILD) -o $(BIN_DIR)/udp-client ./cmd/udp-client
	$(GOBUILD) -o $(BIN_DIR)/udp-server ./cmd/udp-server
	$(GOBUILD) -o $(BIN_DIR)/udp-fec-client ./cmd/udp-fec-client
	$(GOBUILD) -o $(BIN_DIR)/udp-fec-server ./cmd/udp-fec-server
	$(GOBUILD) -o $(BIN_DIR)/quic-client ./cmd/quic-client
	$(GOBUILD) -o $(BIN_DIR)/quic-server ./cmd/quic-server
	$(GOBUILD) -o $(BIN_DIR)/sctp-client ./cmd/sctp-client
	$(GOBUILD) -o $(BIN_DIR)/sctp-server ./cmd/sctp-server
	@echo "Build complete."

certs:
	bash $(CERT_SCRIPT)

# Удобные цели для запуска пар (сервер + клиент) – в разных терминалах или с фоном
run-udp:
	@echo "Starting UDP server..."
	$(GORUN) ./cmd/udp-server &
	sleep 0.5
	$(GORUN) ./cmd/udp-client
	@echo "Stopping UDP server..."
	killall udp-server 2>/dev/null || true

run-udp-fec:
	@echo "Starting UDP+FEC server..."
	$(GORUN) ./cmd/udp-fec-server &
	sleep 0.5
	$(GORUN) ./cmd/udp-fec-client
	@echo "Stopping UDP+FEC server..."
	killall udp-fec-server 2>/dev/null || true

run-quic:
	@echo "Starting QUIC server..."
	$(GORUN) ./cmd/quic-server &
	sleep 1
	$(GORUN) ./cmd/quic-client
	@echo "Stopping QUIC server..."
	killall quic-server 2>/dev/null || true

run-sctp:
	@echo "Starting SCTP server..."
	$(GORUN) ./cmd/sctp-server &
	sleep 0.5
	$(GORUN) ./cmd/sctp-client
	@echo "Stopping SCTP server..."
	killall sctp-server 2>/dev/null || true

# Запуск бенчмарков (требуется, чтобы сервер не был занят)
bench:
	@echo "Running benchmarks for all variants..."
	$(GOTEST) $(BENCHFLAGS) ./internal/variants/...

test:
	$(GOTEST) ./...

clean:
	rm -rf $(BIN_DIR)
	rm -rf certs