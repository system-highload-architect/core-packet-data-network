.PHONY: build test bench certs clean run-udp-server run-udp-client

# Переменные путей
BINARY_DIR=bin
CERT_DIR=certs

# ==============================================================================
# 1. СБОРКА И ТЕСТИРОВАНИЕ (BUILD & TEST)
# ==============================================================================

# Сборка всех бинарников проекта «под ключ»
build: clean
	@echo "Compiling all project variants..."
	@mkdir -p $(BINARY_DIR)
	go build -o $(BINARY_DIR)/udp-server ./cmd/udp-server/main.go
	go build -o $(BINARY_DIR)/udp-client ./cmd/udp-client/main.go
	go build -o $(BINARY_DIR)/quic-server ./cmd/quic-server/main.go
	go build -o $(BINARY_DIR)/quic-client ./cmd/quic-client/main.go
	go build -o $(BINARY_DIR)/sctp-server ./cmd/sctp-server/main.go
	go build -o $(BINARY_DIR)/sctp-client ./cmd/sctp-client/main.go

# Запуск всех юнит-тестов ядра инфраструктуры пакетов (pkg/) и вариантов
test:
	@echo "Running all core infrastructure tests..."
	go test -v ./pkg/config/...
	go test -v ./pkg/fec/...
	go test -v ./pkg/lru/...
	go test -v ./pkg/network/...
	go test -v ./pkg/order/...
	go test -v ./pkg/packet/...
	go test -v ./pkg/pregen/...
	go test -v ./pkg/shutdown/...
	go test -v ./pkg/workerpool/...
	go test -v ./pkg/zeroalloc/...
	go test -v ./internal/common/...
	go test -v ./internal/variants/...

# ==============================================================================
# 2. НАГРУЗОЧНОЕ ПРОФИЛИРОВАНИЕ (BENCHMARKS)
# ==============================================================================

# Запуск всех нагрузочных тестов производительности (RPS) с лимитом времени 30с
# Примечание: SCTP бенчмарк на Windows автоматически уйдет в SKIP
bench: certs
	@echo "Launching high-performance benchmarks (30s timeout)..."
	@echo "==> Running UDP Throughput Benchmark..."
	go test -bench=BenchmarkUDPThroughput -benchmem -timeout 30s ./internal/variants/udp
	@echo "==> Running QUIC Throughput Benchmark..."
	go test -bench=BenchmarkQUICThroughput -benchmem -timeout 30s ./internal/variants/quic
	@echo "==> Running UDP+FEC Throughput Benchmark..."
	go test -bench=BenchmarkUDPFECThroughput -benchmem -timeout 30s ./internal/variants/udp-fec
	@echo "==> Running SCTP Throughput Benchmark..."
	go test -bench=BenchmarkSCTPThroughput -benchmem -timeout 30s ./internal/variants/sctp

# Вспомогательная цель для генерации TLS сертификатов QUIC
certs:
	@chmod +x scripts/generate-certs.sh 2>/dev/null || true
	@./scripts/generate-certs.sh

# Полная очистка скомпилированных артефактов
clean:
	@echo "Cleaning up build artifacts..."
	@rm -rf $(BINARY_DIR)

# ==============================================================================
# 3. ШОРТКАТЫ КОНСОЛЬНОГО ЗАПУСКА (CMD RUNNERS)
# ==============================================================================

# Быстрый запуск базового UDP сервера строго на порту :6000
run-udp-server:
	@echo "Starting basic UDP Server on 127.0.0.1:6000..."
	go run ./cmd/udp-server/main.go --addr=127.0.0.1:6000

# Быстрый запуск базового UDP клиента на отправку 10 000 пакетов в 10 потоков
run-udp-client:
	@echo "Launching basic UDP Client to transmit 10,000 packets..."
	go run ./cmd/udp-client/main.go --server=127.0.0.1:6000 --packets=10000
