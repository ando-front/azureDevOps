# Azure DevOps Testing Environment
# This makefile provides common commands for building and testing

.PHONY: help build up down test test-unit test-specific test-e2e e2e-build e2e-up e2e-down e2e-clean clean logs status

# Default target
help:
	@echo "Available commands:"
	@echo "  build      - Build Docker images"
	@echo "  up         - Start all services"
	@echo "  down       - Stop all services"
	@echo "  test       - Run all tests"
	@echo "  test-unit  - Run unit tests only"
	@echo "  test-specific TEST=<name> - Run specific test"
	@echo "  test-e2e   - Run E2E tests (complete suite)"
	@echo "  e2e-build  - Build E2E test images"
	@echo "  e2e-up     - Start E2E test environment"
	@echo "  e2e-down   - Stop E2E test environment"
	@echo "  e2e-clean  - Clean E2E test environment"
	@echo "  clean      - Clean up containers and volumes"
	@echo "  logs       - Show service logs"
	@echo "  status     - Show service status"

# Build images
build:
	docker-compose build --no-cache

# Start services
up:
	docker-compose up -d
	@echo "Waiting for services to be ready..."
	@sleep 10
	@docker-compose exec pytest-test bash -c "curl -f http://azurite:10000/devstoreaccount1 || echo 'Azurite not ready yet'"

# Stop services
down:
	docker-compose down

# Run all tests
test: up
	docker-compose exec pytest-test bash -c "\
		source /opt/venv/bin/activate && \
		export PYTHONPATH=/tests && \
		export AZURITE_HOST=azurite && \
		cd /tests && \
		python -m pytest unit/ -v --tb=short"

# Run unit tests only
test-unit: up
	docker-compose exec pytest-test bash -c "\
		source /opt/venv/bin/activate && \
		export PYTHONPATH=/tests && \
		export AZURITE_HOST=azurite && \
		cd /tests && \
		python -m pytest unit/ -v"

# Run specific test
test-specific: up
	docker-compose exec pytest-test bash -c "\
		source /opt/venv/bin/activate && \
		export PYTHONPATH=/tests && \
		export AZURITE_HOST=azurite && \
		cd /tests && \
		python -m pytest unit/$(TEST) -v -s"

# Clean up
clean:
	docker-compose down -v
	docker system prune -f

# Show logs
logs:
	docker-compose logs -f

# Show status
status:
	docker-compose ps

# ======================================
# E2E Testing Commands
# ======================================

# Build E2E test images
e2e-build:
	@echo "Building E2E test images..."
	docker-compose -f docker-compose.e2e.yml build --no-cache

# Start E2E test environment
e2e-up:
	@echo "Starting E2E test environment..."
	docker-compose -f docker-compose.e2e.yml up -d sqlserver-test azurite-test ir-simulator
	@echo "Waiting for services to be ready..."
	@sleep 60

# Stop E2E test environment
e2e-down:
	@echo "Stopping E2E test environment..."
	docker-compose -f docker-compose.e2e.yml down

# Run complete E2E test suite
test-e2e: e2e-build
	@echo "Running complete E2E test suite..."
	./run-e2e-tests.sh

# Run E2E tests (quick mode)
test-e2e-quick:
	@echo "Running E2E tests in quick mode..."
	./run-e2e-tests.sh --test-only

# Clean E2E test environment
e2e-clean:
	@echo "Cleaning E2E test environment..."
	docker-compose -f docker-compose.e2e.yml down --volumes --remove-orphans
	docker system prune -f

# Show E2E test logs
e2e-logs:
	docker-compose -f docker-compose.e2e.yml logs -f

# Show E2E test status
e2e-status:
	docker-compose -f docker-compose.e2e.yml ps

# Validate E2E environment
e2e-validate:
	@echo "Validating E2E environment..."
	@docker-compose -f docker-compose.e2e.yml config --quiet && echo "✅ Docker Compose config valid"
	@test -f Dockerfile.e2e.complete-light && echo "✅ E2E Dockerfile exists"
	@test -f requirements.e2e.txt && echo "✅ E2E requirements file exists"
	@test -x run-e2e-tests.sh && echo "✅ E2E test script is executable"
	@test -d tests/e2e && echo "✅ E2E test directory exists"

# ======================================
# プロキシ設定選択可能 E2E Testing Commands
# ======================================

# プロキシなしE2Eテスト（推奨・高速）
e2e-no-proxy:
	@echo "🚀 プロキシなしE2Eテストを実行中..."
	./run-e2e-flexible.sh --no-proxy full

# プロキシありE2Eテスト（企業環境）
e2e-proxy:
	@echo "🚀 プロキシありE2Eテストを実行中..."
	./run-e2e-flexible.sh --proxy full

# 対話的プロキシ設定選択
e2e-interactive:
	@echo "🚀 対話的E2Eテストを実行中..."
	./run-e2e-flexible.sh --interactive full

# 一時的プロキシ無効化テスト
e2e-temp-no-proxy:
	@echo "🚀 一時的プロキシ無効化E2Eテストを実行中..."
	./run-e2e-no-proxy-temp.sh

# E2E環境状況確認
e2e-status:
	@echo "📊 E2E環境状況確認中..."
	./run-e2e-flexible.sh status

# E2E環境クリーンアップ
e2e-cleanup:
	@echo "🧹 E2E環境クリーンアップ中..."
	./run-e2e-flexible.sh cleanup

# 軽量E2Eテスト（データベース + Azuriteのみ）
e2e-lightweight:
	@echo "🚀 軽量E2Eテストを実行中..."
	docker-compose -f docker-compose.e2e.no-proxy.yml up --build --abort-on-container-exit

# ======================================
# E2E Testing Help
# ======================================

e2e-help:
	@echo ""
	@echo "=== E2E テスト実行コマンド ==="
	@echo ""
	@echo "基本コマンド:"
	@echo "  make e2e-no-proxy      プロキシなしで実行（推奨・高速）"
	@echo "  make e2e-proxy         プロキシありで実行（企業環境）"
	@echo "  make e2e-interactive   対話的に設定選択"
	@echo "  make e2e-temp-no-proxy 一時的プロキシ無効化"
	@echo ""
	@echo "管理コマンド:"
	@echo "  make e2e-status        現在の状況確認"
	@echo "  make e2e-cleanup       環境クリーンアップ"
	@echo "  make e2e-lightweight   軽量版テスト"
	@echo ""
	@echo "詳細オプション:"
	@echo "  ./run-e2e-flexible.sh --help    フル機能ヘルプ"
	@echo ""
