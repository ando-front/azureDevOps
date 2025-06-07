# Azure DevOps Testing Environment
# This makefile provides common commands for building and testing

.PHONY: help build up down test test-unit test-specific clean logs status

# Default target
help:
	@echo "Available commands:"
	@echo "  build      - Build Docker images"
	@echo "  up         - Start all services"
	@echo "  down       - Stop all services"
	@echo "  test       - Run all tests"
	@echo "  test-unit  - Run unit tests only"
	@echo "  test-specific TEST=<name> - Run specific test"
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
