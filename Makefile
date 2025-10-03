# CyRedis Makefile
# Comprehensive test runner and build automation

.PHONY: help test test-unit test-integration test-fast test-coverage test-apps
.PHONY: docker-up docker-down docker-clean test-all test-watch
.PHONY: clean build install dev-install lint format

# Python/UV configuration
PYTHON := python3
UV := uv
PYTEST := $(UV) run pytest
PYTEST_ARGS := -v

# Project directories
PROJECT_DIR := $(shell pwd)
TESTS_DIR := $(PROJECT_DIR)/tests
EXAMPLES_DIR := $(PROJECT_DIR)/examples
SCRIPTS_DIR := $(PROJECT_DIR)/scripts

# Coverage settings
COVERAGE_DIR := $(PROJECT_DIR)/htmlcov
COVERAGE_REPORT := $(PROJECT_DIR)/coverage.xml

# Docker compose command
DOCKER_COMPOSE := docker compose

# Color output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

##@ Help

help: ## Display this help message
	@echo "$(BLUE)CyRedis Test Runner$(NC)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make $(YELLOW)<target>$(NC)\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2 } /^##@/ { printf "\n$(BLUE)%s$(NC)\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Testing

test: ## Run all tests
	@echo "$(BLUE)Running all tests...$(NC)"
	$(PYTEST) $(PYTEST_ARGS) $(TESTS_DIR)

test-unit: ## Run unit tests only
	@echo "$(BLUE)Running unit tests...$(NC)"
	$(PYTEST) $(PYTEST_ARGS) -m "not integration and not slow" $(TESTS_DIR)

test-integration: ## Run integration tests only
	@echo "$(BLUE)Running integration tests...$(NC)"
	$(PYTEST) $(PYTEST_ARGS) -m "integration" $(TESTS_DIR)

test-fast: ## Run fast tests only (no slow/cluster)
	@echo "$(BLUE)Running fast tests...$(NC)"
	$(PYTEST) $(PYTEST_ARGS) -m "not slow and not cluster" $(TESTS_DIR)

test-coverage: ## Run tests with coverage report
	@echo "$(BLUE)Running tests with coverage...$(NC)"
	$(PYTEST) $(PYTEST_ARGS) --cov=cy_redis --cov-report=html --cov-report=term-missing --cov-report=xml $(TESTS_DIR)
	@echo "$(GREEN)Coverage report generated:$(NC)"
	@echo "  HTML: $(COVERAGE_DIR)/index.html"
	@echo "  XML:  $(COVERAGE_REPORT)"

test-apps: ## Run test applications/examples
	@echo "$(BLUE)Running test applications...$(NC)"
	@if [ -d "$(EXAMPLES_DIR)" ]; then \
		for example in $(EXAMPLES_DIR)/example_*.py; do \
			if [ -f "$$example" ]; then \
				echo "$(YELLOW)Running $$example...$(NC)"; \
				$(UV) run python "$$example" || true; \
			fi \
		done \
	else \
		echo "$(YELLOW)No examples directory found$(NC)"; \
	fi

test-watch: ## Run tests in watch mode (requires pytest-watch)
	@echo "$(BLUE)Running tests in watch mode...$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to stop$(NC)"
	$(UV) run ptw -- $(PYTEST_ARGS) $(TESTS_DIR)

##@ Docker

docker-up: ## Start Docker services (Redis, PostgreSQL, etc.)
	@echo "$(BLUE)Starting Docker services...$(NC)"
	@if [ -f "docker-compose.yml" ]; then \
		$(DOCKER_COMPOSE) up -d; \
		echo "$(GREEN)Docker services started$(NC)"; \
		$(DOCKER_COMPOSE) ps; \
	else \
		echo "$(YELLOW)No docker-compose.yml found$(NC)"; \
	fi

docker-down: ## Stop Docker services
	@echo "$(BLUE)Stopping Docker services...$(NC)"
	@if [ -f "docker-compose.yml" ]; then \
		$(DOCKER_COMPOSE) down; \
		echo "$(GREEN)Docker services stopped$(NC)"; \
	else \
		echo "$(YELLOW)No docker-compose.yml found$(NC)"; \
	fi

docker-clean: ## Clean Docker volumes and networks
	@echo "$(BLUE)Cleaning Docker volumes...$(NC)"
	@if [ -f "docker-compose.yml" ]; then \
		$(DOCKER_COMPOSE) down -v; \
		echo "$(GREEN)Docker volumes cleaned$(NC)"; \
	else \
		echo "$(YELLOW)No docker-compose.yml found$(NC)"; \
	fi

docker-logs: ## Show Docker service logs
	@if [ -f "docker-compose.yml" ]; then \
		$(DOCKER_COMPOSE) logs -f; \
	else \
		echo "$(YELLOW)No docker-compose.yml found$(NC)"; \
	fi

##@ Comprehensive Testing

test-all: docker-up test-coverage docker-down ## Full test suite: Docker + all tests + coverage
	@echo "$(GREEN)Full test suite completed!$(NC)"

##@ Build and Install

clean: ## Clean build artifacts
	@echo "$(BLUE)Cleaning build artifacts...$(NC)"
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf cy_redis.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.so" -delete
	find . -type f -name "*.c" ! -path "*/hiredis/*" ! -path "*/vendor/*" -delete
	rm -rf $(COVERAGE_DIR)
	rm -f $(COVERAGE_REPORT)
	rm -f .coverage
	@echo "$(GREEN)Clean completed$(NC)"

build: clean ## Build Cython extensions
	@echo "$(BLUE)Building Cython extensions...$(NC)"
	$(UV) run python setup.py build_ext --inplace
	@echo "$(GREEN)Build completed$(NC)"

install: build ## Install the package
	@echo "$(BLUE)Installing package...$(NC)"
	$(UV) pip install -e .
	@echo "$(GREEN)Installation completed$(NC)"

dev-install: ## Install development dependencies
	@echo "$(BLUE)Installing development dependencies...$(NC)"
	$(UV) pip install -e ".[dev,test]"
	@echo "$(GREEN)Development dependencies installed$(NC)"

##@ Code Quality

lint: ## Run linters (flake8, mypy)
	@echo "$(BLUE)Running linters...$(NC)"
	$(UV) run flake8 cy_redis/ tests/ || true
	$(UV) run mypy cy_redis/ || true

format: ## Format code with black and isort
	@echo "$(BLUE)Formatting code...$(NC)"
	$(UV) run black cy_redis/ tests/ examples/
	$(UV) run isort cy_redis/ tests/ examples/
	@echo "$(GREEN)Formatting completed$(NC)"

##@ Utilities

info: ## Show project information
	@echo "$(BLUE)Project Information$(NC)"
	@echo "  Project Dir:  $(PROJECT_DIR)"
	@echo "  Tests Dir:    $(TESTS_DIR)"
	@echo "  Examples Dir: $(EXAMPLES_DIR)"
	@echo "  Scripts Dir:  $(SCRIPTS_DIR)"
	@echo ""
	@echo "$(BLUE)Environment$(NC)"
	@echo "  Python:       $$($(PYTHON) --version)"
	@echo "  UV:           $$($(UV) --version 2>/dev/null || echo 'not installed')"
	@echo "  Pytest:       $$($(UV) run pytest --version 2>/dev/null || echo 'not installed')"
	@echo ""
	@if [ -f "docker-compose.yml" ]; then \
		echo "$(BLUE)Docker Services$(NC)"; \
		$(DOCKER_COMPOSE) ps; \
	fi

.DEFAULT_GOAL := help
