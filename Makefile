# CyRedis Makefile
# Comprehensive test runner and build automation

.PHONY: help test test-unit test-integration test-fast test-coverage test-examples
.PHONY: docker-up docker-down docker-clean test-all test-watch
.PHONY: clean build dist dist-check install dev-install lint lint-report typecheck format format-check
.PHONY: module module-clean module-fetch

# Everything runs through uv (see AGENTS.md); `uv run` syncs the `dev`
# dependency group from pyproject.toml so no manual install step is needed.
UV := uv
PYTHON := $(UV) run python
PYTEST := $(UV) run pytest
PYTEST_ARGS := -v

# Directories the formatters and linters cover; mirrors .github/workflows/tests.yml.
FORMAT_DIRS := cy_redis/ tests/ examples/ scripts/
LINT_DIRS := cy_redis/ tests/ scripts/

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

test-examples: ## Import every example against the installed package (fails on the first broken one)
	@echo "$(BLUE)Importing examples...$(NC)"
	$(PYTHON) scripts/check_examples.py

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

build: ## Build the Cython extensions in place (uses the dev group's Cython)
	@echo "$(BLUE)Building Cython extensions...$(NC)"
	$(PYTHON) setup.py build_ext --inplace
	@echo "$(GREEN)Build completed$(NC)"

dist: ## Build the sdist and wheel into dist/
	@echo "$(BLUE)Building distributions...$(NC)"
	rm -rf dist/
	$(UV) build
	@echo "$(GREEN)Distributions in dist/$(NC)"

dist-check: dist ## Build and validate what would be uploaded to PyPI
	@echo "$(BLUE)Checking distributions...$(NC)"
	$(UV) run twine check dist/*
	$(PYTHON) scripts/check_dist_contents.py dist/*.whl dist/*.tar.gz
	@echo "$(GREEN)Distributions look releasable$(NC)"

install: ## Editable install of the package into the uv environment
	@echo "$(BLUE)Installing package...$(NC)"
	$(UV) sync
	$(UV) pip install --no-build-isolation -e .
	@echo "$(GREEN)Installation completed$(NC)"

dev-install: install ## Alias for install: `uv sync` already brings in the dev group

##@ Code Quality

lint: ## Gating lint, identical to CI: flake8 error classes, black and isort checks
	@echo "$(BLUE)Running linters...$(NC)"
	$(UV) run flake8 $(LINT_DIRS) --count --select=E9,F63,F7,F82 --show-source --statistics
	$(UV) run black --check $(FORMAT_DIRS)
	$(UV) run isort --check-only $(FORMAT_DIRS)

lint-report: ## Advisory flake8 report (style/complexity); never fails
	@echo "$(BLUE)flake8 report (advisory)...$(NC)"
	$(UV) run flake8 $(LINT_DIRS) --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

typecheck: ## Run mypy on the Python surface (the compiled modules have no stubs yet)
	@echo "$(BLUE)Running mypy...$(NC)"
	$(UV) run mypy cy_redis/

format: ## Format code with black and isort
	@echo "$(BLUE)Formatting code...$(NC)"
	$(UV) run black $(FORMAT_DIRS)
	$(UV) run isort $(FORMAT_DIRS)
	@echo "$(GREEN)Formatting completed$(NC)"

format-check: ## Report formatting drift without changing files
	$(UV) run black --check --diff $(FORMAT_DIRS)
	$(UV) run isort --check-only --diff $(FORMAT_DIRS)

##@ Utilities

info: ## Show project information
	@echo "$(BLUE)Project Information$(NC)"
	@echo "  Project Dir:  $(PROJECT_DIR)"
	@echo "  Tests Dir:    $(TESTS_DIR)"
	@echo "  Examples Dir: $(EXAMPLES_DIR)"
	@echo "  Scripts Dir:  $(SCRIPTS_DIR)"
	@echo ""
	@echo "$(BLUE)Environment$(NC)"
	@echo "  Python:       $$($(PYTHON) --version 2>/dev/null || echo 'not installed')"
	@echo "  UV:           $$($(UV) --version 2>/dev/null || echo 'not installed')"
	@echo "  Pytest:       $$($(UV) run pytest --version 2>/dev/null || echo 'not installed')"
	@echo ""
	@if [ -f "docker-compose.yml" ]; then \
		echo "$(BLUE)Docker Services$(NC)"; \
		$(DOCKER_COMPOSE) ps; \
	fi

##@ Redis C Module (cy_game.so)

module-fetch: ## Fetch FLECS + redismodule.h vendor headers (requires internet)
	@echo "$(BLUE)Fetching vendor headers for cy_game module...$(NC)"
	$(MAKE) -C experimental/cyredis_experimental/game/module fetch-headers
	@echo "$(GREEN)Vendor headers fetched$(NC)"

module: ## Build cy_game.so Redis module
	@echo "$(BLUE)Building cy_game Redis module...$(NC)"
	$(MAKE) -C experimental/cyredis_experimental/game/module
	@echo "$(GREEN)cy_game.so built: experimental/cyredis_experimental/game/module/cy_game.so$(NC)"

module-clean: ## Clean cy_game.so build artifacts
	@echo "$(BLUE)Cleaning cy_game module build artifacts...$(NC)"
	$(MAKE) -C experimental/cyredis_experimental/game/module clean
	@echo "$(GREEN)Module clean completed$(NC)"

.DEFAULT_GOAL := help
