# =============================================================================
# PYMES Data Strategy - Development Commands
# =============================================================================

.PHONY: help up down logs clean restart ps db-migrate db-generate db-studio db-reset api-logs worker-logs redis-cli psql api-dev worker-dev install lint format typecheck test test-api test-worker test-coverage frontend-logs frontend-dev ci-watch

# Default target
help:
	@echo "PYMES Data Strategy - Available Commands"
	@echo ""
	@echo "  Infrastructure:"
	@echo "    make up          - Start all services (Docker)"
	@echo "    make down        - Stop all services"
	@echo "    make restart     - Restart all services"
	@echo "    make ps          - Show running services"
	@echo "    make logs        - Follow all logs"
	@echo "    make clean       - Stop and remove volumes (DATA LOSS!)"
	@echo ""
	@echo "  Development (local):"
	@echo "    make api-dev     - Start API Gateway (local dev mode)"
	@echo "    make worker-dev  - Start Worker ETL (local dev mode)"
	@echo ""
	@echo "  Testing:"
	@echo "    make test        - Run all tests (API + Worker)"
	@echo "    make test-api    - Run API tests (Vitest)"
	@echo "    make test-worker - Run Worker tests (pytest)"
	@echo "    make test-coverage - Run all tests with coverage"
	@echo ""
	@echo "  Database (Prisma):"
	@echo "    make db-migrate  - Run pending migrations"
	@echo "    make db-generate - Generate Prisma client"
	@echo "    make db-studio   - Open Prisma Studio"
	@echo "    make db-reset    - Reset database (DATA LOSS!)"
	@echo ""
	@echo "  Logs:"
	@echo "    make api-logs      - Follow API Gateway logs"
	@echo "    make worker-logs   - Follow Worker ETL logs"
	@echo "    make frontend-logs - Follow Frontend logs"
	@echo ""
	@echo "  Frontend:"
	@echo "    make frontend-dev  - Start Frontend (local dev mode)"
	@echo ""
	@echo "  Utilities:"
	@echo "    make redis-cli   - Open Redis CLI"
	@echo "    make psql        - Open PostgreSQL CLI"
	@echo "    make install     - Install all dependencies"
	@echo "    make lint        - Lint all code"
	@echo "    make format      - Format all code"
	@echo "    make typecheck   - Type check all code"

# -----------------------------------------------------------------------------
# Infrastructure
# -----------------------------------------------------------------------------

# Start all services
up:
	docker compose up -d
	@echo ""
	@echo "Services starting..."
	@echo "  - API Gateway:    http://localhost:3000"
	@echo "  - Worker ETL:     http://localhost:8000"
	@echo "  - MinIO Console:  http://localhost:9001"
	@echo ""
	@echo "Run 'make logs' to follow logs"

# Start all services with build
up-build:
	docker compose up -d --build

# Stop all services
down:
	docker compose down

# Restart all services
restart: down up

# Show running services
ps:
	docker compose ps

# Follow all logs
logs:
	docker compose logs -f

# Stop and remove volumes (WARNING: Data loss!)
clean:
	@echo "WARNING: This will delete all data (PostgreSQL, Redis, MinIO)"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	docker compose down -v
	@echo "All volumes removed"

# -----------------------------------------------------------------------------
# Individual service logs
# -----------------------------------------------------------------------------

api-logs:
	docker compose logs -f api

worker-logs:
	docker compose logs -f worker

redis-logs:
	docker compose logs -f redis

postgres-logs:
	docker compose logs -f postgres

minio-logs:
	docker compose logs -f minio

# -----------------------------------------------------------------------------
# Frontend
# -----------------------------------------------------------------------------

frontend-logs:
	docker compose logs -f frontend

frontend-dev:
	cd ../frontend && pnpm dev

# -----------------------------------------------------------------------------
# Database (Prisma)
# -----------------------------------------------------------------------------

# Run pending migrations
db-migrate:
	cd api && export $$(grep -v '^#' ../.env | xargs) && pnpm exec prisma migrate dev --schema=../prisma/schema.prisma

# Generate Prisma client
db-generate:
	cd api && export $$(grep -v '^#' ../.env | xargs) && pnpm exec prisma generate --schema=../prisma/schema.prisma

# Open Prisma Studio
db-studio:
	cd api && export $$(grep -v '^#' ../.env | xargs) && pnpm exec prisma studio --schema=../prisma/schema.prisma

# Reset database (WARNING: Data loss!)
db-reset:
	@echo "WARNING: This will reset the database"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	cd api && export $$(grep -v '^#' ../.env | xargs) && pnpm exec prisma migrate reset --schema=../prisma/schema.prisma

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

# Open Redis CLI
redis-cli:
	docker compose exec redis redis-cli

# Open PostgreSQL CLI
psql:
	docker compose exec postgres psql -U pymes -d pymes_dev

# -----------------------------------------------------------------------------
# Development shortcuts
# -----------------------------------------------------------------------------

# Start API in dev mode (local, not Docker)
api-dev:
	cd api && export $$(grep -v '^#' ../.env | xargs) && pnpm dev

# Start Worker in dev mode (local, not Docker)
worker-dev:
	cd worker && export $$(grep -v '^#' ../.env | xargs) && uv run uvicorn src.main:app --reload --port 8000

# Install all dependencies
install:
	cd api && pnpm install
	cd worker && uv sync

# Lint all code
lint:
	cd api && pnpm lint
	cd worker && uv run ruff check src

# Format all code
format:
	cd api && pnpm format
	cd worker && uv run ruff format src

# Type check all code
typecheck:
	cd api && pnpm typecheck
	cd worker && uv run mypy src

# -----------------------------------------------------------------------------
# Testing
# -----------------------------------------------------------------------------

# Run all tests
test: test-api test-worker

# Run API tests (Vitest)
test-api:
	cd api && pnpm test

# Run Worker tests (pytest)
test-worker:
	cd worker && source .venv/bin/activate && python -m pytest

# Run all tests with coverage
test-coverage:
	@echo "=== API Coverage ==="
	cd api && pnpm test:coverage
	@echo ""
	@echo "=== Worker Coverage ==="
	cd worker && source .venv/bin/activate && python -m pytest --cov=src --cov-report=html

# ── CI Loop ──────────────────────────────────────────────────────────────────
# Esperar resultado del último run de GitHub Actions.
# Si falla, imprime los logs para que el agente pueda corregir y re-pushear.
ci-watch:
	@./scripts/ci-watch.sh
