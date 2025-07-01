# Optimized Makefile for Data Pipeline
.PHONY: help install test test-fast test-full clean docker-build docker-run demo stop

# Default target
help:
	@echo "🚀 Data Pipeline - Available Commands:"
	@echo ""
	@echo "📦 Setup:"
	@echo "  install       - Install dependencies"
	@echo "  clean         - Clean all temporary files"
	@echo ""
	@echo "🧪 Testing:"
	@echo "  test          - Run fast core tests"
	@echo "  test-full     - Run comprehensive test suite"
	@echo ""
	@echo "🐳 Docker:"
	@echo "  docker-build  - Build Docker containers"
	@echo "  docker-run    - Run the pipeline in Docker"
	@echo "  demo          - Run full BI/ML demonstration"
	@echo "  stop          - Stop all Docker services"
	@echo ""
	@echo "🔧 Development:"
	@echo "  format        - Format code with black"
	@echo "  lint          - Run code linting"

# Install dependencies
install:
	pip install -r requirements.txt
	@echo "✅ Dependencies installed"

# Fast tests (core functionality only)
test:
	python -m pytest tests/test_consolidated.py -v
	@echo "✅ Core tests completed"

# Full test suite
test-full:
	python run_tests.py
	@echo "✅ Full test suite completed"

# Clean all temporary files
clean:
	@echo "🧹 Cleaning temporary files..."
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	find . -name ".DS_Store" -delete 2>/dev/null || true
	find . -name ".pytest_cache" -type d -exec rm -rf {} + 2>/dev/null || true
	rm -rf /tmp/delta-table-test /tmp/pipeline_test_* /tmp/delta_test_* 2>/dev/null || true
	rm -f logs/test_*.log 2>/dev/null || true
	@echo "✅ Cleanup completed"

# Docker operations
docker-build:
	docker-compose build
	@echo "✅ Docker containers built"

docker-run:
	docker-compose up -d
	@echo "✅ Docker services started"
	@echo "📊 Access URLs:"
	@echo "   • Jupyter Lab: http://localhost:8888"
	@echo "   • Spark Master UI: http://localhost:8080"

demo:
	@echo "🚀 Starting BI/ML demonstration..."
	./run_bi_ml_demo.sh

stop:
	docker-compose down
	@echo "✅ Docker services stopped"

# Development tools (optional)
format:
	@command -v black >/dev/null 2>&1 && black src/ tests/ || echo "⚠️  black not installed (pip install black)"

lint:
	@command -v pylint >/dev/null 2>&1 && pylint src/ || echo "⚠️  pylint not available"

# Quick health check
health:
	@echo "🔍 Pipeline Health Check:"
	@python -c "import src.main; print('✅ Main module')" 2>/dev/null || echo "❌ Main module"
	@python -c "from pyspark.sql import SparkSession; print('✅ Spark')" 2>/dev/null || echo "❌ Spark"
	@python -c "import delta; print('✅ Delta Lake')" 2>/dev/null || echo "❌ Delta Lake"
	@python -c "import pytest; print('✅ Pytest')" 2>/dev/null || echo "❌ Pytest"
