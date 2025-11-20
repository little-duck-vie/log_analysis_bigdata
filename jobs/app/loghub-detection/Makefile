.PHONY: help install run test clean docker-build docker-up docker-down mock

help:
	@echo "Loghub Detection Dashboard - Available Commands"
	@echo ""
	@echo "Setup & Installation:"
	@echo "  make install        Install dependencies"
	@echo "  make setup          Setup environment (.env from .env.example)"
	@echo ""
	@echo "Running:"
	@echo "  make run            Run Flask + Socket.IO server"
	@echo "  make mock           Run with mock data (no HBase needed)"
	@echo "  make test           Run unit tests"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-build   Build Docker image"
	@echo "  make docker-up      Start Docker Compose stack"
	@echo "  make docker-down    Stop Docker Compose stack"
	@echo "  make docker-logs    Show Docker logs"
	@echo ""
	@echo "Data:"
	@echo "  make generate-data  Generate test data in HBase"
	@echo "  make test-api       Test API endpoints"
	@echo ""
	@echo "Maintenance:"
	@echo "  make clean          Clean cache & logs"
	@echo "  make lint           Run linter (if installed)"
	@echo ""

install:
	@echo "Installing dependencies..."
	pip install -r requirements.txt
	@echo "✅ Dependencies installed"

setup:
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "✅ .env created from .env.example"; \
		echo "📝 Edit .env with your HBase settings"; \
	else \
		echo "⚠️  .env already exists"; \
	fi

run:
	@echo "Starting Loghub Detection Dashboard..."
	python app.py

mock:
	@echo "Starting with Mock HBase Data..."
	@echo "Note: Edit app.py line 'from hbase_client import HBaseClient' to use mock_hbase_client"
	@echo "Replacing import..."
	@sed -i 's/from hbase_client import/from mock_hbase_client import/' app.py || true
	python app.py

test:
	@echo "Running unit tests..."
	python -m unittest test_hbase_client.py -v

test-api:
	@echo "Testing API endpoints..."
	@echo ""
	@echo "1. Health Check:"
	curl -s http://localhost:5000/health | python -m json.tool
	@echo ""
	@echo "2. Get Latest Detections:"
	curl -s "http://localhost:5000/api/latest?limit=5" | python -m json.tool | head -30
	@echo ""
	@echo "3. Get Statistics:"
	curl -s http://localhost:5000/api/stats | python -m json.tool
	@echo ""

generate-data:
	@echo "Generating test data in HBase..."
	python generate_test_data.py --count 200 --host localhost --port 9090

docker-build:
	@echo "Building Docker image..."
	docker build -t loghub-detection:latest .
	@echo "✅ Docker image built"

docker-up:
	@echo "Starting Docker Compose stack..."
	docker-compose up -d
	@echo "✅ Stack started"
	@echo "   Dashboard: http://localhost:5000"
	@echo "   HBase Web: http://localhost:16010"
	@sleep 5
	@echo ""
	@echo "Checking services..."
	@docker-compose ps

docker-down:
	@echo "Stopping Docker Compose stack..."
	docker-compose down
	@echo "✅ Stack stopped"

docker-logs:
	docker-compose logs -f dashboard

docker-sh:
	docker-compose exec dashboard bash

clean:
	@echo "Cleaning cache & logs..."
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.log" -delete
	@echo "✅ Cleaned"

lint:
	@echo "Running pylint..."
	pylint app.py hbase_client.py config.py || true

.DEFAULT_GOAL := help
