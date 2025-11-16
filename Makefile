.PHONY: help install setup start stop clean train-model producer consumer dashboard logs

help:
	@echo "Smart City IoT Real-Time Pipeline"
	@echo "=================================="
	@echo ""
	@echo "Available commands:"
	@echo "  make install        - Install Python dependencies"
	@echo "  make setup          - Start Docker services (Kafka, PostgreSQL, Flink)"
	@echo "  make start          - Start all services in separate terminals"
	@echo "  make stop           - Stop all Docker services"
	@echo "  make clean          - Clean up containers and volumes"
	@echo "  make train-model    - Train the anomaly detection model"
	@echo "  make producer       - Run the IoT sensor data producer"
	@echo "  make consumer       - Run the Kafka consumer with windowing"
	@echo "  make dashboard      - Launch the Streamlit dashboard"
	@echo "  make logs           - Show Docker logs"
	@echo ""

install:
	pip install -r requirements.txt

setup:
	@echo "🚀 Starting Docker services..."
	docker-compose up -d
	@echo "⏳ Waiting for services to be ready..."
	sleep 15
	@echo "✅ Services are ready!"
	@echo ""
	@echo "📊 Service Status:"
	@echo "  - Kafka: localhost:9092"
	@echo "  - PostgreSQL: localhost:5432"
	@echo "  - Flink JobManager UI: http://localhost:8081"
	@echo ""

start:
	@echo "⚡ Starting the pipeline..."
	@echo ""
	@echo "Please run these commands in separate terminals:"
	@echo "  1. make producer   (Generate IoT sensor data)"
	@echo "  2. make consumer   (Process and store data)"
	@echo "  3. make dashboard  (View real-time dashboard)"
	@echo ""

stop:
	@echo "🛑 Stopping Docker services..."
	docker-compose down
	@echo "✅ Services stopped"

clean:
	@echo "🧹 Cleaning up..."
	docker-compose down -v
	rm -rf models/*.pkl
	@echo "✅ Cleanup complete"

train-model:
	@echo "🤖 Training anomaly detection model..."
	python anomaly_detector.py
	@echo "✅ Model training complete"

producer:
	@echo "📡 Starting IoT sensor data producer..."
	python producer.py

consumer:
	@echo "📥 Starting Kafka consumer with windowed aggregations..."
	python consumer.py

dashboard:
	@echo "📊 Launching Streamlit dashboard..."
	@echo "🌐 Dashboard will open at http://localhost:8501"
	streamlit run dashboard.py

logs:
	docker-compose logs -f