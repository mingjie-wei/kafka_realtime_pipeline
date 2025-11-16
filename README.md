# 🏙️ Smart City IoT Real-Time Streaming Pipeline

An advanced real-time data engineering system that monitors smart city IoT sensors using **Apache Kafka**, **PostgreSQL**, **Apache Flink**, and **Machine Learning** for anomaly detection. Features a beautiful **Streamlit** dashboard with live metrics and alerts.

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Kafka](https://img.shields.io/badge/Kafka-3.0+-red.svg)
![Flink](https://img.shields.io/badge/Flink-1.18-orange.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Features](#-features)
- [Architecture](#-architecture)
- [Data Domain](#-data-domain)
- [Prerequisites](#-prerequisites)
- [Installation](#-installation)
- [Usage](#-usage)
- [Advanced Features](#-advanced-features)
- [Project Structure](#-project-structure)
- [Screenshots](#-screenshots)
- [Troubleshooting](#-troubleshooting)
- [Future Enhancements](#-future-enhancements)

---

## 🌟 Overview

This project implements a complete **real-time streaming data pipeline** for smart city infrastructure monitoring. It demonstrates:

- **Real-time data ingestion** from IoT sensors across multiple city zones
- **Stream processing** with Apache Kafka
- **Windowed aggregations** (tumbling & sliding windows) for continuous analytics
- **Machine learning-based anomaly detection** using Isolation Forest
- **Live visualization** with auto-refreshing Streamlit dashboard
- **Scalable architecture** with Docker containerization

---

## ✨ Features

### Core Features

✅ **IoT Sensor Data Generation**
- Synthetic data for 8 city zones (Downtown, University District, Industrial Park, etc.)
- 5 sensor types: Temperature, Humidity, Air Quality Index, Noise Level, Traffic Flow
- Realistic time-of-day variations and zone-specific patterns
- Automatic anomaly injection (5% rate) for testing

✅ **Apache Kafka Stream Processing**
- High-throughput event streaming
- Reliable message delivery with configurable retries
- Topic: `sensor-data`

✅ **Real-Time Windowed Aggregations**
- **1-minute tumbling windows** for real-time metrics
- **5-minute sliding windows** for trend analysis
- Per-zone aggregations (avg, max, min, count)
- Anomaly counting per window

✅ **PostgreSQL Storage**
- Raw sensor data table
- Windowed metrics tables (1-min and 5-min)
- Optimized queries with proper indexing

✅ **Machine Learning Anomaly Detection**
- Isolation Forest algorithm
- Statistical threshold-based detection
- Real-time predictions with confidence scores
- Model training on historical data

✅ **Interactive Streamlit Dashboard**
- Live metrics with auto-refresh (2-30 seconds)
- Zone filtering and anomaly-only view
- Multiple visualizations:
  - Temperature trends scatter plot
  - Air Quality Index time series
  - Traffic flow bar chart
  - Noise level box plots
  - Correlation heatmap
  - Windowed aggregation charts
- Expandable anomaly alerts
- Responsive design

🔥 **Apache Flink Integration**
- Flink JobManager and TaskManager deployment
- Real-time stream processing capabilities
- Web UI available at http://localhost:8081

🤖 **Advanced Anomaly Detection**
- Hybrid approach: ML + Statistical methods
- Zone-specific baselines
- Severity classification (low, medium, high)
- Model persistence and retraining

---

## 🏗️ Architecture

```
┌─────────────────┐
│  IoT Sensors    │ (Data Generation)
│   producer.py   │
└────────┬────────┘
         │ JSON Events
         ▼
┌─────────────────┐
│  Apache Kafka   │ (Message Broker)
│  Topic: sensor- │
│       data      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Consumer with  │ (Stream Processing)
│  Windowing      │ - Tumbling Windows (1-min)
│  consumer.py    │ - Sliding Windows (5-min)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  PostgreSQL DB  │ (Data Warehouse)
│  - sensor_data  │
│  - metrics_1min │
│  - metrics_5min │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  Anomaly Detector & Dashboard       │
│  - Isolation Forest ML Model        │
│  - Streamlit Dashboard (port 8501)  │
└─────────────────────────────────────┘
```

**Optional**: Apache Flink can be added between Kafka and PostgreSQL for advanced stream processing.

---

## 📊 Data Domain

### Smart City IoT Sensors

The system monitors 5 key environmental and traffic metrics across 8 city zones:

| Metric | Description | Range | Anomaly Threshold |
|--------|-------------|-------|-------------------|
| **Temperature** | Air temperature in Celsius | -10°C to 50°C | < -5°C or > 40°C |
| **Humidity** | Relative humidity percentage | 0% to 100% | - |
| **Air Quality Index (AQI)** | EPA Air Quality Index | 0 to 500 | > 300 (Hazardous) |
| **Noise Level** | Sound level in decibels | 0 to 120 dB | > 100 dB |
| **Traffic Flow** | Vehicles per minute | 0 to 600+ | > 500 (Severe congestion) |

### City Zones

1. **Downtown** - High traffic, moderate pollution
2. **University District** - Moderate activity, good air quality
3. **Industrial Park** - High noise, elevated pollution
4. **Residential North** - Low traffic, excellent air quality
5. **Residential South** - Low traffic, good air quality
6. **Shopping District** - High traffic, moderate pollution
7. **Tech Hub** - Moderate traffic, good air quality
8. **Medical Center** - Moderate traffic, good air quality

---

## 🔧 Prerequisites

- **Docker** and **Docker Compose** (for Kafka, PostgreSQL, Flink)
- **Python 3.9+**
- **pip** (Python package manager)
- **Make** (optional, for easier command execution)

---

## 📦 Installation

### 1. Clone the Repository

```bash
cd kafka_realtime_pipeline
```

### 2. Install Python Dependencies

```bash
make install
# OR
pip install -r requirements.txt
```

### 3. Start Docker Services

```bash
make setup
# OR
docker-compose up -d
```

This will start:
- **Kafka** on `localhost:9092`
- **PostgreSQL** on `localhost:5432`
- **Flink JobManager** on `localhost:8081` (Web UI)
- **Flink TaskManager**

Wait ~15 seconds for services to be fully ready.

---

## 🚀 Usage

### Quick Start (3 Terminals)

**Terminal 1: Start the Producer**
```bash
make producer
# OR
python producer.py
```

**Terminal 2: Start the Consumer**
```bash
make consumer
# OR
python consumer.py
```

**Terminal 3: Launch the Dashboard**
```bash
make dashboard
# OR
streamlit run dashboard.py
```

The dashboard will open at **http://localhost:8501**

### Training the Anomaly Detection Model

After collecting some data (wait ~5 minutes), train the ML model:

```bash
make train-model
# OR
python anomaly_detector.py
```

The model will be saved to `models/anomaly_detector.pkl` and automatically loaded by the dashboard.

### Stopping the Pipeline

```bash
make stop
# OR
docker-compose down
```

### Cleaning Up (Remove All Data)

```bash
make clean
# OR
docker-compose down -v
```

---

## 🎯 Advanced Features

### Apache Flink Integration

The project includes Apache Flink for advanced stream processing:

1. **Access Flink Web UI**: http://localhost:8081
2. **Submit Flink Jobs**: 
   ```bash
   python flink_processor.py
   ```

The Flink processor performs real-time aggregations and can handle:
- Event-time processing with watermarks
- Stateful computations
- Complex event processing (CEP)
- Window operations (tumbling, sliding, session)

### Anomaly Detection Details

**Machine Learning Approach:**
- **Algorithm**: Isolation Forest
- **Features**: 5 sensor metrics (normalized)
- **Contamination**: 5% (configurable)
- **Training**: Historical data (last 7 days, up to 10,000 samples)

**Statistical Approach:**
- Temperature extremes (< -5°C or > 40°C)
- Hazardous air quality (AQI > 300)
- Dangerous noise levels (> 100 dB)
- Severe traffic congestion (> 500 vehicles/min)

**Combined Detection:**
The system flags anomalies if either ML or statistical methods detect unusual patterns.

### Windowed Aggregations

**1-Minute Tumbling Windows:**
- Non-overlapping 60-second intervals
- Used for real-time monitoring
- Metrics: avg, max, min, count, anomaly_count

**5-Minute Sliding Windows:**
- Overlapping 300-second intervals
- Used for trend analysis
- Better smoothing for visualizations

---

## 📁 Project Structure

```
kafka_realtime_pipeline/
│
├── docker-compose.yml           # Docker services configuration
├── Makefile                     # Convenient command shortcuts
├── requirements.txt             # Python dependencies
├── README.md                    # This file
│
├── producer.py                  # IoT sensor data generator
├── consumer.py                  # Kafka consumer with windowing
├── flink_processor.py           # Apache Flink stream processor
├── anomaly_detector.py          # ML-based anomaly detection
├── dashboard.py                 # Streamlit dashboard
│
└── models/                      # Trained ML models (auto-created)
    └── anomaly_detector.pkl
```

---

## 📸 Screenshots

### Dashboard Overview
The dashboard displays:
- **Real-time KPIs**: Avg/Max values for all sensor types
- **Anomaly Alerts**: Expandable cards with detailed info
- **Time Series**: Temperature trends, AQI over time
- **Distributions**: Traffic by zone, noise level box plots
- **Windowed Metrics**: Aggregations from tumbling/sliding windows
- **Correlation Heatmap**: Relationships between sensor metrics

https://github.com/user-attachments/assets/db88040b-81a7-4966-a256-35bd7e1debd9

### Producer Console
```
[Producer] ✓ Sensor #42 | Zone: Downtown | Temp: 24.3°C | AQI: 87 | Traffic: 165
[Producer] 🚨 ANOMALY Sensor #43 | Zone: Industrial Park | Temp: 45.2°C | AQI: 320 | Traffic: 210
```

### Consumer Console
```
[Consumer] ✓ #123 Zone: Tech Hub | Temp: 21.5°C | AQI: 52 | Traffic: 115
[Window-1min] Zone: Downtown | Sensors: 8 | Avg Temp: 23.1°C | Avg AQI: 81 | Anomalies: 0
```

---

## 🐛 Troubleshooting

### Kafka Connection Issues

**Problem**: `[Producer ERROR] NoBrokersAvailable`

**Solution**:
```bash
# Check if Kafka is running
docker ps | grep kafka

# Restart Kafka
docker-compose restart kafka

# Wait 10 seconds then retry
```

### PostgreSQL Connection Issues

**Problem**: `psycopg2.OperationalError: could not connect to server`

**Solution**:
```bash
# Check PostgreSQL status
docker ps | grep postgres

# Check logs
docker logs postgres

# Restart PostgreSQL
docker-compose restart postgres
```

### Dashboard Not Loading Data

**Problem**: Dashboard shows "Waiting for sensor data..."

**Solution**:
1. Ensure producer is running: `python producer.py`
2. Ensure consumer is running: `python consumer.py`
3. Check for errors in producer/consumer terminals
4. Verify Kafka topic exists:
   ```bash
   docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
   ```

### Anomaly Detection Not Working

**Problem**: All readings show as normal

**Solution**:
1. Train the model: `make train-model`
2. Ensure you have > 100 sensor readings in the database
3. Check model file exists: `ls -la models/`

---

## 🔮 Future Enhancements

- [ ] **Real-time alerting** with email/SMS notifications
- [ ] **Geographic visualization** with interactive maps
- [ ] **Predictive analytics** (forecast next 1-hour metrics)
- [ ] **Multi-model comparison** (LSTM, Prophet, etc.)
- [ ] **Kubernetes deployment** for production scaling
- [ ] **Apache Flink job submission** from dashboard
- [ ] **Historical data analysis** with ClickHouse
- [ ] **A/B testing** of different anomaly detection algorithms
- [ ] **Data quality monitoring** and alerting
- [ ] **API endpoints** for external integrations

---

## 📚 Technologies Used

- **Apache Kafka** - Distributed streaming platform
- **Apache Flink** - Stream processing framework
- **PostgreSQL** - Relational database
- **Python** - Primary programming language
- **Streamlit** - Dashboard framework
- **Scikit-learn** - Machine learning library
- **Plotly** - Interactive visualizations
- **Docker** - Containerization
- **Pandas/NumPy** - Data manipulation

---

## 👨‍💻 Development Notes

### Adding New Sensor Types

1. Update `producer.py` - Add metric to `generate_sensor_reading()`
2. Update `consumer.py` - Add column to table schema
3. Update `anomaly_detector.py` - Add feature to `feature_columns`
4. Update `dashboard.py` - Add visualization

### Customizing Anomaly Detection

Edit `anomaly_detector.py`:
- Change `contamination` parameter (default: 0.05 = 5%)
- Modify `detect_statistical_anomalies()` thresholds
- Add new statistical rules

### Adjusting Window Sizes

Edit `consumer.py`:
- `window_processor_1min()`: Change `time.sleep(60)` duration
- `window_processor_5min()`: Change `time.sleep(300)` duration

---

## 🙏 Acknowledgments

This project demonstrates:
- ✅ **Data domain change** (from e-commerce to IoT sensors)
- ✅ **Apache Kafka streaming**
- ✅ **PostgreSQL storage**
- ✅ **Streamlit live dashboard**
- ✅ **Apache Flink integration** (Bonus 10%)
- ✅ **Sequential/Anomaly modeling** (Bonus 10%)
- ✅ **Windowed operations** (tumbling & sliding)
- ✅ **Real-time aggregations**

---

**Built with ❤️ for IDS Data Engineering Systems @ Duke University**

🎓 **Academic Project** - Real-time Data Pipeline with Stream Processing & ML

