import time
import json
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
import numpy as np

fake = Faker()

# IoT Smart City Sensor Configuration
SENSOR_LOCATIONS = [
    {"zone": "Downtown", "lat": 35.9940, "lon": -78.8986},
    {"zone": "University District", "lat": 36.0014, "lon": -78.9382},
    {"zone": "Industrial Park", "lat": 35.9751, "lon": -78.8654},
    {"zone": "Residential North", "lat": 36.0182, "lon": -78.9094},
    {"zone": "Residential South", "lat": 35.9765, "lon": -78.9008},
    {"zone": "Shopping District", "lat": 35.9925, "lon": -78.8750},
    {"zone": "Tech Hub", "lat": 36.0053, "lon": -78.9428},
    {"zone": "Medical Center", "lat": 35.9890, "lon": -78.9200},
]

SENSOR_TYPES = ["temperature", "humidity", "air_quality", "noise_level", "traffic_flow"]

# Baseline values for different zones
ZONE_PROFILES = {
    "Downtown": {"temp": 22, "humidity": 55, "aqi": 80, "noise": 70, "traffic": 150},
    "University District": {"temp": 21, "humidity": 60, "aqi": 45, "noise": 55, "traffic": 80},
    "Industrial Park": {"temp": 24, "humidity": 50, "aqi": 120, "noise": 85, "traffic": 200},
    "Residential North": {"temp": 20, "humidity": 65, "aqi": 35, "noise": 45, "traffic": 50},
    "Residential South": {"temp": 21, "humidity": 62, "aqi": 40, "noise": 48, "traffic": 60},
    "Shopping District": {"temp": 23, "humidity": 52, "aqi": 70, "noise": 75, "traffic": 180},
    "Tech Hub": {"temp": 21, "humidity": 58, "aqi": 50, "noise": 60, "traffic": 120},
    "Medical Center": {"temp": 22, "humidity": 55, "aqi": 42, "noise": 52, "traffic": 90},
}


def add_time_variation(base_value, hour):
    """Add time-of-day variations to sensor readings."""
    # Morning peak (7-9am): +20%
    # Midday (11am-2pm): +15%
    # Evening peak (5-7pm): +30%
    # Night (10pm-6am): -30%
    
    if 7 <= hour <= 9:
        factor = 1.2
    elif 11 <= hour <= 14:
        factor = 1.15
    elif 17 <= hour <= 19:
        factor = 1.3
    elif hour >= 22 or hour <= 6:
        factor = 0.7
    else:
        factor = 1.0
    
    return base_value * factor


def generate_sensor_reading():
    """Generates synthetic IoT sensor data for smart city monitoring."""
    location = random.choice(SENSOR_LOCATIONS)
    zone = location["zone"]
    profile = ZONE_PROFILES[zone]
    
    current_time = datetime.now()
    hour = current_time.hour
    
    # Base values with time variations
    temp_base = add_time_variation(profile["temp"], hour)
    traffic_base = add_time_variation(profile["traffic"], hour)
    
    # Generate readings with realistic variations
    temperature = round(np.random.normal(temp_base, 2.5), 2)
    humidity = round(np.random.normal(profile["humidity"], 5), 2)
    air_quality_index = max(0, round(np.random.normal(profile["aqi"], 15)))
    noise_level = round(np.random.normal(profile["noise"], 8), 2)
    traffic_flow = max(0, round(np.random.normal(traffic_base, 25)))
    
    # Occasionally inject anomalies (5% chance)
    is_anomaly = random.random() < 0.05
    if is_anomaly:
        anomaly_type = random.choice(["temp_spike", "aqi_spike", "noise_spike", "traffic_jam"])
        if anomaly_type == "temp_spike":
            temperature += random.uniform(10, 20)
        elif anomaly_type == "aqi_spike":
            air_quality_index += random.uniform(100, 200)
        elif anomaly_type == "noise_spike":
            noise_level += random.uniform(30, 50)
        elif anomaly_type == "traffic_jam":
            traffic_flow += random.uniform(150, 300)
    
    # Clamp values to realistic ranges
    temperature = max(-10, min(50, temperature))
    humidity = max(0, min(100, humidity))
    air_quality_index = max(0, min(500, air_quality_index))
    noise_level = max(0, min(120, noise_level))
    
    return {
        "sensor_id": str(uuid.uuid4())[:8],
        "zone": zone,
        "latitude": location["lat"],
        "longitude": location["lon"],
        "timestamp": current_time.isoformat(),
        "temperature": temperature,
        "humidity": humidity,
        "air_quality_index": air_quality_index,
        "noise_level": noise_level,
        "traffic_flow": traffic_flow,
        "is_anomaly": is_anomaly,
    }


def run_producer():
    """Kafka producer that sends synthetic IoT sensor readings to the 'sensor-data' topic."""
    try:
        print("[Producer] Connecting to Kafka at localhost:9092...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            max_block_ms=60000,
            retries=5,
        )
        print("[Producer] ✓ Connected to Kafka successfully!")
        print("[Producer] 📡 Generating Smart City IoT Sensor Data...\n")

        count = 0
        while True:
            sensor_reading = generate_sensor_reading()
            
            # Colorize anomaly messages
            anomaly_flag = "🚨 ANOMALY" if sensor_reading["is_anomaly"] else "✓"
            print(
                f"[Producer] {anomaly_flag} Sensor #{count} | Zone: {sensor_reading['zone']} | "
                f"Temp: {sensor_reading['temperature']}°C | AQI: {sensor_reading['air_quality_index']} | "
                f"Traffic: {sensor_reading['traffic_flow']}"
            )

            future = producer.send("sensor-data", value=sensor_reading)
            record_metadata = future.get(timeout=10)

            producer.flush()
            count += 1

            # Generate sensors at a faster rate (every 0.5-1.5 seconds)
            sleep_time = random.uniform(0.5, 1.5)
            time.sleep(sleep_time)

    except Exception as e:
        print(f"[Producer ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_producer()
