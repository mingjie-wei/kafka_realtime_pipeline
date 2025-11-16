import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime
from collections import defaultdict
import threading
import time


class WindowedConsumer:
    """
    Kafka consumer with windowed aggregation capabilities.
    Implements tumbling and sliding windows for real-time analytics.
    """
    
    def __init__(self):
        self.window_data_1min = defaultdict(list)  # 1-minute tumbling window
        self.window_data_5min = defaultdict(list)  # 5-minute sliding window
        self.lock = threading.Lock()
        
        # Connect to PostgreSQL
        self.conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5432",
        )
        self.conn.autocommit = True
        self.setup_tables()
        
        # Start window processing threads
        self.start_window_processors()
    
    def setup_tables(self):
        """Initialize PostgreSQL tables."""
        cur = self.conn.cursor()
        
        # Raw sensor data table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sensor_data (
                sensor_id VARCHAR(50),
                zone VARCHAR(100),
                latitude NUMERIC(10, 6),
                longitude NUMERIC(10, 6),
                timestamp TIMESTAMP,
                temperature NUMERIC(10, 2),
                humidity NUMERIC(10, 2),
                air_quality_index NUMERIC(10, 2),
                noise_level NUMERIC(10, 2),
                traffic_flow INTEGER,
                is_anomaly BOOLEAN,
                PRIMARY KEY (sensor_id, timestamp)
            );
        """)
        
        # 1-minute tumbling window aggregations
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sensor_metrics_1min (
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                zone VARCHAR(100),
                avg_temperature NUMERIC(10, 2),
                avg_humidity NUMERIC(10, 2),
                avg_air_quality NUMERIC(10, 2),
                avg_noise_level NUMERIC(10, 2),
                avg_traffic_flow NUMERIC(10, 2),
                max_temperature NUMERIC(10, 2),
                max_air_quality NUMERIC(10, 2),
                min_temperature NUMERIC(10, 2),
                sensor_count INTEGER,
                anomaly_count INTEGER,
                PRIMARY KEY (window_start, zone)
            );
        """)
        
        # 5-minute sliding window aggregations
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sensor_metrics_5min (
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                zone VARCHAR(100),
                avg_temperature NUMERIC(10, 2),
                avg_humidity NUMERIC(10, 2),
                avg_air_quality NUMERIC(10, 2),
                avg_noise_level NUMERIC(10, 2),
                avg_traffic_flow NUMERIC(10, 2),
                max_temperature NUMERIC(10, 2),
                max_air_quality NUMERIC(10, 2),
                min_temperature NUMERIC(10, 2),
                sensor_count INTEGER,
                anomaly_count INTEGER,
                PRIMARY KEY (window_start, zone)
            );
        """)
        
        cur.close()
        print("[Consumer] ✓ All tables initialized")
    
    def store_raw_data(self, sensor_data):
        """Store raw sensor reading in PostgreSQL."""
        try:
            cur = self.conn.cursor()
            insert_query = """
                INSERT INTO sensor_data 
                (sensor_id, zone, latitude, longitude, timestamp, temperature, 
                 humidity, air_quality_index, noise_level, traffic_flow, is_anomaly)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sensor_id, timestamp) DO NOTHING;
            """
            cur.execute(insert_query, (
                sensor_data["sensor_id"],
                sensor_data["zone"],
                sensor_data["latitude"],
                sensor_data["longitude"],
                sensor_data["timestamp"],
                sensor_data["temperature"],
                sensor_data["humidity"],
                sensor_data["air_quality_index"],
                sensor_data["noise_level"],
                sensor_data["traffic_flow"],
                sensor_data.get("is_anomaly", False),
            ))
            cur.close()
        except Exception as e:
            print(f"[Consumer ERROR] Failed to store raw data: {e}")
    
    def add_to_windows(self, sensor_data):
        """Add sensor data to window buffers."""
        with self.lock:
            zone = sensor_data["zone"]
            self.window_data_1min[zone].append(sensor_data)
            self.window_data_5min[zone].append(sensor_data)
    
    def process_window(self, window_data, table_name, window_duration):
        """Process and aggregate window data."""
        with self.lock:
            for zone, readings in list(window_data.items()):
                if not readings:
                    continue
                
                # Calculate window boundaries
                now = datetime.now()
                window_start = now.replace(second=0, microsecond=0)
                window_end = window_start
                
                # Calculate aggregations
                temps = [r['temperature'] for r in readings]
                humidities = [r['humidity'] for r in readings]
                aqis = [r['air_quality_index'] for r in readings]
                noises = [r['noise_level'] for r in readings]
                traffics = [r['traffic_flow'] for r in readings]
                anomalies = [r.get('is_anomaly', False) for r in readings]
                
                metrics = {
                    'window_start': window_start,
                    'window_end': window_end,
                    'zone': zone,
                    'avg_temperature': round(sum(temps) / len(temps), 2),
                    'avg_humidity': round(sum(humidities) / len(humidities), 2),
                    'avg_air_quality': round(sum(aqis) / len(aqis), 2),
                    'avg_noise_level': round(sum(noises) / len(noises), 2),
                    'avg_traffic_flow': round(sum(traffics) / len(traffics), 2),
                    'max_temperature': round(max(temps), 2),
                    'max_air_quality': round(max(aqis), 2),
                    'min_temperature': round(min(temps), 2),
                    'sensor_count': len(readings),
                    'anomaly_count': sum(anomalies),
                }
                
                # Store aggregated metrics
                self.store_window_metrics(metrics, table_name)
                
                print(f"[Window-{window_duration}] Zone: {zone} | Sensors: {metrics['sensor_count']} | "
                      f"Avg Temp: {metrics['avg_temperature']}°C | Avg AQI: {metrics['avg_air_quality']} | "
                      f"Anomalies: {metrics['anomaly_count']}")
                
                # Clear processed data
                window_data[zone] = []
    
    def store_window_metrics(self, metrics, table_name):
        """Store windowed aggregation metrics in PostgreSQL."""
        try:
            cur = self.conn.cursor()
            insert_query = f"""
                INSERT INTO {table_name} 
                (window_start, window_end, zone, avg_temperature, avg_humidity, 
                 avg_air_quality, avg_noise_level, avg_traffic_flow, 
                 max_temperature, max_air_quality, min_temperature, 
                 sensor_count, anomaly_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (window_start, zone) 
                DO UPDATE SET
                    window_end = EXCLUDED.window_end,
                    avg_temperature = EXCLUDED.avg_temperature,
                    avg_humidity = EXCLUDED.avg_humidity,
                    avg_air_quality = EXCLUDED.avg_air_quality,
                    avg_noise_level = EXCLUDED.avg_noise_level,
                    avg_traffic_flow = EXCLUDED.avg_traffic_flow,
                    max_temperature = EXCLUDED.max_temperature,
                    max_air_quality = EXCLUDED.max_air_quality,
                    min_temperature = EXCLUDED.min_temperature,
                    sensor_count = EXCLUDED.sensor_count,
                    anomaly_count = EXCLUDED.anomaly_count;
            """
            cur.execute(insert_query, (
                metrics['window_start'],
                metrics['window_end'],
                metrics['zone'],
                metrics['avg_temperature'],
                metrics['avg_humidity'],
                metrics['avg_air_quality'],
                metrics['avg_noise_level'],
                metrics['avg_traffic_flow'],
                metrics['max_temperature'],
                metrics['max_air_quality'],
                metrics['min_temperature'],
                metrics['sensor_count'],
                metrics['anomaly_count'],
            ))
            cur.close()
        except Exception as e:
            print(f"[Consumer ERROR] Failed to store window metrics: {e}")
    
    def window_processor_1min(self):
        """Process 1-minute tumbling windows."""
        while True:
            time.sleep(60)  # Process every 1 minute
            self.process_window(self.window_data_1min, 'sensor_metrics_1min', '1min')
    
    def window_processor_5min(self):
        """Process 5-minute sliding windows."""
        while True:
            time.sleep(300)  # Process every 5 minutes
            self.process_window(self.window_data_5min, 'sensor_metrics_5min', '5min')
    
    def start_window_processors(self):
        """Start background threads for window processing."""
        thread_1min = threading.Thread(target=self.window_processor_1min, daemon=True)
        thread_5min = threading.Thread(target=self.window_processor_5min, daemon=True)
        thread_1min.start()
        thread_5min.start()
        print("[Consumer] ✓ Window processors started (1min tumbling, 5min sliding)")


def run_consumer():
    """Consumes messages from Kafka and processes them with windowed aggregations."""
    try:
        print("[Consumer] Connecting to Kafka at localhost:9092...")
        consumer = KafkaConsumer(
            "sensor-data",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="sensor-consumer-group",
        )
        print("[Consumer] ✓ Connected to Kafka successfully!")
        
        print("[Consumer] Connecting to PostgreSQL...")
        windowed_consumer = WindowedConsumer()
        print("[Consumer] ✓ Connected to PostgreSQL successfully!")
        print("[Consumer] 🎧 Listening for sensor data...\n")

        message_count = 0
        for message in consumer:
            try:
                sensor_data = message.value
                
                # Store raw data
                windowed_consumer.store_raw_data(sensor_data)
                
                # Add to window buffers
                windowed_consumer.add_to_windows(sensor_data)
                
                message_count += 1
                anomaly_flag = "🚨" if sensor_data.get("is_anomaly") else "✓"
                print(
                    f"[Consumer] {anomaly_flag} #{message_count} Zone: {sensor_data['zone']} | "
                    f"Temp: {sensor_data['temperature']}°C | AQI: {sensor_data['air_quality_index']} | "
                    f"Traffic: {sensor_data['traffic_flow']}"
                )

            except Exception as e:
                print(f"[Consumer ERROR] Failed to process message: {e}")
                continue

    except Exception as e:
        print(f"[Consumer ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_consumer()
