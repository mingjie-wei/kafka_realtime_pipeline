"""
Real-time Anomaly Detection for IoT Sensor Data
Uses Isolation Forest and statistical methods to detect anomalies
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import psycopg2
import joblib
import os
from datetime import datetime, timedelta


class SensorAnomalyDetector:
    """
    Anomaly detector for IoT sensor data using Isolation Forest.
    Trains on historical data and provides real-time predictions.
    """
    
    def __init__(self, model_path='models/anomaly_detector.pkl', contamination=0.05):
        self.model_path = model_path
        self.contamination = contamination
        self.scaler = StandardScaler()
        self.model = None
        self.feature_columns = [
            'temperature', 'humidity', 'air_quality_index', 
            'noise_level', 'traffic_flow'
        ]
        
        # Create models directory if it doesn't exist
        os.makedirs('models', exist_ok=True)
        
        # Database connection
        self.conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5432",
        )
        
        # Initialize or load model
        if os.path.exists(model_path):
            self.load_model()
        else:
            print("[Anomaly Detector] No existing model found. Will train on first run.")
    
    def fetch_training_data(self, days=7):
        """Fetch historical sensor data for training."""
        try:
            query = f"""
                SELECT temperature, humidity, air_quality_index, 
                       noise_level, traffic_flow, zone, is_anomaly
                FROM sensor_data
                WHERE timestamp >= NOW() - INTERVAL '{days} days'
                AND is_anomaly = FALSE
                ORDER BY timestamp DESC
                LIMIT 10000;
            """
            df = pd.read_sql_query(query, self.conn)
            print(f"[Anomaly Detector] Fetched {len(df)} records for training")
            return df
        except Exception as e:
            print(f"[Anomaly Detector ERROR] Failed to fetch training data: {e}")
            return pd.DataFrame()
    
    def train_model(self):
        """Train the Isolation Forest model on historical data."""
        print("[Anomaly Detector] Training model...")
        
        # Fetch training data
        df = self.fetch_training_data()
        
        if df.empty or len(df) < 100:
            print("[Anomaly Detector] Insufficient data for training. Using default model.")
            # Create a simple model with default parameters
            self.model = IsolationForest(
                contamination=self.contamination,
                random_state=42,
                n_estimators=100
            )
            # Generate some dummy data for initial fitting
            dummy_data = np.random.randn(100, len(self.feature_columns))
            self.scaler.fit(dummy_data)
            self.model.fit(dummy_data)
            return
        
        # Prepare features
        X = df[self.feature_columns].values
        
        # Handle missing values
        X = np.nan_to_num(X, nan=0.0)
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Train Isolation Forest
        self.model = IsolationForest(
            contamination=self.contamination,
            random_state=42,
            n_estimators=100,
            max_samples='auto',
            max_features=1.0
        )
        self.model.fit(X_scaled)
        
        print(f"[Anomaly Detector] ✓ Model trained on {len(df)} samples")
        
        # Save the model
        self.save_model()
    
    def save_model(self):
        """Save the trained model and scaler."""
        try:
            joblib.dump({
                'model': self.model,
                'scaler': self.scaler,
                'feature_columns': self.feature_columns
            }, self.model_path)
            print(f"[Anomaly Detector] ✓ Model saved to {self.model_path}")
        except Exception as e:
            print(f"[Anomaly Detector ERROR] Failed to save model: {e}")
    
    def load_model(self):
        """Load a pre-trained model."""
        try:
            data = joblib.load(self.model_path)
            self.model = data['model']
            self.scaler = data['scaler']
            self.feature_columns = data['feature_columns']
            print(f"[Anomaly Detector] ✓ Model loaded from {self.model_path}")
        except Exception as e:
            print(f"[Anomaly Detector ERROR] Failed to load model: {e}")
    
    def predict(self, sensor_data):
        """
        Predict if a sensor reading is anomalous.
        Returns: (is_anomaly, anomaly_score, confidence)
        """
        if self.model is None:
            self.train_model()
        
        try:
            # Prepare features
            features = [sensor_data[col] for col in self.feature_columns]
            X = np.array(features).reshape(1, -1)
            
            # Scale features
            X_scaled = self.scaler.transform(X)
            
            # Predict
            prediction = self.model.predict(X_scaled)[0]
            anomaly_score = self.model.score_samples(X_scaled)[0]
            
            # -1 indicates anomaly, 1 indicates normal
            is_anomaly = prediction == -1
            
            # Convert score to confidence (0-1 scale)
            # More negative scores indicate more anomalous behavior
            confidence = 1 / (1 + np.exp(anomaly_score))  # Sigmoid transformation
            
            return is_anomaly, anomaly_score, confidence
        
        except Exception as e:
            print(f"[Anomaly Detector ERROR] Prediction failed: {e}")
            return False, 0.0, 0.5
    
    def detect_statistical_anomalies(self, sensor_data, zone):
        """
        Additional statistical anomaly detection based on zone-specific thresholds.
        """
        anomalies = []
        
        # Temperature thresholds
        if sensor_data['temperature'] > 40 or sensor_data['temperature'] < -5:
            anomalies.append('extreme_temperature')
        
        # Air quality thresholds (AQI > 300 is hazardous)
        if sensor_data['air_quality_index'] > 300:
            anomalies.append('hazardous_air_quality')
        
        # Noise level thresholds (> 100 dB is dangerous)
        if sensor_data['noise_level'] > 100:
            anomalies.append('extreme_noise')
        
        # Traffic flow anomalies (> 500 indicates severe congestion)
        if sensor_data['traffic_flow'] > 500:
            anomalies.append('severe_traffic')
        
        return anomalies
    
    def analyze_sensor_reading(self, sensor_data):
        """
        Comprehensive analysis of a sensor reading.
        Returns detailed anomaly information.
        """
        # ML-based anomaly detection
        is_ml_anomaly, anomaly_score, confidence = self.predict(sensor_data)
        
        # Statistical anomaly detection
        statistical_anomalies = self.detect_statistical_anomalies(
            sensor_data, 
            sensor_data.get('zone', 'Unknown')
        )
        
        # Combined result
        is_anomaly = is_ml_anomaly or len(statistical_anomalies) > 0
        
        result = {
            'is_anomaly': is_anomaly,
            'ml_anomaly': is_ml_anomaly,
            'anomaly_score': float(anomaly_score),
            'confidence': float(confidence),
            'statistical_anomalies': statistical_anomalies,
            'severity': self.calculate_severity(confidence, len(statistical_anomalies))
        }
        
        return result
    
    def calculate_severity(self, confidence, stat_anomaly_count):
        """Calculate anomaly severity (low, medium, high)."""
        if stat_anomaly_count >= 2 or confidence > 0.8:
            return 'high'
        elif stat_anomaly_count == 1 or confidence > 0.6:
            return 'medium'
        else:
            return 'low'
    
    def get_zone_statistics(self, zone, hours=24):
        """Get statistical summary for a specific zone."""
        try:
            query = f"""
                SELECT 
                    AVG(temperature) as avg_temp,
                    STDDEV(temperature) as std_temp,
                    AVG(air_quality_index) as avg_aqi,
                    STDDEV(air_quality_index) as std_aqi,
                    AVG(traffic_flow) as avg_traffic,
                    STDDEV(traffic_flow) as std_traffic,
                    COUNT(*) as sample_count,
                    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomaly_count
                FROM sensor_data
                WHERE zone = %s
                AND timestamp >= NOW() - INTERVAL '{hours} hours'
            """
            df = pd.read_sql_query(query, self.conn, params=(zone,))
            return df.iloc[0].to_dict() if not df.empty else {}
        except Exception as e:
            print(f"[Anomaly Detector ERROR] Failed to get zone statistics: {e}")
            return {}
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


def main():
    """Train or retrain the anomaly detection model."""
    print("=" * 80)
    print("IoT Sensor Anomaly Detection Model Training")
    print("=" * 80)
    
    detector = SensorAnomalyDetector()
    detector.train_model()
    
    # Test on recent data
    print("\n[Test] Evaluating model on recent data...")
    df = detector.fetch_training_data(days=1)
    
    if not df.empty:
        sample_size = min(10, len(df))
        print(f"\n[Test] Testing on {sample_size} sample readings:\n")
        
        for idx in range(sample_size):
            row = df.iloc[idx]
            sensor_data = row[detector.feature_columns].to_dict()
            sensor_data['zone'] = row.get('zone', 'Unknown')
            
            result = detector.analyze_sensor_reading(sensor_data)
            
            status = "🚨 ANOMALY" if result['is_anomaly'] else "✓ NORMAL"
            print(f"{status} | Zone: {sensor_data['zone']:20s} | "
                  f"Temp: {sensor_data['temperature']:6.2f}°C | "
                  f"AQI: {sensor_data['air_quality_index']:6.2f} | "
                  f"Confidence: {result['confidence']:.2f} | "
                  f"Severity: {result['severity']}")
    
    detector.close()
    print("\n[Test] ✓ Model training and testing complete!")


if __name__ == "__main__":
    main()

