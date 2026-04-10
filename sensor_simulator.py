
import json
import time
import random
import ssl
import threading
import boto3
import requests
from datetime import datetime
import paho.mqtt.client as mqtt


import os

class DisasterSensorSimulator:
    def __init__(self):
        self.is_running = False
        self.mqtt_client = None
        self.thread = None
        self.stop_thread = False
        
        # AWS IoT Core Configuration
        self.ENDPOINT = "a26mg3sf6twdp7-ats.iot.us-east-1.amazonaws.com"
        self.PORT = 8883
        self.TOPIC = "smart/disaster/detection"
        self.CLIENT_ID = "sensor-client"
        
        base_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Certificate paths
        self.CA_PATH = os.path.join(base_dir, "AmazonRootCA1.pem")
        self.CERT_PATH = os.path.join(base_dir, "certificate.pem.crt")
        self.KEY_PATH = os.path.join(base_dir, "private.pem.key")
        
        # Lambda-2 API Gateway URL
        self.LAMBDA_2_URL = "https://6xhiwcxx5j.execute-api.us-east-1.amazonaws.com/default/x24274429-lambda-database-fog"
        
        # Alert probability and intensity control
        self.alert_mode = False
        self.alert_duration = 0
        self.normal_duration = 0
        self.alert_intensity = 1.0  # 1.0 = normal, 2.0 = severe
        
    def generate_normal_data(self):
        """Generate normal sensor data (no alerts)"""
        return {
            "device_id": "disaster-sensor-001",
            "timestamp": datetime.utcnow().isoformat(),
            "temperature": round(random.uniform(20, 35), 2),      # Normal: 20-35°C
            "humidity": round(random.uniform(30, 60), 2),        # Normal: 30-60%
            "rainfall": round(random.uniform(0, 30), 2),         # Normal: 0-30mm
            "water_level": round(random.uniform(0, 3), 2),       # Normal: 0-3m
            "seismic": round(random.uniform(0, 2), 2),           # Normal: 0-2
            "wind_speed": round(random.uniform(0, 40), 2)        # Normal: 0-40 km/h
        }
    
    def generate_alert_data(self):
        """Generate alert data based on intensity"""
        # Different disaster scenarios
        scenarios = [
            # Wildfire scenario - high temperature
            {
                "temperature": round(random.uniform(50, 80), 2),
                "humidity": round(random.uniform(20, 40), 2),
                "rainfall": round(random.uniform(0, 10), 2),
                "water_level": round(random.uniform(0, 2), 2),
                "seismic": round(random.uniform(0, 2), 2),
                "wind_speed": round(random.uniform(30, 80), 2)
            },
            # Flood scenario - high rainfall and water level
            {
                "temperature": round(random.uniform(20, 30), 2),
                "humidity": round(random.uniform(80, 95), 2),
                "rainfall": round(random.uniform(120, 200), 2),
                "water_level": round(random.uniform(8, 15), 2),
                "seismic": round(random.uniform(0, 3), 2),
                "wind_speed": round(random.uniform(40, 100), 2)
            },
            # Earthquake scenario - high seismic
            {
                "temperature": round(random.uniform(20, 35), 2),
                "humidity": round(random.uniform(40, 70), 2),
                "rainfall": round(random.uniform(0, 50), 2),
                "water_level": round(random.uniform(2, 5), 2),
                "seismic": round(random.uniform(6, 8), 2),
                "wind_speed": round(random.uniform(20, 60), 2)
            },
            # Storm scenario - high wind and humidity
            {
                "temperature": round(random.uniform(25, 35), 2),
                "humidity": round(random.uniform(85, 100), 2),
                "rainfall": round(random.uniform(80, 150), 2),
                "water_level": round(random.uniform(3, 8), 2),
                "seismic": round(random.uniform(0, 3), 2),
                "wind_speed": round(random.uniform(100, 150), 2)
            },
            # Multiple disaster scenario - multiple high values
            {
                "temperature": round(random.uniform(50, 75), 2),
                "humidity": round(random.uniform(85, 95), 2),
                "rainfall": round(random.uniform(120, 180), 2),
                "water_level": round(random.uniform(8, 12), 2),
                "seismic": round(random.uniform(5, 7), 2),
                "wind_speed": round(random.uniform(90, 130), 2)
            }
        ]
        
        # Select random scenario
        scenario = random.choice(scenarios)
        
        # Apply intensity multiplier
        if self.alert_intensity > 1.0:
            scenario["temperature"] = min(80, scenario["temperature"] * self.alert_intensity)
            scenario["rainfall"] = min(200, scenario["rainfall"] * self.alert_intensity)
            scenario["water_level"] = min(15, scenario["water_level"] * self.alert_intensity)
            scenario["seismic"] = min(8, scenario["seismic"] * self.alert_intensity)
            scenario["wind_speed"] = min(150, scenario["wind_speed"] * self.alert_intensity)
        
        return {
            "device_id": "disaster-sensor-001",
            "timestamp": datetime.utcnow().isoformat(),
            "temperature": round(scenario["temperature"], 2),
            "humidity": round(scenario["humidity"], 2),
            "rainfall": round(scenario["rainfall"], 2),
            "water_level": round(scenario["water_level"], 2),
            "seismic": round(scenario["seismic"], 2),
            "wind_speed": round(scenario["wind_speed"], 2)
        }
    
    def should_generate_alert(self):
        """
        Determine if alert should be generated based on random intervals
        Returns: (generate_alert, duration, intensity)
        """
        if not hasattr(self, 'next_alert_time'):
            # Initialize with random time between 30-90 seconds
            self.next_alert_time = time.time() + random.uniform(30, 90)
            self.alert_active = False
            self.alert_end_time = 0
        
        current_time = time.time()
        
        # Check if currently in alert mode
        if self.alert_active:
            if current_time >= self.alert_end_time:
                # Alert period ended
                self.alert_active = False
                self.next_alert_time = current_time + random.uniform(30, 120)  # Wait 30-120 seconds before next alert
                print(f"Alert period ended. Next alert in {self.next_alert_time - current_time:.1f} seconds")
                return False, 0, 1.0
            else:
                # Continue alert
                remaining = self.alert_end_time - current_time
                return True, remaining, self.alert_intensity
        
        # Check if it's time for new alert
        if current_time >= self.next_alert_time:
            # Start new alert period
            self.alert_active = True
            # Random alert duration: 15-45 seconds (3-9 readings)
            alert_duration = random.uniform(15, 45)
            self.alert_end_time = current_time + alert_duration
            # Random intensity: 1.0 to 2.5
            self.alert_intensity = random.uniform(1.0, 2.5)
            
            print(f"\n" + "="*50)
            print(f"ALERT TRIGGERED!")
            print(f"Duration: {alert_duration:.1f} seconds")
            print(f"Intensity: {self.alert_intensity:.1f}x")
            print(f"="*50 + "\n")
            
            return True, alert_duration, self.alert_intensity
        
        return False, 0, 1.0
    
    def generate_sensor_data(self):
        """Generate sensor data with random alert intervals"""
        should_alert, duration, intensity = self.should_generate_alert()
        
        if should_alert:
            self.alert_intensity = intensity
            data = self.generate_alert_data()
            data['alert_status'] = 'ACTIVE'
            data['alert_intensity'] = round(intensity, 2)
            data['alert_remaining'] = round(duration, 1)
        else:
            data = self.generate_normal_data()
            data['alert_status'] = 'NORMAL'
        
        return data
    
    def connect_mqtt(self):
        """Connect to AWS IoT Core"""
        try:
            self.mqtt_client = mqtt.Client(client_id=self.CLIENT_ID)
            self.mqtt_client.tls_set(self.CA_PATH, certfile=self.CERT_PATH, 
                                      keyfile=self.KEY_PATH, tls_version=ssl.PROTOCOL_TLSv1_2)
            self.mqtt_client.connect(self.ENDPOINT, self.PORT)
            self.mqtt_client.loop_start()
            print("Connected to AWS IoT Core")
            return True
        except Exception as e:
            print(f"Failed to connect to AWS IoT Core: {str(e)}")
            return False
    
    def publish_data(self):
        """Publish sensor data to MQTT with random intervals"""
        publish_count = 0
        alert_count = 0
        
        print("Publishing thread started")
        
        while self.is_running and not self.stop_thread:
            try:
                data = self.generate_sensor_data()
                publish_count += 1
                
                if data.get('alert_status') == 'ACTIVE':
                    alert_count += 1
                    print(f"[{publish_count}] ALERT DATA - Intensity: {data.get('alert_intensity')}x - Temp: {data['temperature']}C, Rainfall: {data['rainfall']}mm, Seismic: {data['seismic']}")
                else:
                    print(f"[{publish_count}] Normal data - Temp: {data['temperature']}C, Humidity: {data['humidity']}%")
                
                # Publish to MQTT
                self.mqtt_client.publish(self.TOPIC, json.dumps(data), qos=1)
                
                # Random interval between 3-8 seconds
                interval = random.uniform(3, 8)
                time.sleep(interval)
                
            except Exception as e:
                print(f"Error publishing data: {str(e)}")
                time.sleep(7)
        
        print(f"Publishing thread stopped. Total: {publish_count}, Alerts: {alert_count}")
    
    def start_simulation(self):
        """Start sensor data generation"""
        if self.is_running:
            return {"status": "error", "message": "Simulation already running"}
        
        try:
            self.stop_thread = False
            
            # Reset alert timing
            self.next_alert_time = time.time() + random.uniform(30, 90)
            self.alert_active = False
            
            if not self.connect_mqtt():
                return {"status": "error", "message": "Failed to connect to AWS IoT Core"}
            
            self.is_running = True
            self.thread = threading.Thread(target=self.publish_data)
            self.thread.daemon = True
            self.thread.start()
            
            print("Sensor simulation started - Alert mode enabled")
            print(f"First alert expected in {self.next_alert_time - time.time():.1f} seconds")
            return {"status": "success", "message": "Sensor simulation started"}
            
        except Exception as e:
            self.is_running = False
            print(f"Failed to start simulation: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def stop_simulation(self):
        """Stop sensor data generation"""
        if not self.is_running:
            return {"status": "error", "message": "Simulation not running"}
        
        print("Stopping sensor simulation...")
        
        self.is_running = False
        self.stop_thread = True
        
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        
        if self.mqtt_client:
            try:
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
                print("MQTT client disconnected")
            except Exception as e:
                print(f"Error disconnecting MQTT: {str(e)}")
        
        self.mqtt_client = None
        self.thread = None
        
        print("Sensor simulation stopped")
        return {"status": "success", "message": "Sensor simulation stopped"}
    
    def delete_all_records(self):
        """Delete all records from DynamoDB via Lambda-2 API Gateway"""
        try:
            print("Deleting all records from DynamoDB...")
            
            response = requests.delete(self.LAMBDA_2_URL, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                print(result.get('message', 'Records deleted successfully'))
                return {"status": "success", "message": result.get('message', 'All records deleted')}
            else:
                error_msg = f"Delete failed with status {response.status_code}: {response.text}"
                print(error_msg)
                return {"status": "error", "message": error_msg}
                
        except requests.exceptions.RequestException as e:
            error_msg = f"Network error: {str(e)}"
            print(error_msg)
            return {"status": "error", "message": error_msg}
        except Exception as e:
            error_msg = f"Error deleting records: {str(e)}"
            print(error_msg)
            return {"status": "error", "message": error_msg}
    
    def get_status(self):
        """Get current simulation status"""
        status_msg = "Running" if self.is_running else "Stopped"
        if self.is_running and hasattr(self, 'alert_active'):
            if self.alert_active:
                remaining = max(0, self.alert_end_time - time.time())
                status_msg = f"Running - ALERT ACTIVE ({remaining:.0f}s remaining)"
            else:
                next_alert = max(0, self.next_alert_time - time.time())
                status_msg = f"Running - Normal (next alert in {next_alert:.0f}s)"
        
        return {
            "is_running": self.is_running,
            "message": status_msg,
            "alert_active": self.alert_active if hasattr(self, 'alert_active') else False
        }

# Create global instance
sensor_simulator = DisasterSensorSimulator()

# Functions to be called from Flask
def start_simulation():
    """Start sensor simulation"""
    return sensor_simulator.start_simulation()

def stop_simulation():
    """Stop sensor simulation"""
    return sensor_simulator.stop_simulation()

def delete_all_records():
    """Delete all records"""
    return sensor_simulator.delete_all_records()

def get_simulation_status():
    """Get simulation status"""
    return sensor_simulator.get_status()