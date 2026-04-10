

from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import requests
import json
import logging
import sys
import os
import time
import threading
from datetime import datetime
from collections import defaultdict

# Add sensor simulator to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from sensor_simulator import start_simulation, stop_simulation, delete_all_records as delete_all_db_records, get_simulation_status

application = Flask(__name__)

app = application

CORS(app)

# Configure minimal logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

# API Gateway endpoints
LAMBDA_2_API_URL = "https://6xhiwcxx5j.execute-api.us-east-1.amazonaws.com/default/x24274429-lambda-database-fog"
LAMBDA_3_API_URL = "https://j3l83s8bj0.execute-api.us-east-1.amazonaws.com/default/x24274429-lambda-sns"

# Store last 3 readings for each sensor type
last_readings = {
    'temperature': [],
    'rainfall': [],
    'water_level': [],
    'seismic': [],
    'humidity': [],
    'wind_speed': []
}

# Track sent alerts with timestamp to prevent duplicate emails
sent_alerts = {}  # {alert_key: timestamp}
ALERT_COOLDOWN_SECONDS = 3600  # 1 hour cooldown for same alert type

# Threshold values
THRESHOLDS = {
    'temperature': 45,
    'rainfall': 100,
    'water_level': 8,
    'seismic': 5,
    'humidity': 85,
    'wind_speed': 80
}

# Track last processed record to avoid re-processing
last_processed_timestamp = None

def safe_float(value, default=0.0):
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except:
            return default
    return default

def check_consecutive_threshold(sensor_type, current_value, threshold, record_id):
    global last_readings, sent_alerts
    
    current_value = safe_float(current_value)
    
    # Check if alert is in cooldown
    if sensor_type in sent_alerts:
        time_since_last = time.time() - sent_alerts[sensor_type]
        if time_since_last < ALERT_COOLDOWN_SECONDS:
            return False, None
    
    # Add current reading to list
    last_readings[sensor_type].append({
        'value': current_value,
        'exceeded': current_value > threshold
    })
    
    # Keep only last 3 readings
    if len(last_readings[sensor_type]) > 3:
        last_readings[sensor_type].pop(0)
    
    # Check if we have 3 readings and all exceeded threshold
    if len(last_readings[sensor_type]) >= 3:
        all_exceeded = all(r['exceeded'] for r in last_readings[sensor_type])
        
        if all_exceeded:
            # Clear the readings for this sensor
            last_readings[sensor_type] = []
            # Record alert sent time
            sent_alerts[sensor_type] = time.time()
            alert_message = f"ALERT: {sensor_type} exceeded {threshold} for 3 consecutive readings. Current: {current_value}"
            return True, alert_message
    
    return False, None

def check_disaster_alerts(sensor_data):
    alerts = []
    
    temp = safe_float(sensor_data.get('temperature', 0))
    if temp > THRESHOLDS['temperature']:
        should_alert, msg = check_consecutive_threshold('temperature', temp, THRESHOLDS['temperature'], None)
        if should_alert:
            alerts.append(msg)
            print(f"[ALERT] Temperature: {temp}C (3 consecutive high readings)")
    
    rainfall = safe_float(sensor_data.get('rainfall', 0))
    if rainfall > THRESHOLDS['rainfall']:
        should_alert, msg = check_consecutive_threshold('rainfall', rainfall, THRESHOLDS['rainfall'], None)
        if should_alert:
            alerts.append(msg)
            print(f"[ALERT] Rainfall: {rainfall}mm (3 consecutive high readings)")
    
    water_level = safe_float(sensor_data.get('water_level', 0))
    if water_level > THRESHOLDS['water_level']:
        should_alert, msg = check_consecutive_threshold('water_level', water_level, THRESHOLDS['water_level'], None)
        if should_alert:
            alerts.append(msg)
            print(f"[ALERT] Water Level: {water_level}m (3 consecutive high readings)")
    
    seismic = safe_float(sensor_data.get('seismic', 0))
    if seismic > THRESHOLDS['seismic']:
        should_alert, msg = check_consecutive_threshold('seismic', seismic, THRESHOLDS['seismic'], None)
        if should_alert:
            alerts.append(msg)
            print(f"[ALERT] Seismic: {seismic} (3 consecutive high readings)")
    
    humidity = safe_float(sensor_data.get('humidity', 0))
    if humidity > THRESHOLDS['humidity']:
        should_alert, msg = check_consecutive_threshold('humidity', humidity, THRESHOLDS['humidity'], None)
        if should_alert:
            alerts.append(msg)
            print(f"[ALERT] Humidity: {humidity}% (3 consecutive high readings)")
    
    wind_speed = safe_float(sensor_data.get('wind_speed', 0))
    if wind_speed > THRESHOLDS['wind_speed']:
        should_alert, msg = check_consecutive_threshold('wind_speed', wind_speed, THRESHOLDS['wind_speed'], None)
        if should_alert:
            alerts.append(msg)
            print(f"[ALERT] Wind Speed: {wind_speed}km/h (3 consecutive high readings)")
    
    return alerts

def send_sns_alert(alert_message, sensor_data):
    try:
        clean_data = {}
        for key, value in sensor_data.items():
            if key in ['temperature', 'humidity', 'rainfall', 'water_level', 'seismic', 'wind_speed']:
                clean_data[key] = safe_float(value)
            else:
                clean_data[key] = value
        
        payload = {
            "alert_type": "DISASTER_WARNING",
            "severity": "HIGH",
            "alert_data": clean_data,
            "alert_message": alert_message
        }
        
        response = requests.post(LAMBDA_3_API_URL, json=payload, headers={"Content-Type": "application/json"}, timeout=10)
        
        if response.status_code == 200:
            print(f"[EMAIL SENT] {alert_message[:80]}...")
            return True
        else:
            print(f"[EMAIL FAILED] Status: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"[EMAIL ERROR] {str(e)}")
        return False

def fetch_and_check_alerts():
    global last_processed_timestamp
    
    try:
        response = requests.get(f"{LAMBDA_2_API_URL}?action=fetch", timeout=10)
        if response.status_code == 200:
            data = response.json()
            data.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            
            if not data:
                return []
            
            # Get the latest record only (avoid processing multiple records)
            latest_record = data[0]
            record_timestamp = latest_record.get('timestamp', '')
            
            # Skip if we already processed this timestamp
            if record_timestamp == last_processed_timestamp:
                return []
            
            last_processed_timestamp = record_timestamp
            
            # Check alerts only for the latest record
            alerts = check_disaster_alerts(latest_record)
            
            alerts_sent = []
            for alert in alerts:
                if send_sns_alert(alert, latest_record):
                    alerts_sent.append(alert)
            
            if alerts_sent:
                print(f"[SUMMARY] {len(alerts_sent)} alert(s) sent for record at {record_timestamp}")
            
            return alerts_sent
        return []
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        return []

def fetch_sensor_data():
    try:
        response = requests.get(f"{LAMBDA_2_API_URL}?action=fetch", timeout=10)
        if response.status_code == 200:
            data = response.json()
            data.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            for record in data:
                for key in ['temperature', 'humidity', 'rainfall', 'water_level', 'seismic', 'wind_speed']:
                    if key in record:
                        record[key] = safe_float(record[key])
            return data
        return []
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        return []

def alert_checker_thread():
    while True:
        try:
            alerts = fetch_and_check_alerts()
            time.sleep(10)  # Check every 10 seconds
        except Exception as e:
            print(f"[THREAD ERROR] {str(e)}")
            time.sleep(10)

def start_alert_checker():
    thread = threading.Thread(target=alert_checker_thread, daemon=True)
    thread.start()
    print("[STARTED] Alert checker running (checks every 10 seconds)")

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/api/data')
def get_data():
    return jsonify(fetch_sensor_data())

@app.route('/api/latest')
def get_latest():
    data = fetch_sensor_data()
    return jsonify(data[0] if data else {})

@app.route('/api/check-alerts', methods=['POST'])
def check_alerts():
    alerts = fetch_and_check_alerts()
    return jsonify({"status": "completed", "alerts_sent": len(alerts)})

@app.route('/api/alerts')
def get_alerts():
    return jsonify(fetch_sensor_data())

@app.route('/api/alert-config')
def get_alert_config():
    return jsonify({
        'thresholds': THRESHOLDS,
        'required_consecutive': 3,
        'cooldown_seconds': ALERT_COOLDOWN_SECONDS,
        'active_cooldowns': {k: int(time.time() - v) for k, v in sent_alerts.items() if time.time() - v < ALERT_COOLDOWN_SECONDS}
    })

@app.route('/api/reset-alerts', methods=['POST'])
def reset_alerts():
    global last_readings, sent_alerts, last_processed_timestamp
    last_readings = {k: [] for k in last_readings}
    sent_alerts.clear()
    last_processed_timestamp = None
    print("[RESET] Alert tracking reset")
    return jsonify({"status": "success"})

@app.route('/api/start-simulation', methods=['POST'])
def start_sensor_simulation():
    result = start_simulation()
    print(f"[SIMULATION] Started" if result.get('status') == 'success' else f"[SIMULATION] Failed: {result.get('message')}")
    return jsonify(result)

@app.route('/api/stop-simulation', methods=['POST'])
def stop_sensor_simulation():
    result = stop_simulation()
    print(f"[SIMULATION] Stopped" if result.get('status') == 'success' else f"[SIMULATION] Failed: {result.get('message')}")
    return jsonify(result)

@app.route('/api/delete-records', methods=['DELETE'])
def delete_all_records():
    result = delete_all_db_records()
    if result.get('status') == 'success':
        global last_readings, sent_alerts, last_processed_timestamp
        last_readings = {k: [] for k in last_readings}
        sent_alerts.clear()
        last_processed_timestamp = None
        print("[DATABASE] All records deleted")
    return jsonify(result)

@app.route('/api/simulation-status')
def simulation_status():
    return jsonify(get_simulation_status())

@app.route('/api/test-sns', methods=['POST'])
def test_sns():
    try:
        test_payload = {
            "alert_type": "TEST",
            "severity": "TEST",
            "alert_data": {"temperature": 99, "timestamp": datetime.now().isoformat()},
            "alert_message": "Test SNS email"
        }
        response = requests.post(LAMBDA_3_API_URL, json=test_payload, timeout=10)
        print(f"[TEST] SNS test result: {response.status_code}")
        return jsonify({"status": "test completed", "response": response.status_code})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/stats')
def get_stats():
    data = fetch_sensor_data()
    if not data:
        return jsonify({})
    
    return jsonify({
        'total_readings': len(data),
        'latest_temperature': safe_float(data[0].get('temperature', 0)),
        'latest_humidity': safe_float(data[0].get('humidity', 0)),
        'latest_rainfall': safe_float(data[0].get('rainfall', 0)),
        'latest_water_level': safe_float(data[0].get('water_level', 0)),
        'latest_seismic': safe_float(data[0].get('seismic', 0)),
        'latest_wind_speed': safe_float(data[0].get('wind_speed', 0))
    })

if __name__ == '__main__':
    start_alert_checker()
    print("[STARTUP] Disaster Early Warning System Running on port 5000")
    app.run(debug=False, host='0.0.0.0', port=5000)