


from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import requests
import json
import logging
import boto3
from datetime import datetime, timedelta
import sys
import os
from collections import defaultdict
import time
import threading

# Add sensor simulator to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from sensor_simulator import start_simulation, stop_simulation, delete_all_records as delete_all_db_records, get_simulation_status

app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# API Gateway endpoints for Lambdas
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

# Track sent alerts to avoid duplicates
sent_alerts = set()

# Threshold values for each sensor type
THRESHOLDS = {
    'temperature': 45,
    'rainfall': 100,
    'water_level': 8,
    'seismic': 5,
    'humidity': 85,
    'wind_speed': 80
}

def safe_float(value, default=0.0):
    """Safely convert any value to float"""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    return default

def safe_int(value, default=0):
    """Safely convert any value to int"""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return default
    return default

def check_consecutive_threshold(sensor_type, current_value, threshold, timestamp, record_id):
    """
    Check if threshold exceeded for 3 consecutive readings
    """
    global last_readings, sent_alerts
    
    # Convert to float safely
    current_value = safe_float(current_value)
    threshold = safe_float(threshold)
    
    # Create unique key for this alert sequence
    alert_key = f"{sensor_type}_{record_id}"
    
    # Check if we already sent alert for this sequence
    if alert_key in sent_alerts:
        return False, None
    
    # Add current reading to list
    last_readings[sensor_type].append({
        'value': current_value,
        'timestamp': timestamp,
        'exceeded': current_value > threshold
    })
    
    # Keep only last 3 readings
    if len(last_readings[sensor_type]) > 3:
        last_readings[sensor_type].pop(0)
    
    # Check if we have 3 readings and all exceeded threshold
    if len(last_readings[sensor_type]) >= 3:
        all_exceeded = all(r['exceeded'] for r in last_readings[sensor_type])
        
        if all_exceeded:
            # Clear the readings for this sensor to avoid duplicate alerts
            last_readings[sensor_type] = []
            sent_alerts.add(alert_key)
            alert_message = f"ALERT: {sensor_type} exceeded threshold {threshold} for 3 consecutive readings. Current value: {current_value}"
            return True, alert_message
    
    return False, None

def check_disaster_alerts(sensor_data, record_id):
    """
    Check for alerts based on 3 consecutive readings
    Returns: list of alert messages
    """
    alerts = []
    timestamp = sensor_data.get('timestamp', datetime.now().isoformat())
    
    # Check temperature - convert to float
    temp = safe_float(sensor_data.get('temperature', 0))
    if temp > THRESHOLDS['temperature']:
        should_alert, alert_msg = check_consecutive_threshold('temperature', temp, THRESHOLDS['temperature'], timestamp, record_id)
        if should_alert:
            alerts.append(alert_msg)
            logger.info(f"Temperature alert triggered after 3 consecutive high readings: {temp}C")
    
    # Check rainfall
    rainfall = safe_float(sensor_data.get('rainfall', 0))
    if rainfall > THRESHOLDS['rainfall']:
        should_alert, alert_msg = check_consecutive_threshold('rainfall', rainfall, THRESHOLDS['rainfall'], timestamp, record_id)
        if should_alert:
            alerts.append(alert_msg)
            logger.info(f"Rainfall alert triggered after 3 consecutive high readings: {rainfall}mm")
    
    # Check water level
    water_level = safe_float(sensor_data.get('water_level', 0))
    if water_level > THRESHOLDS['water_level']:
        should_alert, alert_msg = check_consecutive_threshold('water_level', water_level, THRESHOLDS['water_level'], timestamp, record_id)
        if should_alert:
            alerts.append(alert_msg)
            logger.info(f"Water level alert triggered after 3 consecutive high readings: {water_level}m")
    
    # Check seismic
    seismic = safe_float(sensor_data.get('seismic', 0))
    if seismic > THRESHOLDS['seismic']:
        should_alert, alert_msg = check_consecutive_threshold('seismic', seismic, THRESHOLDS['seismic'], timestamp, record_id)
        if should_alert:
            alerts.append(alert_msg)
            logger.info(f"Seismic alert triggered after 3 consecutive high readings: {seismic}")
    
    # Check humidity
    humidity = safe_float(sensor_data.get('humidity', 0))
    if humidity > THRESHOLDS['humidity']:
        should_alert, alert_msg = check_consecutive_threshold('humidity', humidity, THRESHOLDS['humidity'], timestamp, record_id)
        if should_alert:
            alerts.append(alert_msg)
            logger.info(f"Humidity alert triggered after 3 consecutive high readings: {humidity}%")
    
    # Check wind speed
    wind_speed = safe_float(sensor_data.get('wind_speed', 0))
    if wind_speed > THRESHOLDS['wind_speed']:
        should_alert, alert_msg = check_consecutive_threshold('wind_speed', wind_speed, THRESHOLDS['wind_speed'], timestamp, record_id)
        if should_alert:
            alerts.append(alert_msg)
            logger.info(f"Wind speed alert triggered after 3 consecutive high readings: {wind_speed}km/h")
    
    return alerts

def send_sns_alert(alert_message, sensor_data):
    """Send SNS alert via Lambda-3"""
    try:
        # Clean and convert all values to proper types
        clean_data = {}
        for key, value in sensor_data.items():
            if key in ['temperature', 'humidity', 'rainfall', 'water_level', 'seismic', 'wind_speed']:
                clean_data[key] = safe_float(value)
            elif key in ['alert_flag', 'motion_detected']:
                clean_data[key] = safe_int(value)
            else:
                clean_data[key] = value
        
        payload = {
            "alert_type": "DISASTER_WARNING",
            "severity": "HIGH",
            "alert_data": clean_data,
            "alert_message": alert_message
        }
        
        logger.info(f"Sending SNS alert to Lambda-3")
        logger.info(f"Alert message: {alert_message[:100]}...")
        
        response = requests.post(
            LAMBDA_3_API_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            logger.info(f"SNS ALERT SENT SUCCESSFULLY")
            return True
        else:
            logger.error(f"Failed to send SNS alert. Status: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error sending SNS alert: {str(e)}")
        return False

def fetch_and_check_alerts():
    """Fetch data from DynamoDB and check for alerts"""
    try:
        response = requests.get(f"{LAMBDA_2_API_URL}?action=fetch", timeout=10)
        if response.status_code == 200:
            data = response.json()
            data.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            logger.info(f"Fetched {len(data)} records from DynamoDB")
            
            # Log first record to debug
            if data:
                logger.info(f"Sample record keys: {list(data[0].keys())}")
                logger.info(f"Sample temperature value: {data[0].get('temperature')} (type: {type(data[0].get('temperature'))})")
            
            alerts_triggered = []
            
            # Check each record for alerts (from newest to oldest)
            for record in data:
                record_id = record.get('id', record.get('timestamp', str(time.time())))
                alerts = check_disaster_alerts(record, record_id)
                
                if alerts:
                    for alert in alerts:
                        # logger.info(f"ALERT DETECTED: {alert}")
                        # Send SNS email
                        if send_sns_alert(alert, record):
                            alerts_triggered.append(alert)
                            # logger.info(f"Email sent for alert: {alert[:50]}...")
                            print(f"Email sent successfully for alert : {alert[:50]} ")
            
            return alerts_triggered
        else:
            logger.error(f"Error fetching data: {response.status_code}")
            return []
    except Exception as e:
        logger.error(f"Exception in fetch_and_check_alerts: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return []

def fetch_sensor_data():
    """Fetch data from DynamoDB via Lambda-2"""
    try:
        response = requests.get(f"{LAMBDA_2_API_URL}?action=fetch", timeout=10)
        if response.status_code == 200:
            data = response.json()
            data.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            # Convert string values to numbers for display
            for record in data:
                for key in ['temperature', 'humidity', 'rainfall', 'water_level', 'seismic', 'wind_speed']:
                    if key in record:
                        record[key] = safe_float(record[key])
            return data
        else:
            logger.error(f"Error fetching data: {response.status_code}")
            return []
    except Exception as e:
        logger.error(f"Exception in fetch_sensor_data: {str(e)}")
        return []

# Background thread to check alerts periodically
def alert_checker_thread():
    """Background thread that checks for alerts every 10 seconds"""
    while True:
        try:
            # logger.info("Running periodic alert check...")
            alerts = fetch_and_check_alerts()
            if alerts:
                logger.info(f"Found {len(alerts)} alert(s) and sent emails")
            time.sleep(10)  # Check every 10 seconds
        except Exception as e:
            logger.error(f"Error in alert checker thread: {str(e)}")
            time.sleep(10)

# Start background thread
def start_alert_checker():
    thread = threading.Thread(target=alert_checker_thread, daemon=True)
    thread.start()
    logger.info("Alert checker thread started")

@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/data')
def get_data():
    """Get sensor data"""
    data = fetch_sensor_data()
    return jsonify(data)

@app.route('/api/latest')
def get_latest():
    """Get latest sensor reading"""
    data = fetch_sensor_data()
    if data:
        return jsonify(data[0])
    return jsonify({})

@app.route('/api/check-alerts', methods=['POST'])
def check_alerts():
    """Manually trigger alert check"""
    alerts = fetch_and_check_alerts()
    return jsonify({
        "status": "completed",
        "alerts_triggered": alerts,
        "alert_count": len(alerts)
    })

@app.route('/api/alerts')
def get_alerts():
    """Get all alert records"""
    data = fetch_sensor_data()
    return jsonify(data)

@app.route('/api/alert-config')
def get_alert_config():
    """Get current alert configuration"""
    return jsonify({
        'thresholds': THRESHOLDS,
        'required_consecutive_readings': 3,
        'current_readings': last_readings,
        'sent_alerts_count': len(sent_alerts)
    })

@app.route('/api/reset-alerts', methods=['POST'])
def reset_alerts():
    """Reset all alert tracking"""
    global last_readings, sent_alerts
    last_readings = {
        'temperature': [],
        'rainfall': [],
        'water_level': [],
        'seismic': [],
        'humidity': [],
        'wind_speed': []
    }
    sent_alerts.clear()
    logger.info("Alert tracking reset")
    return jsonify({"status": "success", "message": "Alert tracking reset"})

@app.route('/api/start-simulation', methods=['POST'])
def start_sensor_simulation():
    """Start sensor data generation"""
    result = start_simulation()
    return jsonify(result)

@app.route('/api/stop-simulation', methods=['POST'])
def stop_sensor_simulation():
    """Stop sensor data generation"""
    result = stop_simulation()
    return jsonify(result)

@app.route('/api/delete-records', methods=['DELETE'])
def delete_all_records():
    """Delete all records from DynamoDB"""
    result = delete_all_db_records()
    if result.get('status') == 'success':
        # Reset tracking when records are deleted
        global last_readings, sent_alerts
        last_readings = {
            'temperature': [],
            'rainfall': [],
            'water_level': [],
            'seismic': [],
            'humidity': [],
            'wind_speed': []
        }
        sent_alerts.clear()
    return jsonify(result)

@app.route('/api/simulation-status')
def simulation_status():
    """Get simulation status"""
    result = get_simulation_status()
    return jsonify(result)

@app.route('/api/test-sns', methods=['POST'])
def test_sns():
    """Test endpoint to verify SNS is working"""
    try:
        test_payload = {
            "alert_type": "TEST_ALERT",
            "severity": "TEST",
            "alert_data": {
                "temperature": 99,
                "humidity": 99,
                "rainfall": 999,
                "water_level": 99,
                "seismic": 9,
                "wind_speed": 999,
                "timestamp": datetime.now().isoformat(),
                "device_id": "test-device"
            },
            "alert_message": "This is a test alert to verify SNS configuration"
        }
        
        response = requests.post(
            LAMBDA_3_API_URL,
            json=test_payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        return jsonify({
            "status": "test completed",
            "lambda_3_response_status": response.status_code,
            "lambda_3_response_body": response.text
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/stats')
def get_stats():
    """Get statistics"""
    data = fetch_sensor_data()
    
    if not data:
        return jsonify({})
    
    stats = {
        'total_readings': len(data),
        'latest_temperature': safe_float(data[0].get('temperature', 0)),
        'latest_humidity': safe_float(data[0].get('humidity', 0)),
        'latest_rainfall': safe_float(data[0].get('rainfall', 0)),
        'latest_water_level': safe_float(data[0].get('water_level', 0)),
        'latest_seismic': safe_float(data[0].get('seismic', 0)),
        'latest_wind_speed': safe_float(data[0].get('wind_speed', 0))
    }
    
    return jsonify(stats)

if __name__ == '__main__':
    # Start the background alert checker
    start_alert_checker()
    app.run(debug=True, host='0.0.0.0', port=5000)