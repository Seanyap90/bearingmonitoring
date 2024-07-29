# mqtt client to manage incoming data, send to ML server for predictions and
# publish predictions back to broker

import paho.mqtt.client as mqtt
import requests
import json
import redis
from time import sleep

# MQTT configuration
MQTT_BROKER = 'localhost'  # Replace with your MQTT broker
LISTEN_TOPIC = 'bearing/sendData'
PUBLISH_TOPIC = 'bearing/label'

# Initialize Redis client
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Redis key for the model settings
REDIS_SETTINGS_KEY = 'config:settings'

def get_flask_server_url():
    # Fetch the configuration from Redis
    config_data = redis_client.get(REDIS_SETTINGS_KEY)
    
    if not config_data:
        raise ValueError("No configuration found in Redis")

    # Parse the JSON configuration
    config = json.loads(config_data)
    
    # Initialize variables to store the latest model and version
    latest_model_name = None
    latest_version = -1

    # Iterate through the models to find the one with the highest version
    for model_name, model_entries in config['MODELS'].items():
        for entry in model_entries:
            version = entry.get('version', -1)
            if version > latest_version:
                latest_version = version
                latest_model_name = entry['model_subdirectory']

    if latest_model_name is None:
        raise ValueError("No valid model settings found in Redis")

    url = f'http://localhost:5000/api/{latest_model_name}/{latest_version}/predict'
    return url, latest_model_name, latest_version

# Callback when the client receives a CONNACK response from the server
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe(LISTEN_TOPIC)
    if rc != 0:
        print(f"Failed to connect, return code {rc}")
        
def on_disconnect(client, userdata, rc):
    print(f"Disconnected with result code {rc}")
    if rc != 0:
        print("Unexpected disconnection. Reconnecting...")
        client.reconnect()

# Callback when a PUBLISH message is received from the server
def on_message(client, userdata, msg):
    print(f"Message received on topic {msg.topic}")
    data = json.loads(msg.payload.decode())
    first_row = data[0]
    directory = first_row.get("Directory")
    h = first_row.get("h")
    m = first_row.get("m")
    s = first_row.get("s")
    time_str = f"{h:02}:{m:02}:{s:02}"
    print(time_str + ": " + directory)
    
    try:
        # Fetch the latest Flask server URL
        flask_server_url, model_name, version = get_flask_server_url()
        
        # Print the model name and version
        print(f"Using model: {model_name}, version: {version}")
        
        # The payload is already a JSON string, so we can send it directly
        payload = msg.payload.decode()
        response = requests.post(flask_server_url, data=payload, headers={'Content-Type': 'application/json'})
        response_data = response.json()
        client.publish(PUBLISH_TOPIC, json.dumps(response_data))
        print(f"Published response to topic {PUBLISH_TOPIC}: {response_data}")
    except Exception as e:
        print(f"Error processing message: {e}")

# Create an MQTT client instance
client = mqtt.Client()
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

# Connect to the MQTT broker
try:
    client.connect(MQTT_BROKER, 1883, 60)
except Exception as e:
    print(f"Failed to connect to MQTT broker: {e}")

# Start the MQTT client loop
while True:
    client.loop_start()
    sleep(2)
    client.loop_stop()
