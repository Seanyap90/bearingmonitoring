import paho.mqtt.client as mqtt
import requests
import json
import redis
from time import sleep

# Configuration
REDIS_HOST = 'YOUR REDIS HOST'
REDIS_PORT = 6379
MQTT_BROKER = 'YOUR MQTT BROKER URL'  # Replace with your MQTT broker host
MQTT_PORT = 1883
LISTEN_TOPIC = 'bearing/label'
ALERT_TOPIC = 'bearing/alert'
OBSERVE_CONSECUTIVE_LABEL_2 = 3  # Number of consecutive entries with label: 2 to trigger action
OBSERVE_CONSECUTIVE_LABEL_1 = 3  # Number of consecutive entries with label: 1 to trigger action
CLEAR_CONSECUTIVE = 12  # Number of total entries to trigger clearing

# Initialize Redis client
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

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

def check_consecutive_entries(key, label, count):
    entries = redis_client.lrange(key, 0, count - 1)
    return all(json.loads(entry).get("label") == label for entry in entries)

def on_message(client, userdata, msg):
    print(f"Message received on topic {msg.topic}")
    
    try:
        # Decode the message payload
        data = json.loads(msg.payload.decode())
        
        # Extract the required fields
        bearing = data.get("bearing")
        label = data.get("label")
        time = data.get("time")

        if bearing and label and time:
            # Store the data in Redis
            key = f"{bearing}:labels"
            value = {
                "label": label,
                "time": time
            }
            
            # Check if the key exists and is not a list, then delete it
            if redis_client.exists(key) and not redis_client.type(key) == 'list':
                redis_client.delete(key)
                print(f"Deleted key {key} as it was not a list")
            
            # Append the new value to the list
            redis_client.lpush(key, json.dumps(value))
            print(f"Stored in Redis: {key} -> {value}")
            
            # Get the length of the list
            list_length = redis_client.llen(key)

            # Check if observe consecutive entries with label: 2 threshold is reached
            if list_length >= OBSERVE_CONSECUTIVE_LABEL_2 and check_consecutive_entries(key, "2", OBSERVE_CONSECUTIVE_LABEL_2):
                print(f"{bearing} has {OBSERVE_CONSECUTIVE_LABEL_2} consecutive entries with label 2.")
                latest_entry = json.loads(redis_client.lindex(key, 0))
                alert_message = {
                    "bearing_number": bearing,
                    "time": latest_entry.get("time"),
                    "alert_status": "warning"
                }
                client.publish(ALERT_TOPIC, json.dumps(alert_message))
                print(f"Published alert: {alert_message}")
            
            # Check if observe consecutive entries with label: 1 threshold is reached
            if list_length >= OBSERVE_CONSECUTIVE_LABEL_1 and check_consecutive_entries(key, "1", OBSERVE_CONSECUTIVE_LABEL_1):
                print(f"{bearing} has {OBSERVE_CONSECUTIVE_LABEL_1} consecutive entries with label 1.")
                latest_entry = json.loads(redis_client.lindex(key, 0))
                alert_message = {
                    "bearing_number": bearing,
                    "time": latest_entry.get("time"),
                    "alert_status": "verify"
                }
                client.publish(ALERT_TOPIC, json.dumps(alert_message))
                print(f"Published alert: {alert_message}")

            # Check if total entries threshold is reached
            if list_length >= CLEAR_CONSECUTIVE:
                print(f"{bearing} has {CLEAR_CONSECUTIVE} total entries. Clearing all entries.")
                
                # Clear all entries for all bearings
                keys = redis_client.keys('*:labels')
                for k in keys:
                    redis_client.delete(k)
                    print(f"Deleted key: {k}")
            
    
    except Exception as e:
        print(f"Error processing message: {e}")

# Create an MQTT client instance
client = mqtt.Client()
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

# Connect to the MQTT broker
try:
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
except Exception as e:
    print(f"Failed to connect to MQTT broker: {e}")

# Start the MQTT client loop
while True:
    client.loop_start()
    sleep(2)
    client.loop_stop()
