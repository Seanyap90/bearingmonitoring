import os
import pandas as pd
import requests
from time import sleep
import glob
import threading
import paho.mqtt.client as mqtt
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# Define the base directory and server URL
BASE_DIR = 'Learning_set'
SERVER_URL = 'http://localhost:8080/sendData'
BROKER = "127.0.0.1"
NUM_WORKERS = 6  # Number of concurrent workers
client = mqtt.Client("P1")
client.connect(BROKER, 1883, 60)

# Function to send DataFrame to the server
def send_dataframe(csv_file, directory, client):
    try:
        sleep(10)
        column_names=["h", "m", "s", "ms", "Hacc", "Vacc"]
        df = pd.read_csv(csv_file, header=None, names=column_names)
        h = df['h'].iloc[0]
        m = df['m'].iloc[0]
        s = df['s'].iloc[0]
        time_str = f"{h:02}:{m:02}:{s:02}"
        df['Directory'] = os.path.basename(directory)
        print(time_str + ": "+ df['Directory'].iloc[0])
        json_data = df.to_json(orient='records')
        headers = {'Content-Type': 'application/json'}
        client.publish("bearing/sendData", json_data, qos=0)
        #response = requests.post(url, data=json_data, headers=headers)
        sleep(8)
        return (csv_file)
    except Exception as e:
        return (csv_file, None, str(e))

# Function to process all CSV files in a directory
def process_directory(directory, url):
    worker_id = threading.get_ident()
    print(f'Worker {worker_id} starting to process directory: {directory}')
    sleep(5)
    csv_files = sorted(glob.glob(os.path.join(directory, '*.csv')))
    results = []
    for csv_file in csv_files:
        print(f'Worker {worker_id} opening CSV file: {csv_file}')
        result = send_dataframe(csv_file, directory, url)
        sleep(5)
        results.append(result)
    return results

# Find all subdirectories in the base directory
subdirectories = [os.path.join(BASE_DIR, name) for name in os.listdir(BASE_DIR)
                  if os.path.isdir(os.path.join(BASE_DIR, name))]

# Process each CSV file concurrently
with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
    futures = {executor.submit(process_directory, subdirectory, client): subdirectory for subdirectory in subdirectories}
    for future in as_completed(futures):
        subdirectory = futures[future]
        try:
            csv_file = future.result()
            if status_code:
                print(f'Processed {csv_file}: Status Code: {status_code}, Response: {response}')
            else:
                print(f'Error processing {csv_file}: {response}')
        except Exception as e:
            print(f'Error processing directory {subdirectory}: {e}')

client.disconnect()