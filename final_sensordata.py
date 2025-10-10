from decimal import Decimal
import requests
import time
from datetime import datetime
import random
import threading

# API endpoint
api_url = "https://mknncrj1nj.execute-api.us-east-1.amazonaws.com/dev/data"

# Number of sensors and installations
num_sensors = 10  
installation_names = ["Site_A", "Site_B", "Site_C", "Site_D", "Site_E"]

# Assign sensors evenly to installations
installation_map = {}
for i, site in enumerate(installation_names):
    installation_map[site] = []

sensor_ids = [f"SENSOR_{i:03d}" for i in range(1, num_sensors + 1)]
for idx, sensor_id in enumerate(sensor_ids):
    site = installation_names[idx % len(installation_names)]
    installation_map[site].append(sensor_id)

# Function to simulate a sensor reading
def simulate_sensor_reading(sensor_id):
    return {
        "ID": sensor_id,
        "timestamp": int(datetime.now().timestamp()),
        "IA": round(random.uniform(0.5, 2.0), 2),
        "IB": round(random.uniform(0.5, 2.0), 2),
        "IC": round(random.uniform(0.5, 2.0), 2)
    }

# Function to send data for an entire installation
def simulate_installation(site_name, sensors, interval=5):
    try:
        while True:
            sensors_payload = [simulate_sensor_reading(sid) for sid in sensors]
            payload = {
                "Installations": {
                    "name": site_name,
                    "sensors": sensors_payload
                }
            }

            response = requests.post(api_url, json=payload)
            sensor_ids_str = ", ".join(sensors)
            print(f"[{site_name}] Sent data for sensors: {sensor_ids_str}, status: {response.status_code}")

            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"Simulation stopped for installation {site_name}")

# Start a thread per installation
threads = []
for site_name, sensors in installation_map.items():
    t = threading.Thread(target=simulate_installation, args=(site_name, sensors))
    t.start()
    threads.append(t)

# Keep main thread alive
for t in threads:
    t.join()
