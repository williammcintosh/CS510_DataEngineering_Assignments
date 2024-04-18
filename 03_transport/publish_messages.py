import json
from google.cloud import pubsub_v1
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

project_id = "cs510-project1"
topic_id = "my-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Load the JSON data from the file
with open('bcsample.json', 'r') as file:
    data = json.load(file)

# Iterate over each vehicle's data
for vehicle_key in data:
    records = data[vehicle_key]
    for i, record in enumerate(records):
        if i >= 200:
            break
        record['timestamp'] = time.time()
        data_str = json.dumps(record)
        data_bytes = data_str.encode("utf-8")
        future = publisher.publish(topic_path, data_bytes)
        print(f"Published record from {vehicle_key} to Pub/Sub: {future.result()}")
        logging.info(f"Published record from {vehicle_key} to Pub/Sub: {future.result()}")


print(f"Published all messages to {topic_path}.")

