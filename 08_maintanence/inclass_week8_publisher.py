import requests
import pandas as pd
from datetime import datetime
import json
import time
from tqdm import tqdm
from google.cloud import pubsub_v1
from bs4 import BeautifulSoup

def get_vehicle_ids():
    doc_key = "10VKMye65LhbEgMLld5Ol3lOocWUwCaEgnPVgFQf9em0"
    url = f"https://docs.google.com/spreadsheets/d/{doc_key}/export?format=csv"
    response = requests.get(url)
    csv_data = response.content
    with open("vehicle_ids_sheet.csv", "wb") as file:
        file.write(csv_data)
    vehicle_ids = pd.read_csv("vehicle_ids_sheet.csv")['Doodle'].tolist()
    return vehicle_ids

def publish_breadcrumbs():
    project_id = "cs510-project1"
    topic_id = "archivetest"
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    vehicle_ids = get_vehicle_ids()

    for vehicle_id in tqdm(vehicle_ids, desc="Processing vehicle IDs"):
        url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
        response = requests.get(url)
        if response.status_code == 200:
            breadcrumbs = response.json()
            for breadcrumb in breadcrumbs:
                data_str = json.dumps(breadcrumb)
                data_bytes = data_str.encode('utf-8')
                publisher.publish(topic_path, data_bytes)
        time.sleep(1)  # Respectful delay to avoid rate limiting

if __name__ == "__main__":
    publish_breadcrumbs()

