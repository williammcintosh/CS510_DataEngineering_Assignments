import argparse
import requests
import pandas as pd
from datetime import datetime
from google.cloud import pubsub_v1
import json
import time
import zlib
from tqdm import tqdm
from google.cloud import storage
import logging
from google.cloud.logging.handlers import CloudLoggingHandler
import google.cloud.logging
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import os

# Initialize the logger
client = google.cloud.logging.Client()
handler = CloudLoggingHandler(client)
cloud_logger = logging.getLogger('cloudLogger')
cloud_logger.setLevel(logging.INFO)
cloud_logger.addHandler(handler)

# Prepare storage client
storage_client = storage.Client()
bucket_name = 'cs510-spring24-project1-bucket'
bucket = storage_client.bucket(bucket_name)

# Temporary storage for messages
messages = []

# Load the RSA keys
def load_rsa_keys():
    # Download private key
    private_key_blob = bucket.blob('keys/private_key.pem')
    private_key_pem = private_key_blob.download_as_string()
    private_key = serialization.load_pem_private_key(private_key_pem, password=None)

    # Download public key
    public_key_blob = bucket.blob('keys/public_key.pem')
    public_key_pem = public_key_blob.download_as_string()
    public_key = serialization.load_pem_public_key(public_key_pem)

    return private_key, public_key


private_key, public_key = load_rsa_keys()

# Function to encrypt data with AES
def encrypt_data(data):
    aes_key = os.urandom(32)  # AES-256 key
    iv = os.urandom(16)  # Initialization vector
    cipher = Cipher(algorithms.AES(aes_key), modes.CFB(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    encrypted_data = encryptor.update(data) + encryptor.finalize()

    encrypted_aes_key = public_key.encrypt(
        aes_key,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )

    return iv + encrypted_aes_key + encrypted_data


def get_vehicle_ids():
    doc_key = "10VKMye65LhbEgMLld5Ol3lOocWUwCaEgnPVgFQf9em0"
    url = f"https://docs.google.com/spreadsheets/d/{doc_key}/export?format=csv"
    response = requests.get(url)
    csv_data = response.content
    with open("vehicle_ids_sheet.csv", "wb") as file:
        file.write(csv_data)
    vehicle_ids = pd.read_csv("vehicle_ids_sheet.csv")['Doodle'].tolist()
    return vehicle_ids


def save_to_gcs(data, filename, compressed=False, encrypted=False):
    if compressed:
        data = zlib.compress(json.dumps(data).encode('utf-8'))

    if encrypted:
        data = encrypt_data(data)
        folder_name = "inclass_week8_data_encrypted"
        blob = bucket.blob(f"{folder_name}/{filename}.enc")
        blob.upload_from_string(data)
    else:
        folder_name = "inclass_week8_data_compressed" if compressed else "inclass_week8_data"
        blob = bucket.blob(f"{folder_name}/{filename}")
        blob.upload_from_string(data, content_type='application/json' if not compressed else 'application/gzip')

    cloud_logger.info(f"Data saved successfully to GCS with filename {filename}")


def callback(message):
    message_data = json.loads(message.data.decode('utf-8'))
    messages.append(message_data)
    message.ack()  # Acknowledge the message

    if len(messages) % 1000 == 0:
        print(f"Processed {len(messages)} messages.", end='\r', flush=True)


def sort_and_store_messages(compressed=False, encrypted=False):
    if not messages:
        cloud_logger.debug("No messages to process.")
        return

    # Group messages by 'VEHICLE_ID'
    grouped_messages = {}
    for message in messages:
        vehicle_id = message['VEHICLE_ID']
        if vehicle_id not in grouped_messages:
            grouped_messages[vehicle_id] = []
        grouped_messages[vehicle_id].append(message)

    # Sort each group by 'ACT_TIME'
    for vehicle_id in grouped_messages:
        grouped_messages[vehicle_id].sort(key=lambda x: x['ACT_TIME'])

    # Sort vehicle IDs to ensure keys are in order
    sorted_grouped_messages = {k: grouped_messages[k] for k in sorted(grouped_messages)}

    # Generate a filename with the current date
    today_date = datetime.now().strftime("%Y-%m-%d")
    filename = f"TriMet__{today_date}.json"

    # Convert the dictionary to bytes for encryption
    data_bytes = json.dumps(sorted_grouped_messages).encode('utf-8')

    # Save all messages as a single JSON object indexed by sorted 'VEHICLE_ID'
    save_to_gcs(data_bytes, filename, compressed, encrypted)


def main():
    parser = argparse.ArgumentParser(description="Receive and process messages.")
    parser.add_argument("-c", "--compressed", action="store_true", help="Compress the JSON output")
    parser.add_argument("-e", "--encrypt", action="store_true", help="Encrypt the JSON output")
    args = parser.parse_args()

    project_id = "cs510-project1"
    subscription_id = "archivetest-sub"
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..")

    try:
        streaming_pull_future.result(timeout=150)  # Extended timeout to ensure all messages are received
    except TimeoutError:
        streaming_pull_future.cancel()
        sort_and_store_messages(compressed=args.compressed, encrypted=args.encrypt)

if __name__ == "__main__":
    main()

