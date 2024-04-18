import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
import logging
import sys
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import json

client = google.cloud.logging.Client()
handler = CloudLoggingHandler(client)
cloud_logger = logging.getLogger('cloudLogger')
cloud_logger.setLevel(logging.INFO)
cloud_logger.addHandler(handler)

instance_id = sys.argv[1] if len(sys.argv) > 1 else "Default Receiver"

project_id = "cs510-project1"
subscription_id = "my-sub"

timeout = 60.0

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    message_data = json.loads(message.data.decode('utf-8'))
    print(f"{instance_id} Received message:\n{json.dumps(message_data, indent=4)}")
    cloud_logger.info(f"{instance_id} Received message.")
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    print(f"{instance_id} Listening for messages on {subscription_path}..\n")
    try:
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.

