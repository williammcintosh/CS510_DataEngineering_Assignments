from google.cloud import pubsub_v1

project_id = "cs510-project1"
subscription_id = "my-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    print(f"Discarding message: {message.data.decode('utf-8')}")
    message.ack()

with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}.. and discarding them.\n")
    try:
        streaming_pull_future.result()  # Block indefinitely.
    except KeyboardInterrupt:
        streaming_pull_future.cancel()  # Cancel subscription on a keyboard interrupt.

