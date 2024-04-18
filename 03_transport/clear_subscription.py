from google.cloud import pubsub_v1

# TODO (developer)
project_id = "cs510-project1"
subscription_id = "my-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Delete the subscription
subscriber.delete_subscription(subscription=subscription_path)
print(f"Subscription {subscription_id} deleted.")

