import functions_framework
import logging
from google.cloud import pubsub_v1
import json

logging.basicConfig(level=logging.INFO)

# Initialize Pub/Sub client
publisher = pubsub_v1.PublisherClient()
project_id = "kalanu-scout"
topic_id = "TBA_New_Score"
topic_path = publisher.topic_path(project_id, topic_id)

@functions_framework.http
def webhook_target(request):
    request_json = request.get_json(silent=True)
    
    logging.info(f"Request JSON: {request_json}")

    if request_json and "message_data" in request_json and "match" in request_json["message_data"] and "event_key" in request_json["message_data"]["match"]:
        event_key = request_json["message_data"]["match"]["event_key"]
        # Publish the event_key to Pub/Sub
        message_json = json.dumps({"event_key": event_key})
        message_bytes = message_json.encode("utf-8")
        try:
            publish_future = publisher.publish(topic_path, data=message_bytes)
            publish_future.result()  # Verify the publish succeeded
            logging.info(f"Published {event_key} to {topic_path}")
        except Exception as e:
            logging.error(f"Publishing to Pub/Sub failed: {e}")
            return "Publishing to Pub/Sub failed", 500
    else:
        return "Invalid request", 400

    return "OK", 200
