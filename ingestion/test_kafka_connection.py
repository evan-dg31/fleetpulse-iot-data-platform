import json
import os

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

producer = Producer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "sasl.mechanism": "PLAIN",
    "sasl.username": os.environ["KAFKA_API_KEY"],
    "sasl.password": os.environ["KAFKA_API_SECRET"],
    "security.protocol": "SASL_SSL",
    "client.id": "test_kafka_connection"
})

topic = os.environ["KAFKA_TOPIC"]


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(
            f"Message delivered to {msg.topic()} [{msg.partition()}] {msg.offset()}")


event = {

    "event_id": "test-001",
    "device_id": "b28",
    "event_time": "2026-10-07T01:12:09.429Z",
    "latitude": 43.5162659,
    "longitude": -79.6833496,
    "speed_kmh": 6,
}

producer.produce(
    topic=topic,
    key=event["device_id"],
    value=json.dumps(event),
    callback=delivery_report
)

producer.flush()

# def main():
#     conf = {
#         "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
#         "sasl.mechanism": "PLAIN",
#         "sasl.username": os.environ["KAFKA_API_KEY"],
#         "sasl.password": os.environ["KAFKA_API_SECRET"],
#         "security.protocol": "SASL_SSL",
#     }

#     producer = Producer(conf)
#     topic = os.environ["KAFKA_TOPIC"]
#     message = json.dumps({"test": "message"})
#     producer.produce(topic, message.encode("utf-8"))
#     producer.flush()


# if __name__ == "__main__":
#     main()
