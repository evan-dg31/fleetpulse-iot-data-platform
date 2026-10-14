"""Shared Kafka configuration and validation helpers."""
import os
from pathlib import Path
from dotenv import load_dotenv
from confluent_kafka import Producer

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

TOPIC = os.getenv("KAFKA_TOPIC", "fleetpulse.gps.raw")


def kafka_producer():
    config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "client.id": "fleetpulse-ingestion",
        "enable.idempotence": True,
        "acks": "all",
    }
    key = os.getenv("KAFKA_API_KEY")
    secret = os.getenv("KAFKA_API_SECRET")
    if key and secret:
        config.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": key,
            "sasl.password": secret,
        })
    return Producer(config)


def publish_confirmed(producer, messages, timeout=60):
    """Send a batch and wait for broker acknowledgements before returning."""
    failures = []
    count = 0

    def on_delivery(error, message):
        if error is not None:
            failures.append(str(error))

    for device_id, payload in messages:
        import json
        producer.produce(
            TOPIC,
            key=device_id.encode("utf-8"),
            value=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            on_delivery=on_delivery,
        )
        producer.poll(0)
        count += 1

    remaining = producer.flush(timeout)
    if failures or remaining:
        raise RuntimeError(f"Kafka delivery failed: {failures[:3]}, remaining={remaining}")
    return count
