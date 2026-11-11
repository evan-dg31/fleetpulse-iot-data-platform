"""Shared Kafka configuration and validation helpers."""
import os
import json
from pathlib import Path

from dotenv import load_dotenv
from confluent_kafka import Producer

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

TOPIC = os.getenv("KAFKA_TOPIC", "fleetpulse.gps.raw")


def kafka_producer():
    """Create a Kafka producer using the configured connection settings.

    Returns:
        Producer: A configured Confluent Kafka producer.

    Raises:
        ValueError: If only one of the Kafka API key or secret is configured.
    """

    key = os.getenv("KAFKA_API_KEY")
    secret = os.getenv("KAFKA_API_SECRET")

    if bool(key) != bool(secret):
        raise ValueError(
            "KAFKA_API_KEY and KAFKA_API_SECRET must both be configured"
        )

    config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "client.id": "fleetpulse-ingestion",
        "enable.idempotence": True,
        "acks": "all",

    }

    if key and secret:
        config.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": key,
            "sasl.password": secret,
        })

    return Producer(config)


def publish_confirmed(producer, messages, timeout=60):
    """Publish a batch and wait for broker acknowledgements.

    Args:
        producer: The Kafka producer used to publish the messages.
        messages: An iterable of ``(device_id, payload)`` pairs.
        timeout: Maximum time in seconds to wait for delivery confirmation.

    Returns:
        int: The number of messages acknowledged by the broker.

    Raises:
        RuntimeError: If any message fails delivery or messages remain
            undelivered after the timeout.
    """
    failures = []
    acknowledged = 0

    def on_delivery(error, message):
        """Record the result of a message delivery.

        Args:
            error: The delivery error, or ``None`` if delivery succeeded.
            message: The Kafka message associated with this callback.
        """
        nonlocal acknowledged

        if error is not None:
            failures.append(str(error))
        else:
            acknowledged += 1

    for device_id, payload in messages:

        producer.produce(
            topic=TOPIC,
            key=device_id.encode("utf-8"),
            value=json.dumps(
                payload,
                separators=(",", ":")).encode("utf-8"),
            on_delivery=on_delivery,
        )
        producer.poll(0)

    remaining = producer.flush(timeout)

    if failures or remaining:
        raise RuntimeError(
            f"Kafka delivery failed: errores={failures[:3]}, remaining={remaining}")
    return acknowledged
