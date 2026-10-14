"""Generate self-labeled synthetic GPS events for smoke testing without Geotab."""
import argparse
import random
import time
from datetime import datetime, timezone
from uuid import uuid4
from ingestion.common import kafka_producer, publish_confirmed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=50)
    parser.add_argument("--devices", type=int, default=5)
    parser.add_argument("--rate", type=float, default=5.0, help="target messages/sec")
    args = parser.parse_args()
    producer = kafka_producer()
    random.seed(42)
    for i in range(args.events):
        device_id = f"SIM-{i % args.devices:04d}"
        payload = {
            "schema_version": 1,
            "source": "synthetic_test",
            "event_id": f"sim:{uuid4()}",
            "device_id": device_id,
            "event_time": datetime.now(timezone.utc).isoformat(),
            "received_at": datetime.now(timezone.utc).isoformat(),
            "latitude": 35.22 + random.uniform(-0.02, 0.02),
            "longitude": -80.84 + random.uniform(-0.02, 0.02),
            "speed_kmh": round(random.uniform(0, 45), 2),
        }
        publish_confirmed(producer, [(device_id, payload)])
        if args.rate > 0:
            time.sleep(1 / args.rate)
    print(f"Published {args.events} synthetic records")


if __name__ == "__main__":
    main()
