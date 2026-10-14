"""Geotab LogRecord incremental feed -> Kafka; at-least-once semantics."""
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
import mygeotab
from ingestion.common import PROJECT_ROOT, kafka_producer, publish_confirmed

CURSOR_FILE = PROJECT_ROOT / os.getenv("GEOTAB_CURSOR_PATH", "state/geotab_logrecord_cursor.json")
BATCH_LIMIT = 500


def load_cursor():
    if not CURSOR_FILE.exists():
        return os.getenv("GEOTAB_FIRST_VERSION", "0")
    return json.loads(CURSOR_FILE.read_text(encoding="utf-8"))["toVersion"]


def save_cursor(token):
    CURSOR_FILE.parent.mkdir(parents=True, exist_ok=True)
    temp = CURSOR_FILE.with_suffix(".tmp")
    temp.write_text(json.dumps({"toVersion": token}), encoding="utf-8")
    temp.replace(CURSOR_FILE)


def geotab_client():
    kwargs = {
        "username": os.environ["GEOTAB_USERNAME"],
        "password": os.environ["GEOTAB_PASSWORD"],
        "database": os.environ["GEOTAB_DATABASE"],
    }
    if os.getenv("GEOTAB_SERVER"):
        kwargs["server"] = os.environ["GEOTAB_SERVER"]
    api = mygeotab.API(**kwargs)
    api.authenticate()
    return api


def normalize(record):
    timestamp = record["dateTime"]
    if hasattr(timestamp, "isoformat"):
        timestamp = timestamp.isoformat()
    received = datetime.now(timezone.utc).isoformat()
    device_id = str(record["device"]["id"])
    payload = {
        "schema_version": 1,
        "source": "geotab_demo",
        "event_id": f"logrecord:{record['id']}",
        "device_id": device_id,
        "event_time": str(timestamp),
        "received_at": received,
        "latitude": record.get("latitude"),
        "longitude": record.get("longitude"),
        "speed_kmh": record.get("speed"),
    }
    return device_id, payload


def main():
    api = geotab_client()
    producer = kafka_producer()
    cursor = load_cursor()
    poll_seconds = max(1, int(os.getenv("GEOTAB_POLL_SECONDS", "15")))
    print("Starting GetFeed. Cursor:", cursor)

    while True:
        try:
            feed = api.call(
                "GetFeed", typeName="LogRecord",
                fromVersion=cursor, resultsLimit=BATCH_LIMIT,
            )
            records = feed.get("data", [])
            confirmed = publish_confirmed(producer, map(normalize, records))
            # The cursor advances ONLY after broker acknowledgements.
            cursor = str(feed["toVersion"])
            save_cursor(cursor)
            print(f"Published {confirmed} GPS readings; cursor={cursor}")
            # No faster than ~1 request/sec for this single feed.
            time.sleep(1.2 if len(records) == BATCH_LIMIT else poll_seconds)
        except KeyboardInterrupt:
            print("Stopped by user")
            break
        except Exception as exc:
            print(f"Ingestion error, cursor unchanged: {exc}")
            time.sleep(30)


if __name__ == "__main__":
    main()
