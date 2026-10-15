"""Inspect a Geotab demo database before ingestion."""
import os
from pprint import pprint
import mygeotab
from ingestion.common import PROJECT_ROOT  # loads .env


def main():
    # Authenticate with Geotab using credentials from .env, then sample devices and GPS feed.
    kwargs = {
        "username": os.environ["GEOTAB_USERNAME"],
        "password": os.environ["GEOTAB_PASSWORD"],
        "database": os.environ["GEOTAB_DATABASE"],
    }

    if os.getenv("GEOTAB_SERVER"):
        kwargs["server"] = os.environ["GEOTAB_SERVER"]
        
    api = mygeotab.API(**kwargs)
    api.authenticate()
    devices = api.call("Get", typeName="Device", resultsLimit=10)
    print(f"First {len(devices)} devices:")
    
    for device in devices:
        print(device.get("id"), device.get("name"))

    feed = api.call("GetFeed", typeName="LogRecord", fromVersion="0", resultsLimit=3)
    print(f"First GPS records: {len(feed.get('data', []))}")

    for record in feed["data"]:
        pprint(record)
        print("-" * 80)


if __name__ == "__main__":
    main()
