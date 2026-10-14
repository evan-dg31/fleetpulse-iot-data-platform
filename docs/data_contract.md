# FleetPulse v1 GPS event contract

Topic: `fleetpulse.gps.raw`
Key: `device_id` (Kafka partition key); event payload is UTF-8 JSON.

```json
{
  "schema_version": 1,
  "source": "synthetic_test",
  "event_id": "sim:d9e6...",
  "device_id": "SIM-0001",
  "event_time": "2026-10-13T21:00:00+00:00",
  "received_at": "2026-10-13T21:00:01+00:00",
  "latitude": 35.22,
  "longitude": -80.84,
  "speed_kmh": 10.5
}
```

- `source`: `geotab_demo` or `synthetic_test`; never claim Geotab demo data is physical device observation.
- `event_id`: Stable Geotab LogRecord ID for idempotent ingestion; random UUID for test events.
- `event_time`: SOURCE GPS observation timestamp, not ingestion time.
- `received_at`: Timestamp the Python ingestion service received/normalized the record.
- `is_moving`: speed > 1 km/h derived field; not engine state or true rental utilization.
- Geotab feed cursors are **not** event timestamps and records need not arrive by event time.
- Silver invalid records are filtered in this starter; create quarantine table before production.
- For historical demo data, `now() - event_time` measures age of a historical observation, not live latency.
