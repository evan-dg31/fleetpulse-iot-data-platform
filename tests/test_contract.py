from ingestion.geotab_to_kafka import normalize


def test_normalize_geotab_logrecord():
    record = {
        "id": "b123",
        "device": {"id": "d44"},
        "dateTime": "2026-10-13T20:00:00Z",
        "latitude": 35.2,
        "longitude": -80.8,
        "speed": 15.0,
    }
    key, payload = normalize(record)
    assert key == "d44"
    assert payload["event_id"] == "logrecord:b123"
    assert payload["speed_kmh"] == 15.0
    assert payload["source"] == "geotab_demo"
