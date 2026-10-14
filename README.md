# FleetPulse — Fleet Telematics Streaming Platform

Portfolio starter: Geotab **demo** telemetry -> Kafka -> Azure Databricks Spark -> Delta Lake -> AI/BI dashboard.

This starter has not been tested with your private Geotab credentials or Databricks workspace.

## Step 1. Local development (VS Code / PowerShell)

Install Python 3.11+, VS Code with Python extension, Docker Desktop and Git.

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Edit `.env`. For local Kafka leave `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` and blank Kafka API credentials.

## Step 2. Run Apache Kafka for local smoke tests

```powershell
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic fleetpulse.gps.raw --partitions 3 --replication-factor 1
python -m ingestion.synthetic_publisher --events 30 --devices 5 --rate 5
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fleetpulse.gps.raw --from-beginning --max-messages 5
```

Run tests: `pytest -q`.

## Step 3. Geotab demo

Create a demo database: https://support.geotab.com/help/mygeotab/access-and-administration/getting-started/database-setup

Choose Public works as demo vocation if available. Fill Geotab account variables in `.env` and run:

```powershell
python -m ingestion.inspect_geotab
python -m ingestion.geotab_to_kafka
```

The default starts from historical beginning (`GEOTAB_FIRST_VERSION=0`). It can take a while. Stop it with Ctrl+C and restart; it should resume from `state/geotab_logrecord_cursor.json`.

If Geotab has no demo GPS records, you can continue using `synthetic_publisher.py` but label your data as synthetic.

## Step 4. Connect Azure Databricks (cloud deployment)

**Local `localhost:9092` is not reachable from remote Databricks.** Switch your `.env` to a managed, network-reachable Kafka broker (e.g. Confluent Cloud), create `fleetpulse.gps.raw` with 3 partitions, and obtain API key/secret.

Set `.env`. **Important:** change `GEOTAB_CURSOR_PATH` to a new filename when changing Kafka destinations, otherwise you could skip data already checkpointed against your local broker:

```dotenv
GEOTAB_CURSOR_PATH=state/geotab_cloud_cursor.json
KAFKA_BOOTSTRAP_SERVERS=YOUR_CLOUD_BOOTSTRAP:9092
KAFKA_API_KEY=YOUR_CLUSTER_KEY
KAFKA_API_SECRET=YOUR_CLUSTER_SECRET
```

Your Python producer uses SASL_SSL automatically when credentials are set.

Create a secret scope named `fleetpulse` with these keys: `kafka-bootstrap`, `kafka-api-key`, `kafka-api-secret`. Do NOT paste secrets into notebooks or Git.

Databricks steps, in this order:

1. Run `databricks/00_setup.sql` in SQL editor; if you cannot create a catalog, replace `fleetpulse` with a catalog you can use in every SQL/Python file.
2. Start `databricks/01_bronze.py` on streaming-compatible compute.
3. Start `databricks/02_silver.py`.
4. Start `databricks/03_gold_state.py`.
5. Run the four queries in `databricks/04_dashboard_queries.sql` separately in SQL editor.
6. Databricks -> New -> Dashboard -> Data -> Add SQL dataset; add location, counters and time series; create a Point Map, counters and line graph. Publish when ready.

All three streaming jobs need to keep running (or use an appropriate scheduled incremental processing strategy). They have separate checkpoint directories in Unity Catalog Volume.

## Data semantics & caveats

- `LogRecord` has lat/lon/speed, **not necessarily fuel, engine state or operating hours**. Movement (`speed_kmh > 1`) is a **proxy**, not actual rental utilization.
- Retain original `event_time` alongside `received_at`. Old demo timestamps do not mean current live events.
- `GetFeed` is incremental REST polling; do not insert MQTT between Geotab and Kafka.
- At-least-once semantics: cursor saved after Kafka acknowledgements; duplicates possible after crash. Silver deduplicates by event ID within an event-time watermark.
- Invalid Silver records are filtered, not yet quarantined; production extension should add a dead-letter table, corrections, diagnostics, fleet dimensions and tests.
- The Gold `MERGE` only adopts a strictly newer event timestamp (or deterministic event-ID tie-break) per device, preventing late updates from regressing a device's position.

## Future phase (MQTT simulator)

Simulated physical device -> MQTT -> Kafka (same canonical event contract) -> reuse downstream Bronze/Silver/Gold pipelines.

Official references:
- https://developers.geotab.com/myGeotab/guides/dataFeed/index.html
- https://learn.microsoft.com/en-us/azure/databricks/connect/streaming/kafka
- https://learn.microsoft.com/en-us/azure/databricks/dashboards/tutorials/create-dashboard
