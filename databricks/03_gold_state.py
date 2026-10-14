# Databricks Python notebook: update one latest-known record per device.
from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window

CATALOG = "fleetpulse"
STATE = f"{CATALOG}.gold.device_state"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {STATE} (
  device_id STRING, event_id STRING, event_time TIMESTAMP,
  received_at TIMESTAMP, latitude DOUBLE, longitude DOUBLE,
  speed_kmh DOUBLE, is_moving BOOLEAN
) USING DELTA
""")


def merge_state(batch, batch_id):
    if batch.isEmpty():
        return
    w = Window.partitionBy("device_id").orderBy(
        F.col("event_time").desc(), F.col("event_id").desc()
    )
    latest = (
        batch.withColumn("row_number", F.row_number().over(w))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
        .select("device_id", "event_id", "event_time", "received_at",
                "latitude", "longitude", "speed_kmh", "is_moving")
    )
    target = DeltaTable.forName(spark, STATE)
    (
        target.alias("t")
        .merge(latest.alias("s"), "t.device_id = s.device_id")
        .whenMatchedUpdateAll(condition=(
            "s.event_time > t.event_time OR "
            "(s.event_time = t.event_time AND s.event_id > t.event_id)"
        ))
        .whenNotMatchedInsertAll()
        .execute()
    )


query = (
    spark.readStream.table(f"{CATALOG}.silver.gps_events")
    .writeStream.foreachBatch(merge_state)
    .option("checkpointLocation", f"/Volumes/{CATALOG}/ops/checkpoints/gold_state")
    .trigger(processingTime="10 seconds")
    .start()
)
print("Gold state stream:", query.id)
