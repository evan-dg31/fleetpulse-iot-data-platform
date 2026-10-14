# Databricks Python notebook: parse, validate and deduplicate GPS events.
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

CATALOG = "fleetpulse"
schema = StructType([
    StructField("schema_version", IntegerType()),
    StructField("source", StringType()),
    StructField("event_id", StringType()),
    StructField("device_id", StringType()),
    StructField("event_time", StringType()),
    StructField("received_at", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("speed_kmh", DoubleType()),
])

silver_df = (
    spark.readStream.table(f"{CATALOG}.bronze.gps_raw")
    .withColumn("event", F.from_json("raw_json", schema))
    .select("event.*", "kafka_timestamp", "partition", "offset")
    .withColumn("event_time", F.to_timestamp("event_time"))
    .withColumn("received_at", F.to_timestamp("received_at"))
    .filter(
        F.col("event_id").isNotNull()
        & F.col("device_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("latitude").between(-90, 90)
        & F.col("longitude").between(-180, 180)
        & (F.col("speed_kmh") >= 0)
        & (F.col("speed_kmh") < 400)
    )
    # For a demo; tune the watermark to actual source late-arrival behavior.
    .withWatermark("event_time", "2 days")
    .dropDuplicates(["event_id"])
    .withColumn("is_moving", F.col("speed_kmh") > F.lit(1.0))
)

query = (
    silver_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"/Volumes/{CATALOG}/ops/checkpoints/silver_gps")
    .trigger(processingTime="10 seconds")
    .toTable(f"{CATALOG}.silver.gps_events")
)
print("Silver stream:", query.id)
