# Databricks Python notebook: start a continuously running Bronze stream.
# Store these credentials in Databricks secret scope 'fleetpulse'.
CATALOG = "fleetpulse"
TOPIC = "fleetpulse.gps.raw"
BOOTSTRAP = dbutils.secrets.get("fleetpulse", "kafka-bootstrap")
KAFKA_USER = dbutils.secrets.get("fleetpulse", "kafka-api-key")
KAFKA_PASSWORD = dbutils.secrets.get("fleetpulse", "kafka-api-secret")

kafka_options = {
    "kafka.bootstrap.servers": BOOTSTRAP,
    "subscribe": TOPIC,
    "startingOffsets": "earliest",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";'
    ),
}

bronze_df = (
    spark.readStream.format("kafka").options(**kafka_options).load()
    .selectExpr(
        "CAST(key AS STRING) AS message_key",
        "CAST(value AS STRING) AS raw_json",
        "topic", "partition", "offset",
        "timestamp AS kafka_timestamp",
    )
)

query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"/Volumes/{CATALOG}/ops/checkpoints/bronze_gps")
    .trigger(processingTime="10 seconds")
    .toTable(f"{CATALOG}.bronze.gps_raw")
)
print("Bronze stream:", query.id)
