from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
from cleansing_rules import apply_cleansing
import requests
import os

# ------------------------------
# spark session
# ------------------------------
spark = SparkSession.builder \
    .appName("StreamingJobCleansing") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ------------------------------
# kafka raw streaming
# ------------------------------
raw_streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "kafka-class-db-001.demo.movies") \
    .option("startingOffsets", "earliest") \
    .load()

# ------------------------------
# get Avro schema from Schema Registry
# ------------------------------
schema_registry_url = "http://schema-registry:8081"
topic_name = "kafka-class-db-001.demo.movies"

def get_avro_schema(topic):
    subject = f"{topic}-value"
    latest_version_url = f"{schema_registry_url}/subjects/{subject}/versions/latest"
    resp = requests.get(latest_version_url)
    resp.raise_for_status()
    return resp.json()["schema"]

avro_schema_str = get_avro_schema(topic_name)
print("Avro Schema from Schema Registry:")
print(avro_schema_str)

# ------------------------------
# decode Avro message
# ------------------------------
decoded_df = raw_streaming_df.select(
    from_avro(col("value"), avro_schema_str, {"mode": "PERMISSIVE"}).alias("data")
).select("data.after.*")  # ถ้าไม่ใช่ Debezium ให้ใช้ "data.*"

# ------------------------------
# cleansing
# ------------------------------
cleansed_df = apply_cleansing(decoded_df)
# ------------------------------
# write stream
# ------------------------------
DEBUG = True  # True = console, False = Kafka

if DEBUG:
    query = cleansed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
else:
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        raise EnvironmentError("Please set GOOGLE_APPLICATION_CREDENTIALS environment variable")
    query = cleansed_df.writeStream \
        .outputMode("append") \
        .format("bigquery") \
        .option("table", "datath-th-kafka.bigquery_demo.movies_cleansed") \
        .option("checkpointLocation", "/opt/spark-apps/checkpoints/datath-th-kafka.bigquery_demo.movies.cleansed.checkpoint") \
        .start()

query.awaitTermination()
