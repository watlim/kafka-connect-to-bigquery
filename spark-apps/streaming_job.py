from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
import requests

spack = SparkSession.builder \
    .appName("StreamingJob") \
    .getOrCreate()
spack.sparkContext.setLogLevel("WARN") 

raw_streaming_df = spack.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "kafka-class-db-001.demo.movies") \
    .option("startingOffsets", "earliest") \
    .load()

# แสดงข้อความดิบ (แค่ debug)
raw_streaming_df.selectExpr("CAST(value AS STRING)").writeStream.format("console") \
    .option("truncate", False).start()

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

cleansed_df = raw_streaming_df.select(
    from_avro(col("value"), avro_schema_str, {"mode": "PERMISSIVE"}).alias("data")
).select("data.after.*")

DEBUG = True
if DEBUG:
    query = cleansed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
else:
    query = cleansed_df.writeStream \
        .outputMode("append") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("topic", "kafka-class-db-001.demo.movies.clearsed") \
        .option("checkpointLocation", "/tmp/kafka-class-db-001.demo.movies.clearsed.checkpoint") \
        .start()

query.awaitTermination()
