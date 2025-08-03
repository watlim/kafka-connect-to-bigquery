from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
import requests

spark = SparkSession.builder.appName("MoviesCleansing").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
spark.conf.set("spark.sql.adaptive.enabled", "false")


schema_registry_url = "http://schema-registry:8081"
topic_name = "kafka-class-db-001.demo.movies"

# ดึง schema จาก schema registry
def get_avro_schema(schema_registry_url, topic_name):
    url = f"{schema_registry_url}/subjects/{topic_name}-value/versions/latest"
    response = requests.get(url)
    response.raise_for_status()
    schema_json = response.json()['schema']
    return schema_json

avro_schema = get_avro_schema(schema_registry_url, topic_name)

# อ่าน Kafka stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", topic_name) \
    .load()

# แปลงข้อมูล avro จาก column "value"
avro_df = df.select(from_avro(col("value"), avro_schema).alias("data")) \
            .filter(col("data.after").isNotNull()) \
            .select("data.after.*")


avro_df.createOrReplaceTempView("input_table")

try:
    with open("/opt/spark-apps/cleansing.sql", "r") as f:
        sql_query = f.read()
except Exception as e:
    spark.stop()
    raise RuntimeError(f"Error reading SQL file: {e}")

cleansed_df = spark.sql(sql_query)

DEBUG_MODE = True

if DEBUG_MODE:
    query = cleansed_df.writeStream \
        .format("console") \
        .option("truncate", "false") \
        .start()
else:
    query = cleansed_df.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("topic", "cleaned-movies-topic") \
        .option("checkpointLocation", "/opt/spark-apps/checkpoints/movies-cleaning") \
        .start()

query.awaitTermination()
