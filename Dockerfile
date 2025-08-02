FROM bitnami/spark:3.5.0

# คัดลอก requirements แล้วติดตั้ง
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# คัดลอกโค้ด Spark เข้า container
COPY spark-apps /opt/spark-apps

# Optional: กำหนด Python interpreter
ENV PYSPARK_PYTHON=python
ENV SPARK_LOG_DIR=/opt/spark-apps/logs

# ใช้ spark-submit พร้อม package Kafka และ BigQuery
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.apache.spark:spark-avro_2.12:3.5.0,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.0", "/opt/spark-apps/streaming_job.py"]