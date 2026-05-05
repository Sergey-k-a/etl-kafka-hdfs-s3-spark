#!/usr/bin/env python3
"""
Spark Structured Streaming
Kafka → Bronze (Parquet)
"""

import sys
sys.path.append('/app')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import time
import boto3
from botocore.client import Config


class StreamingProcessor:

    def __init__(self):
        self.spark = None
        self.queries = []
        self.minio_endpoint = "http://shminio:9302"
        self.minio_access_key = "minioadmin"
        self.minio_secret_key = "minioadmin"

    def log(self, msg):
        """Timestamped logging"""
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

    def get_s3_client(self):
        """Создает клиент S3/MinIO"""
        return boto3.client(
            's3',
            endpoint_url=self.minio_endpoint,
            aws_access_key_id=self.minio_access_key,
            aws_secret_access_key=self.minio_secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
    
    def ensure_bucket_and_path(self, bucket, path=""):
        """
        Проверяет существование бакета и создает
        """
        s3 = self.get_s3_client()
        
        # 1. Проверяем/создаем бакет
        try:
            s3.head_bucket(Bucket=bucket)
            self.log(f"✓ Bucket '{bucket}' already exists")
        except Exception as e:
            self.log(f"ℹ Bucket '{bucket}' not found, creating...")
            try:
                s3.create_bucket(Bucket=bucket)
                self.log(f"✓ Bucket '{bucket}' created successfully")
            except Exception as create_error:
                self.log(f"✗ Failed to create bucket '{bucket}': {create_error}")
                raise
        
        
        if path:
            if not path.endswith('/'):
                path += '/'
            
            try:
                result = s3.list_objects_v2(
                    Bucket=bucket,
                    Prefix=path,
                    MaxKeys=1
                )
                if 'Contents' in result:
                    self.log(f"✓ Path '{path}' already exists in bucket '{bucket}'")
                else:
                    self.log(f"ℹ Path '{path}' not found, creating...")
                    s3.put_object(Bucket=bucket, Key=path)
                    self.log(f"✓ Path '{path}' created in bucket '{bucket}'")
            except Exception as e:
                self.log(f"ℹ Creating path '{path}' in bucket '{bucket}'...")
                s3.put_object(Bucket=bucket, Key=path)
                self.log(f"✓ Path '{path}' created in bucket '{bucket}'")

    def create_spark_session(self):
        self.log("Creating Spark session...")
        t0 = time.time()
        
        # Автоматически создаем нужные бакеты и пути
        self.log("Checking MinIO buckets and paths...")
        self.ensure_bucket_and_path("bronze", "events/")
        self.ensure_bucket_and_path("logs", "spark-events/")
        
        self.spark = (
            SparkSession.builder
            .appName("Kafka-To-Bronze-Optimized")
            .config("spark.hadoop.fs.s3a.endpoint", self.minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", self.minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.files.maxPartitionBytes", "268435456")
            .config("spark.sql.files.openCostInBytes", "134217728")

            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
            .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
            
            # Включение метрик и логов
            .config("spark.sql.streaming.metricsEnabled", "true")
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", "s3a://logs/spark-events/")
            
            # Ускоряет первый батч
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel("WARN")
        self.log(f"✓ Spark session created in {time.time()-t0:.1f}s")

    def read_from_kafka(self, topic):
        self.log(f"Setting up Kafka reader for topic: {topic}")
        t0 = time.time()
        
        stream = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "shkafka:29092")
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("maxOffsetsPerTrigger", "50000")
            .option("failOnDataLoss", "false")
            .load()
        )
        
        self.log(f"✓ Kafka reader ready in {time.time()-t0:.1f}s")
        return stream

    def process_user_events(self):
        self.log("=" * 50)
        self.log("PROCESSING: User Events Pipeline")
        self.log("=" * 50)

        t0 = time.time()
        kafka_stream = self.read_from_kafka("ecommerce.user.events")

        self.log("Defining schema...")
        schema = StructType([
            StructField("event_id", StringType()),
            StructField("session_id", StringType()),
            StructField("user_id", StringType()),
            StructField("username", StringType()),
            StructField("event_timestamp", StringType()),
            StructField("event_type", StringType()),
            StructField("action", StringType()),
            StructField("product_id", StringType()),
            StructField("product_name", StringType()),
            StructField("category", StringType()),
            StructField("price", DoubleType()),
            StructField("page_url", StringType()),
            StructField("device", StringType()),
            StructField("browser", StringType()),
            StructField("ip_address", StringType()),
            StructField("country", StringType()),
            StructField("membership_level", StringType()),
            StructField("session_duration_sec", IntegerType())
        ])

        self.log("Parsing Kafka messages...")
        parsed = (
            kafka_stream
            .select(from_json(col("value").cast("string"), schema).alias("data"))
            .select("data.*")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("event_timestamp", to_timestamp("event_timestamp"))
            .withColumn("year", year("event_timestamp"))
            .withColumn("month", month("event_timestamp"))
            .withColumn("day", dayofmonth("event_timestamp"))
        )

        
        self.log("Applying repartition strategy...")
        parsed = parsed.repartition(4, "year", "month", "day")

        
        self.log("Starting write stream to Bronze...")
        query = (
            parsed.writeStream
            .format("parquet")
            .option("path", "s3a://bronze/events/")
            .option(
                "checkpointLocation",
                "/tmp/spark-checkpoints/bronze-events"
            )
            .option("compression", "snappy")
            .partitionBy("year", "month", "day")
            .trigger(processingTime="300 seconds")
            .outputMode("append")
            .queryName("Bronze-Events")
            .start()
        )

        self.log(f"✓ Stream started (setup took {time.time()-t0:.1f}s)")
        self.log(f"  Query name: {query.name}")
        self.log(f"  Query ID: {query.id}")
        self.log(f"  Status: {query.status}")
        
        # Мониторинг
        import threading
        def monitor_progress():
            last_batch = -1
            while True:
                time.sleep(15)  # Проверка каждые 15 секунд
                try:
                    if query.lastProgress:
                        p = query.lastProgress
                        batch_id = p['batchId']
                        if batch_id != last_batch:
                            last_batch = batch_id
                            rows = p['numInputRows']
                            batch_duration = p['durationMs']['triggerExecution']
                            self.log(
                                f"📦 Batch #{batch_id}: "
                                f"{rows} rows, "
                                f"duration: {batch_duration}ms"
                            )
                    else:
                        # Статус запроса (исправлено!)
                        status = query.status
                        if isinstance(status, dict):
                            msg = status.get('message', 'Unknown')
                        else:
                            msg = str(status)
                        self.log(f"⏳ Query status: {msg}")
                except Exception as e:
                    self.log(f"⚠ Monitor error: {e}")

        monitor_thread = threading.Thread(target=monitor_progress, daemon=True)
        monitor_thread.start()

        self.queries.append(query)


    def run(self):
        self.log("=" * 60)
        self.log("SPARK STREAMING: Kafka → Bronze")
        self.log("=" * 60)

        self.create_spark_session()
        self.process_user_events()

        self.log(f"✓ Started {len(self.queries)} streaming query/ies")
        self.log("Press Ctrl+C to stop...")
        self.log("")

        try:
            for q in self.queries:
                q.awaitTermination()

        except KeyboardInterrupt:
            self.log("\nShutting down gracefully...")
            for q in self.queries:
                self.log(f"Stopping query: {q.name}")
                q.stop()
                self.log(f"✓ Query {q.name} stopped")

        self.log("Stopping Spark session...")
        self.spark.stop()
        self.log("✓ Shutdown complete")


if __name__ == "__main__":
    StreamingProcessor().run()