#!/usr/bin/env python3
"""
Spark Structured Streaming: Kafka → Bronze (MinIO) в формате Parquet
Только запись сырых данных, без агрегаций.

Запуск:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
      src/streaming/stream_processor.py
"""
import time
import sys
sys.path.append('/app')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


class StreamingProcessor:
    """Запись потока Kafka в Bronze слой (MinIO, Parquet)"""
    
    def __init__(self):
        self.spark = None
        self.queries = []
    
    def create_spark_session(self):
        """Создание Spark сессии для стриминга"""
        self.spark = (SparkSession.builder
            .appName("Kafka-To-Bronze")
            # MinIO
            .config("spark.hadoop.fs.s3a.endpoint", "http://172.22.0.8:9302")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            # HDFS
            .config("spark.hadoop.fs.defaultFS", "hdfs://172.22.0.2:9000")
            # Streaming
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
            .config("spark.sql.streaming.minBatchesToRetain", "5")
            .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        
        self.spark.sparkContext.setLogLevel("WARN")
        print(f"✓ Spark Streaming initialized (master: {self.spark.sparkContext.master})")
    
    def read_from_kafka(self, topic):
        """Чтение потока из Kafka топика"""
        return (self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "shkafka:29092")
            .option("subscribe", topic)
            .option("group.id", "bronze-streaming-group")
            .option("startingOffsets", "latest")
            .option("maxOffsetsPerTrigger", "50000")
            .option("failOnDataLoss", "false")
            .load()
        )
    
    def process_user_events(self):
        """
        Пользовательские события: Kafka → Bronze (Parquet)
        """
        print("\n📡 [1/2] User Events: Kafka → Bronze (Parquet)")
        
        # Чтение из Kafka
        kafka_stream = self.read_from_kafka("ecommerce.user.events")
        
        # Схема JSON
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
        
        # Парсинг JSON + партиционирование
        parsed = kafka_stream \
            .select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*") \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("event_timestamp", to_timestamp("event_timestamp")) \
            .withColumn("year", year(col("event_timestamp"))) \
            .withColumn("month", month(col("event_timestamp"))) \
            .withColumn("day", dayofmonth(col("event_timestamp"))) #\
            #.repartition(1)
        parsed = parsed.repartition(2, "year", "month", "day")
        # Запись в Bronze (Parquet + Snappy)
        query = (parsed.writeStream
            .format("parquet")
            .option("path", "s3a://bronze/events/")
            #.option("path", "hdfs://172.22.0.2:9000/data/bronze/events/")
            .option("checkpointLocation", "/tmp/spark-checkpoints/bronze-events")
            #.option("checkpointLocation", "hdfs://172.22.0.2:9000/spark-checkpoints/bronze-events")
            .option("compression", "snappy")
            .partitionBy("year", "month", "day")
            #.trigger(processingTime="60 seconds")
            .trigger(processingTime="5 minutes")
            .outputMode("append")
            .queryName(f"Bronze-Events")
            .start()
        )
        
        self.queries.append(query)
        return parsed
    
    def process_orders(self):
        """
        Заказы: Kafka → Bronze (Parquet)
        """
        print("📡 [2/2] Orders: Kafka → Bronze (Parquet)")
        
        kafka_stream = self.read_from_kafka("ecommerce.orders")
        
        schema = StructType([
            StructField("order_id", StringType()),
            StructField("user_id", StringType()),
            StructField("username", StringType()),
            StructField("session_id", StringType()),
            StructField("order_timestamp", StringType()),
            StructField("event_type", StringType()),
            StructField("items_count", IntegerType()),
            StructField("total_amount", DoubleType()),
            StructField("discount_applied", DoubleType()),
            StructField("shipping_cost", DoubleType()),
            StructField("payment_method", StringType()),
            StructField("order_status", StringType()),
            StructField("country", StringType()),
            StructField("items", ArrayType(StructType([
                StructField("product_id", StringType()),
                StructField("product_name", StringType()),
                StructField("price", DoubleType()),
                StructField("quantity", IntegerType())
            ])))
        ])
        
        parsed = kafka_stream \
            .select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*") \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("order_timestamp", to_timestamp("order_timestamp")) \
            .withColumn("net_amount",
                col("total_amount") - (col("total_amount") * col("discount_applied")) + col("shipping_cost")) \
            .withColumn("year", year(col("order_timestamp"))) \
            .withColumn("month", month(col("order_timestamp")))
        parsed = parsed.repartition(2, "year", "month", "day")
        query = (parsed.writeStream
            .format("parquet")
            # .option("path", "s3a://bronze/orders/")
            # .option("checkpointLocation", "/tmp/spark-checkpoints/bronze-orders")
            .option("path", "hdfs://172.22.0.2:9000/data/bronze/orders/")
            .option("checkpointLocation", "hdfs://172.22.0.2:9000/spark-checkpoints/bronze-orders")
            .option("compression", "snappy")
            .partitionBy("year", "month")
            #.trigger(processingTime="60 seconds")
            .trigger(processingTime="5 minutes")
            .outputMode("append")
            .queryName("Bronze-Orders")
            .start()
        )
        
        self.queries.append(query)
        return parsed
    
    def run(self):
        """Запуск стриминга"""
        print("=" * 60)
        print("🚀 SPARK STREAMING: Kafka → Bronze (Parquet)")
        print("=" * 60)
        
        self.create_spark_session()
        
        # Запускаем обработку топиков
        self.process_user_events()
        #self.process_orders()
        
        print(f"\n✓ Started {len(self.queries)} streaming queries")
        print("  Writing to:")
        print("    s3a://bronze/events/   (partitioned by date)")
        print("    s3a://bronze/orders/   (partitioned by month)")
        print("\nPress Ctrl+C to stop...\n")
        
        try:
            for query in self.queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            print("\n⏹️  Stopping...")
            for query in self.queries:
                query.stop()
            print(f"✓ {len(self.queries)} queries stopped")
        
        self.spark.stop()


if __name__ == "__main__":
    processor = StreamingProcessor()
    processor.run()