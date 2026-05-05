#!/usr/bin/env python3
"""
Spark Structured Streaming: Обработка потока из Kafka → MinIO + HDFS
Запуск: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
        src/streaming/stream_processor.py
"""

import sys
sys.path.append('/app')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

class StreamingProcessor:
    """Обработка потоковых данных из Kafka"""
    
    def __init__(self):
        self.spark = None
        self.queries = []
    
    def create_spark_session(self):
        """Создание Spark сессии для стриминга"""
        self.spark = (SparkSession.builder
            .appName("Kafka-Stream-Processor")
            .master("spark://172.22.0.4:7077")
            .config("spark.jars", 
                    "/jars/hadoop-aws-3.3.4.jar,/jars/aws-java-sdk-bundle-1.12.262.jar")
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
            # MinIO
            .config("spark.hadoop.fs.s3a.endpoint", "http://172.22.0.8:9302")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            # HDFS
            .config("spark.hadoop.fs.defaultFS", "hdfs://172.22.0.2:9000")
            # Streaming оптимизация
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
            .config("spark.sql.streaming.minBatchesToRetain", "5")
            .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )
        
        self.spark.sparkContext.setLogLevel("WARN")
        print("✓ Spark Streaming initialized")
    
    def read_from_kafka(self, topic):
        """Чтение потока из Kafka топика"""
        return (self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "shkafka:29092")
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("maxOffsetsPerTrigger", "10000")
            .option("failOnDataLoss", "false")
            .load()
        )
    
    def process_user_events(self):
        """Обработка потока пользовательских событий"""
        print("\n📡 Starting: User Events Stream Processing")
        
        # Чтение из Kafka
        kafka_stream = self.read_from_kafka("ecommerce.user.events")
        
        # Парсинг JSON
        schema = StructType([
            StructField("event_id", StringType()),
            StructField("session_id", StringType()),
            StructField("user_id", StringType()),
            StructField("username", StringType()),
            StructField("event_timestamp", StringType()),
            StructField("action", StringType()),
            StructField("product_id", StringType()),
            StructField("product_name", StringType()),
            StructField("category", StringType()),
            StructField("price", DoubleType()),
            StructField("device", StringType()),
            StructField("browser", StringType()),
            StructField("country", StringType()),
            StructField("membership_level", StringType()),
            StructField("session_duration_sec", IntegerType())
        ])
        
        parsed_stream = kafka_stream \
            .select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*") \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("event_timestamp", to_timestamp("event_timestamp")) \
            .withColumn("year", year(col("event_timestamp"))) \
            .withColumn("month", month(col("event_timestamp"))) \
            .withColumn("day", dayofmonth(col("event_timestamp"))) \
            .withColumn("hour", hour(col("event_timestamp")))
        
        # 1. Сохранение сырых данных в Bronze (MinIO)
        bronze_query = (parsed_stream.writeStream
            .format("parquet")
            .option("path", "s3a://bronze/streaming/events/")
            .option("checkpointLocation", "/tmp/spark-checkpoints/bronze-events")
            .partitionBy("year", "month", "day")
            .trigger(processingTime="30 seconds")
            .outputMode("append")
            .queryName("Bronze-User-Events")
            .start()
        )
        
        # 2. Агрегация каждую минуту
        aggregated_stream = parsed_stream \
            .withWatermark("event_timestamp", "5 minutes") \
            .groupBy(
                window("event_timestamp", "1 minute"),
                "action",
                "device",
                "country"
            ) \
            .agg(
                count("*").alias("event_count"),
                countDistinct("user_id").alias("unique_users"),
                countDistinct("session_id").alias("unique_sessions"),
                avg("session_duration_sec").alias("avg_session_duration"),
                sum(when(col("action") == "purchase", 1).otherwise(0)).alias("purchases")
            ) \
            .withColumn("processing_time", current_timestamp())
        
        # 3. Сохранение агрегаций
        agg_query = (aggregated_stream.writeStream
            .format("parquet")
            .option("path", "s3a://silver/streaming/aggregations/")
            .option("checkpointLocation", "/tmp/spark-checkpoints/silver-aggs")
            .trigger(processingTime="1 minute")
            .outputMode("append")
            .queryName("Silver-Aggregations")
            .start()
        )
        
        self.queries.extend([bronze_query, agg_query])
        
        # Вывод в консоль для демонстрации
        console_query = (aggregated_stream.writeStream
            .format("console")
            .option("numRows", "10")
            .option("truncate", "false")
            .trigger(processingTime="1 minute")
            .outputMode("complete")
            .queryName("Console-Output")
            .start()
        )
        
        self.queries.append(console_query)
        
        return parsed_stream
    
    def process_orders(self):
        """Обработка потока заказов"""
        print("\n📡 Starting: Orders Stream Processing")
        
        kafka_stream = self.read_from_kafka("ecommerce.orders")
        
        schema = StructType([
            StructField("order_id", StringType()),
            StructField("user_id", StringType()),
            StructField("username", StringType()),
            StructField("session_id", StringType()),
            StructField("order_timestamp", StringType()),
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
        
        parsed_orders = kafka_stream \
            .select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*") \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("order_timestamp", to_timestamp("order_timestamp")) \
            .withColumn("net_amount", 
                col("total_amount") - (col("total_amount") * col("discount_applied")) + col("shipping_cost"))
        
        # Сохранение заказов
        orders_query = (parsed_orders.writeStream
            .format("parquet")
            .option("path", "s3a://bronze/streaming/orders/")
            .option("checkpointLocation", "/tmp/spark-checkpoints/orders")
            .trigger(processingTime="30 seconds")
            .outputMode("append")
            .queryName("Bronze-Orders")
            .start()
        )
        
        self.queries.append(orders_query)
        return parsed_orders
    
    def real_time_metrics(self, events_stream, orders_stream):
        """Объединение потоков для бизнес-метрик"""
        print("\n📊 Starting: Real-Time Business Metrics")
        
        # Метрики каждые 2 минуты
        metrics = events_stream \
            .withWatermark("event_timestamp", "5 minutes") \
            .groupBy(window("event_timestamp", "2 minutes")) \
            .agg(
                count("*").alias("total_events"),
                countDistinct("user_id").alias("active_users"),
                countDistinct("session_id").alias("active_sessions"),
                sum(when(col("action") == "add_to_cart", 1).otherwise(0)).alias("cart_adds"),
                sum(when(col("action") == "purchase", 1).otherwise(0)).alias("purchases")
            ) \
            .withColumn("conversion_rate", 
                round(col("purchases") / col("active_sessions") * 100, 2))
        
        metrics_query = (metrics.writeStream
            .format("parquet")
            .option("path", "s3a://gold/streaming/realtime_metrics/")
            .option("checkpointLocation", "/tmp/spark-checkpoints/metrics")
            .trigger(processingTime="2 minutes")
            .outputMode("append")
            .queryName("Gold-Realtime-Metrics")
            .start()
        )
        
        self.queries.append(metrics_query)
    
    def run(self):
        """Запуск всей потоковой обработки"""
        print("=" * 60)
        print("🚀 SPARK STREAMING PROCESSOR")
        print("=" * 60)
        
        self.create_spark_session()
        
        # Запускаем обработку разных топиков
        events_stream = self.process_user_events()
        orders_stream = self.process_orders()
        
        # Real-time метрики
        self.real_time_metrics(events_stream, orders_stream)
        
        print(f"\n✓ Started {len(self.queries)} streaming queries")
        print("Press Ctrl+C to stop...\n")
        
        try:
            # Ожидание завершения всех запросов
            for query in self.queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            print("\n⏹️  Stopping streaming queries...")
            for query in self.queries:
                query.stop()
            print("✓ All queries stopped")
        
        self.spark.stop()


if __name__ == "__main__":
    processor = StreamingProcessor()
    processor.run()