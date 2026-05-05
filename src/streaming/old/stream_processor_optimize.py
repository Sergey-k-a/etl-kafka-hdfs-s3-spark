#!/usr/bin/env python3
"""
Spark Structured Streaming: Kafka → Bronze (MinIO) — ОПТИМИЗИРОВАННАЯ ВЕРСИЯ
"""

import sys
sys.path.append('/app')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


class StreamingProcessor:
    """Запись потока Kafka в Bronze слой (MinIO, оптимизированный Parquet)"""
    
    def __init__(self):
        self.spark = None
        self.queries = []
    
    def create_spark_session(self):
        """Создание Spark сессии с оптимизациями"""
        self.spark = (SparkSession.builder
            .appName("Kafka-To-Bronze-Optimized")
            # MinIO
            .config("spark.hadoop.fs.s3a.endpoint", "http://172.22.0.8:9302")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            # Оптимизация малых файлов
            .config("spark.sql.files.openCostInBytes", "134217728")  # 128 MB
            .config("spark.sql.files.maxPartitionBytes", "268435456")  # 256 MB
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            # Streaming
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
            .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )
        
        self.spark.sparkContext.setLogLevel("WARN")
        print(f"✓ Spark Streaming initialized")
    
    def read_from_kafka(self, topic):
        """Чтение потока из Kafka"""
        return (self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "shkafka:29092")
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("maxOffsetsPerTrigger", "50000")
            .option("failOnDataLoss", "false")
            .load()
        )
    
    def process_user_events(self):
        """Пользовательские события с оптимизацией размера файлов"""
        print("\n📡 User Events: Kafka → Bronze (оптимизированный Parquet)")
        
        kafka_stream = self.read_from_kafka("ecommerce.user.events")
        
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
        
        parsed = kafka_stream \
            .select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*") \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("event_timestamp", to_timestamp("event_timestamp")) \
            .withColumn("year", year(col("event_timestamp"))) \
            .withColumn("month", month(col("event_timestamp"))) \
            .withColumn("day", dayofmonth(col("event_timestamp")))
        
        # Функция записи внутри метода (правильные отступы)
        def write_batch(df, epoch_id):
            """Запись каждого микробатча с repartition"""
            count = df.count()
            # Исправлено: без конфликта с max()
            if count > 100000:
                num_partitions = count // 100000
            else:
                num_partitions = 1
            
            df.repartition(num_partitions).write \
                .mode("append") \
                .partitionBy("year", "month", "day") \
                .parquet("s3a://bronze/events/")
            
            print(f"   ✓ Batch {epoch_id}: {count:,} events → {num_partitions} файлов")
        
        query = (parsed.writeStream
            .foreachBatch(write_batch)
            .trigger(processingTime="5 minutes")
            .option("checkpointLocation", "/tmp/spark-checkpoints/bronze-events-v3")
            .outputMode("append")
            .queryName("Bronze-Events-Optimized")
            .start()
        )
        
        # ВАЖНО: Добавляем в список queries
        self.queries.append(query)
        return parsed
    
    def run(self):
        """Запуск стриминга"""
        print("=" * 60)
        print("🚀 OPTIMIZED STREAMING: Kafka → Bronze")
        print("=" * 60)
        
        self.create_spark_session()
        self.process_user_events()
        
        print(f"\n✓ Started {len(self.queries)} streaming queries")
        print("  Оптимизации:")
        print("    • Триггер: каждые 5 минут")
        print("    • Размер файла: ~100K записей")
        print("    • Динамический repartition")
        print("\nPress Ctrl+C to stop...\n")
        
        try:
            for query in self.queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            print("\n⏹️  Stopping...")
            for query in self.queries:
                query.stop()
        
        self.spark.stop()


# ============ COMPACTION JOB ============
def run_compaction():
    """Периодический compaction: объединение мелких файлов"""
    spark = (SparkSession.builder
        .appName("Parquet-Compaction")
        .config("spark.hadoop.fs.s3a.endpoint", "http://172.22.0.8:9302")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    
    print("🔄 Compaction: объединение мелких файлов...")
    
    df = spark.read.parquet("s3a://bronze/events/")
    count = df.count()
    
    df.coalesce(8).write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet("s3a://bronze/events/")
    
    print(f"   ✓ Compaction done: {count:,} записей → 8 партиций")
    
    spark.stop()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["stream", "compact"], default="stream")
    args = parser.parse_args()
    
    if args.mode == "compact":
        run_compaction()
    else:
        processor = StreamingProcessor()
        processor.run()