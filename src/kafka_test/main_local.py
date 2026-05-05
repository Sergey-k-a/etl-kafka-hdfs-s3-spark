#!/usr/bin/env python3
"""
ETL Pipeline - ЛОКАЛЬНЫЙ ЗАПУСК (без кластера)
Не требует воркеров, работает в одном контейнере.
"""

import sys
import os
import time
sys.path.append('/app')

from pyspark.sql import SparkSession
from streaming.silver_processor_incremental import SilverProcessor
from src.streaming.gold_aggregator_old import GoldAggregator

def create_local_spark():
    """Создание локальной Spark сессии"""
    spark = (SparkSession.builder
        .appName("ETL-Local-Mode")
        .master("local[2]")  # ← ЛОКАЛЬНЫЙ РЕЖИМ, 2 потока
        .config("spark.driver.memory", "2g")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.ui.port", "4040")
        # JAR файлы
        .config("spark.jars", 
                "/jars/hadoop-aws-3.3.4.jar,/jars/aws-java-sdk-bundle-1.12.262.jar")
        # MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://172.22.0.8:9302")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # HDFS
        .config("spark.hadoop.fs.defaultFS", "hdfs://172.22.0.2:9000")
        # Оптимизация
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    print("=" * 60)
    print("DATA LAKEHOUSE ETL: Silver & Gold Layers")
    print("Mode: LOCAL (no cluster required)")
    print("=" * 60)
    
    start_time = time.time()
    
    # Создать локальный Spark
    spark = create_local_spark()
    print(f"✓ Spark {spark.version} initialized (local mode)")
    print(f"  Master: {spark.sparkContext.master}")
    print(f"  Cores: {spark.sparkContext.defaultParallelism}")
    
    # Проверить данные в Bronze
    print("\n📊 Checking Bronze layer...")
    try:
        bronze_events = spark.read.json("s3a://bronze/topics/ecommerce.user.events/")
        bronze_count = bronze_events.count()
        print(f"   Bronze events available: {bronze_count:,}")
        
        if bronze_count == 0:
            print("❌ No data in Bronze! Start Kafka producer first.")
            spark.stop()
            return
        
        # Показать пример
        print("\n   Sample data:")
        bronze_events.select(
            "event_timestamp", "user_id", "action", "device", "country"
        ).show(5, truncate=False)
        
    except Exception as e:
        print(f"❌ Cannot read Bronze: {e}")
        spark.stop()
        return
    
    # SILVER LAYER
    print("\n" + "=" * 60)
    print("SILVER LAYER PROCESSING")
    print("=" * 60)
    
    step_start = time.time()
    processor = SilverProcessor(spark, mode="incremental")
    enriched_events, sessions = processor.process_all()
    silver_time = time.time() - step_start
    
    if enriched_events is None:
        print("❌ Silver processing failed or no new data")
        spark.stop()
        return
    
    # GOLD LAYER
    print("\n" + "=" * 60)
    print("GOLD LAYER PROCESSING")
    print("=" * 60)
    
    step_start = time.time()
    aggregator = GoldAggregator(spark, mode="full")
    stats = aggregator.create_all_views()
    gold_time = time.time() - step_start
    
    # ИТОГИ
    total_time = time.time() - start_time
    
    print("\n" + "=" * 60)
    print("✅ ETL PIPELINE COMPLETED")
    print("=" * 60)
    print(f"""
⏱ Timing:
  ├── Silver layer: {silver_time:.1f}s
  ├── Gold layer:   {gold_time:.1f}s
  └── Total:        {total_time:.1f}s

📊 Statistics:
  ├── Bronze events:  {bronze_count:,}
  ├── Silver events:  {enriched_events.count():,}
  └── Gold views:     {len(stats)} views created

💾 Storage:
  ├── Bronze: s3a://bronze/
  ├── Silver: hdfs://172.22.0.2:9000/data/silver/
  ├── Silver: s3a://silver/
  ├── Gold:   hdfs://172.22.0.2:9000/data/gold/
  └── Gold:   s3a://gold/

🌐 Monitor:
  ├── MinIO:   http://localhost:9006
  ├── HDFS:    http://localhost:9870
  └── Spark UI: http://localhost:4040
""")
    
    spark.stop()
    print("✓ Spark session closed")


if __name__ == "__main__":
    main()