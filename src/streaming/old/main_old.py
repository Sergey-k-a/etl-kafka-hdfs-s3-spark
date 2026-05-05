#!/usr/bin/env python3
"""
FULL ETL Pipeline: Bronze → Silver → Gold
"""
import sys, os, time
sys.path.append('/app')

from src.streaming.silver_processor_old import SilverProcessor
from src.streaming.gold_aggregator_old import GoldAggregator
from utils.spark_config import create_spark_session
from pyspark.sql import SparkSession
from datetime import datetime, time, timedelta
from pyspark.sql.functions import col, to_date, lit



def main(format_file = "parquet"):
    print("=" * 60)
    print("DATA LAKEHOUSE ETL: Silver & Gold Layers")
    print("=" * 60)
    
    spark = create_spark_session("ETL-Silver-Gold")

    spark.sparkContext.setLogLevel("WARN")
    
    # Проверяем данные в Bronze
    print("\n📊 Checking Bronze layer...")

    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    today = datetime.now().strftime("%Y-%m-%d")

    print(datetime.now())
    if format_file == "parquet":
        bronze_events = spark.read.format(format_file).load("s3a://bronze/events/")
        # bronze_events = spark.read.format(format_file) \
        #                 .load("s3a://bronze/events/") \
        #                 .filter(to_date(col("event_timestamp")) >= lit(week_ago))
        bronze_events.limit(10).show()
        # bronze_events = spark.read.format(format_file) \
        #                 .load("s3a://bronze/events/") \
        #                 .filter(to_date("event_timestamp") == lit(today))

        print(f"   Bronze events available: {bronze_events.count():,}")
    else:
        bronze_events = spark.read.format(format_file).load("s3a://bronze/topics/ecommerce.user.events/")
        # print(f"   Bronze events available: {bronze_events.count():,}")
        bronze_events.limit(10).show()
    print(datetime.now())

    if bronze_events.count() == 0:
        print("❌ No data in Bronze! Start Kafka producer first.")
        spark.stop()
        return
    
    # SILVER
    print("\n" + "🔸" * 30)
    processor = SilverProcessor(spark, mode="incremental", bronze_format=format_file)
    enriched_events, sessions = processor.process_all()
    
    # GOLD
    print("\n" + "🔷" * 30)
    aggregator = GoldAggregator(spark, mode="incremental")
    aggregator.create_all_views()
    
    # ФИНАЛ
    print("\n" + "=" * 60)
    print("✅ ETL PIPELINE COMPLETE")
    print("=" * 60)
    print("""
💾 Data Locations:
  Bronze (raw):      s3a://bronze/topics/ecommerce.user.events/
  Silver (clean):    hdfs://172.22.0.2:9000/data/silver/
  Gold (analytics):  s3a://gold/

🌐 Monitor:
  MinIO: http://localhost:9006
  HDFS:  http://localhost:9870
""")
    
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        format_file = sys.argv[1]  # Из командной строки
    else:
        format_file = os.getenv("BRONZE_FORMAT", "parquet")  # Из env или default
    
    print(f"Bronze format: {format_file}")
    main(format_file)