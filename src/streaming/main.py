#!/usr/bin/env python3
"""
ETL Pipeline
"""

import sys
import os
sys.path.append('/app')

from src.streaming.silver_processor import SilverProcessor
from src.streaming.gold_aggregator import GoldAggregator
from utils.spark_config import create_spark_session


def main(format_file="parquet"):
    print("=" * 60)
    print("Silver & Gold Layers")
    print("=" * 60)

    spark = create_spark_session("ETL-Silver-Gold")
    spark.sparkContext.setLogLevel("WARN")

    print("\n📊 Checking Bronze layer...")

    bronze_path = (
        "s3a://bronze/events/"
        if format_file == "parquet"
        else "s3a://bronze/topics/ecommerce.user.events/"
    )

    bronze_events = spark.read.format(format_file).load(bronze_path)

    bronze_count = bronze_events.count()
    print(f"   Bronze events available: {bronze_count:,}")
    

    if bronze_count == 0:
        print("❌ No data in Bronze! Start Kafka producer first.")
        spark.stop()
        return

    # silver
    print("\n" + "🔸" * 30)
    processor = SilverProcessor(
        spark,
        mode="incremental",
        bronze_format=format_file
    )

    enriched_events, sessions = processor.process_all()

    if enriched_events is None:
        print("✅ No new data to process.")
        spark.stop()
        return

    # gold
    print("\n" + "🔷" * 30)

    aggregator = GoldAggregator(
        spark,
        mode="incremental",
        events_df=enriched_events,
        sessions_df=sessions
    )

    aggregator.create_all_views()

    print("\n" + "=" * 60)
    print("✅ ETL PIPELINE COMPLETE")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        format_file = sys.argv[1]
    else:
        format_file = os.getenv("BRONZE_FORMAT", "parquet")

    print(f"Bronze format: {format_file}")
    main(format_file)