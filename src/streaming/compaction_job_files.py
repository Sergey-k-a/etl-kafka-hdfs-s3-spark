# bronze_compaction.py
#!/usr/bin/env python3
"""
Bronze compaction job.
"""

import sys
import argparse
import logging
from datetime import datetime, timedelta

from pyspark.sql.functions import col, to_date, lit

sys.path.append("/app")
from utils.spark_config import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze_compaction")


class BronzeCompactor:
    def __init__(
        self,
        spark,
        bronze_path="s3a://bronze/events/",
        days_back=1,
        target_files=16,
    ):
        self.spark = spark
        self.bronze_path = bronze_path
        self.days_back = days_back
        self.target_files = target_files

    def _read_source(self):
        cutoff = (datetime.now() - timedelta(days=self.days_back)).strftime("%Y-%m-%d")

        logger.info("=" * 60)
        logger.info("Reading Bronze source")
        logger.info(f"Path: {self.bronze_path}")
        logger.info(f"Cutoff date: {cutoff}")
        logger.info("=" * 60)

        df = (
            self.spark.read
            .parquet(self.bronze_path)
            .filter(to_date(col("event_timestamp")) >= lit(cutoff))
        )

        count = df.count()
        logger.info(f"Loaded records: {count:,}")

        return df

    def compact(self):
        df = self._read_source()

        if df.rdd.isEmpty():
            logger.warning("No data found for compaction")
            return

        if "event_timestamp" not in df.columns:
            raise ValueError("Column event_timestamp is required")

        compacted = (
            df.withColumn("event_date", to_date(col("event_timestamp")))
              .repartition(self.target_files, "event_date")
        )

        temp_path = self.bronze_path.rstrip("/") + "_compacted_tmp"

        logger.info("=" * 60)
        logger.info("Writing compacted Bronze")
        logger.info(f"Temp path: {temp_path}")
        logger.info(f"Target files: {self.target_files}")
        logger.info("=" * 60)

        (
            compacted.write
            .mode("overwrite")
            .option("compression", "snappy")
            .partitionBy("event_date")
            .parquet(temp_path)
        )

        logger.info("Compaction write finished")
        logger.info("")
        logger.info("Next step:")
        logger.info("Manually validate temp output, then swap paths if OK")
        logger.info(f"  source: {self.bronze_path}")
        logger.info(f"  temp:   {temp_path}")

    def optimize_in_place(self):
        """
        Осторожный режим:
        перезапись в тот же Bronze path.
        Используй только если уверен, что upstream в этот момент не пишет.
        """
        df = self._read_source()

        if df.rdd.isEmpty():
            logger.warning("No data found for compaction")
            return

        compacted = (
            df.withColumn("event_date", to_date(col("event_timestamp")))
              .repartition(self.target_files, "event_date")
        )

        logger.warning("=" * 60)
        logger.warning("IN-PLACE COMPACTION ENABLED")
        logger.warning("This rewrites Bronze data in-place")
        logger.warning("=" * 60)

        (
            compacted.write
            .mode("overwrite")
            .option("compression", "snappy")
            .partitionBy("event_date")
            .parquet(self.bronze_path)
        )

        logger.info("In-place compaction finished")


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--bronze-path",
        default="s3a://bronze/events/",
        help="Bronze parquet path",
    )

    parser.add_argument(
        "--days-back",
        type=int,
        default=1,
        help="Compact only recent N days",
    )

    parser.add_argument(
        "--target-files",
        type=int,
        default=16,
        help="Number of output files",
    )

    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Rewrite Bronze in-place",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    spark = create_spark_session("Bronze-Compaction")
    spark.sparkContext.setLogLevel("WARN")

    compactor = BronzeCompactor(
        spark=spark,
        bronze_path=args.bronze_path,
        days_back=args.days_back,
        target_files=args.target_files,
    )

    try:
        if args.in_place:
            compactor.optimize_in_place()
        else:
            compactor.compact()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()