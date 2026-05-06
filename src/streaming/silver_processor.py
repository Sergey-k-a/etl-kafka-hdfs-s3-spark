from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark import StorageLevel
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SilverProcessor:
    """
    Silver Processor
    """

    def __init__(self, spark, mode="incremental", hours_back=None, bronze_format="parquet"):
        self.spark = spark
        self.mode = mode
        self.hours_back = hours_back or 1
        self.bronze_format = bronze_format

        if bronze_format == "parquet":
            self.bronze_path = "s3a://bronze/events/"
        else:
            self.bronze_path = "s3a://bronze/topics/ecommerce.user.events/"

        self.silver_bucket = "silver"
        self.hdfs_base = "hdfs://172.22.0.2:9000"
        self.checkpoint_dir = f"{self.hdfs_base}/etl_checkpoint"


    def _read_bronze(self):
        if self.bronze_format == "parquet":
            return self.spark.read.parquet(self.bronze_path)
        elif self.bronze_format == "json":
            return self.spark.read.json(self.bronze_path)
        else:
            raise ValueError(f"Unknown format: {self.bronze_format}")

    def _get_last_processed_date(self):
        try:
            checkpoint_file = f"{self.checkpoint_dir}/last_processed_date.txt"
            df = self.spark.read.text(checkpoint_file)
            return df.collect()[0][0].strip()
        except Exception:
            return "2024-01-01"

    def _read_incremental(self):
        last_date = self._get_last_processed_date()
        logger.info(f"📥 Incremental read since {last_date}")

        if self.bronze_format != "parquet":
            return self._read_bronze().filter(
                to_date(col("event_timestamp")) > lit(last_date)
            )

        last_dt = datetime.strptime(last_date, "%Y-%m-%d")

        return (
            self._read_bronze()
            .filter(
                (col("year") > last_dt.year) |
                (
                    (col("year") == last_dt.year) &
                    (col("month") > last_dt.month)
                ) |
                (
                    (col("year") == last_dt.year) &
                    (col("month") == last_dt.month) &
                    (col("day") > last_dt.day)
                )
            )
        )

    
    def process_all(self):
        logger.info("=" * 60)
        logger.info(f"SILVER PROCESSOR ({self.mode})")
        logger.info("=" * 60)

        if self.mode == "incremental":
            df = self._read_incremental()
        else:
            df = self._read_bronze()

        df = df.persist(StorageLevel.MEMORY_AND_DISK)

        bronze_count = df.count()

        if bronze_count == 0:
            logger.info("No new data.")
            return None, None

        logger.info(f"Bronze rows: {bronze_count:,}")

        cleaned = self._clean_data(df)
        deduplicate = self._deduplicate(cleaned)
        enriched = self._enrich_data(deduplicate).persist(StorageLevel.MEMORY_AND_DISK)

        self._save_to_silver(enriched)

        sessions = self._create_session_aggregates(enriched)

        max_date = enriched.agg(max("event_date")).collect()[0][0]
        if max_date:
            self._save_checkpoint(str(max_date))

        return enriched, sessions

    def _clean_data(self, df):
        return (
            df
            .filter(col("event_id").isNotNull())
            .filter(col("user_id").isNotNull())
            .filter(col("event_timestamp").isNotNull())
            .filter(col("action").isNotNull())
            .filter(col("session_id").isNotNull())
            .withColumn("event_timestamp", to_timestamp("event_timestamp"))
            .filter(col("event_timestamp").isNotNull())
            .withColumn("action", lower(trim(col("action"))))
            .withColumn("device", lower(trim(col("device"))))
            .withColumn("browser", lower(trim(col("browser"))))
            .withColumn("country", initcap(trim(col("country"))))
        )
    
    def _deduplicate(self, df):
        window_spec = Window.partitionBy("event_id").orderBy(col("event_timestamp").desc())
        return (
            df
            .withColumn("_row_num", row_number().over(window_spec))
            .filter(col("_row_num") == 1)
            .drop("_row_num")
        )

    def _enrich_data(self, df):
        return (
            df
            .withColumn("event_date", to_date("event_timestamp"))
            .withColumn("event_hour", hour("event_timestamp"))
            .withColumn("event_month", month("event_timestamp"))
            .withColumn("event_year", year("event_timestamp"))
            .withColumn("year", year("event_timestamp"))
            .withColumn("month", month("event_timestamp"))
            .withColumn("day", dayofmonth("event_timestamp"))
            .withColumn("processing_timestamp", current_timestamp())
        )

    def _save_to_silver(self, df):
        write_mode = "overwrite" if self.mode == "full" else "append"

        (
            df
            .repartition(8, "year", "month", "day")
            .write
            .mode(write_mode)
            .partitionBy("year", "month", "day")
            .option("compression", "snappy")
            .parquet(f"{self.hdfs_base}/data/silver/events")
        )

    def _create_session_aggregates(self, events_df):
        sessions = (
            events_df
            .groupBy("user_id", "session_id")
            .agg(
                min("event_timestamp").alias("session_start"),
                max("event_timestamp").alias("session_end"),
                count("*").alias("total_events")
            )
            .withColumn("year", year("session_start"))
            .withColumn("month", month("session_start"))
        )

        write_mode = "overwrite" if self.mode == "full" else "append"

        (
            sessions
            .repartition(4, "year", "month")
            .write
            .mode(write_mode)
            .partitionBy("year", "month")
            .option("compression", "snappy")
            .parquet(f"{self.hdfs_base}/data/silver/sessions")
        )

        return sessions

    def _save_checkpoint(self, date_str):
        checkpoint_df = self.spark.createDataFrame(
            [(date_str,)], ["last_processed_date"]
        )

        checkpoint_df.coalesce(1).write.mode("overwrite").text(
            f"{self.checkpoint_dir}/last_processed_date.txt"
        )