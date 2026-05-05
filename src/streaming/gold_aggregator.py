from pyspark.sql.functions import *
from pyspark import StorageLevel
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GoldAggregator:
    """
    Gold Layer
    """

    def __init__(self, spark, mode="incremental", events_df=None, sessions_df=None):
        self.spark = spark
        self.mode = mode
        self.events_df = events_df
        self.sessions_df = sessions_df

        self.gold_bucket = "gold"
        self.hdfs_base = "hdfs://172.22.0.2:9000"

        self.silver_events_path = f"{self.hdfs_base}/data/silver/events"
        self.silver_sessions_path = f"{self.hdfs_base}/data/silver/sessions"

    def create_all_views(self):
        logger.info("=" * 60)
        logger.info("GOLD LAYER")
        logger.info("=" * 60)

        if self.events_df is None:
            logger.info("📥 Reading events from Silver...")
            self.events_df = self.spark.read.parquet(
                self.silver_events_path
            )

        if self.sessions_df is None:
            try:
                logger.info("📥 Reading sessions from Silver...")
                self.sessions_df = self.spark.read.parquet(
                    self.silver_sessions_path
                )
            except Exception:
                self.sessions_df = None

        self.events_df = self.events_df.persist(StorageLevel.MEMORY_AND_DISK)

        self._create_hourly_metrics(self.events_df)
        self._create_user_activity(self.events_df)
        self._create_product_analytics(self.events_df)

        if self.sessions_df is not None:
            self.sessions_df = self.sessions_df.persist(StorageLevel.MEMORY_AND_DISK)
            self._create_session_analytics(self.sessions_df)

    def _save_view(self, df, view_name, partitions=None):
        hdfs_path = f"{self.hdfs_base}/data/gold/{view_name}"
        
        # Удаляем старые данные
        try:
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jsc.hadoopConfiguration()
            )
            fs.delete(
                self.spark._jvm.org.apache.hadoop.fs.Path(hdfs_path), 
                True  
            )
        except:
            pass
        
        writer = df.write.mode("overwrite").option("compression", "snappy")
        if partitions:
            writer = writer.partitionBy(*partitions)
        writer.parquet(hdfs_path)

    def _create_hourly_metrics(self, df):
        logger.info("📊 hourly_metrics")

        hourly = (
            df.groupBy(
                "event_year",
                "event_month",
                "event_date",
                "event_hour"
            )
            .agg(
                count("*").alias("total_events"),
                countDistinct("user_id").alias("unique_users"),
                countDistinct("session_id").alias("unique_sessions")
            )
            .withColumn(
                "avg_events_per_user",
                round(col("total_events") / col("unique_users"), 2)
            )
        )

        self._save_view(
            hourly,
            "hourly_metrics",
            ["event_year", "event_month"]
        )

    def _create_user_activity(self, df):
        logger.info("👥 user_activity")

        users = (
            df.groupBy("user_id")
            .agg(
                countDistinct("session_id").alias("sessions"),
                count("*").alias("events"),
                min("event_timestamp").alias("first_seen"),
                max("event_timestamp").alias("last_seen")
            )
            .withColumn(
                "active_days",
                datediff(col("last_seen"), col("first_seen"))
            )
        )

        self._save_view(users, "user_activity")

    def _create_product_analytics(self, df):
        logger.info("📦 product_analytics")

        products = (
            df.filter(col("product_id").isNotNull())
            .groupBy("product_id", "product_name", "category")
            .agg(
                count("*").alias("interactions"),
                countDistinct("user_id").alias("unique_users"),
                avg("price").alias("avg_price")
            )
        )

        self._save_view(products, "product_analytics")

    def _create_session_analytics(self, sessions_df):
        logger.info("📈 session_analytics")

        sessions = (
            sessions_df.groupBy("year", "month")
            .agg(
                count("*").alias("sessions"),
                avg("total_events").alias("avg_events_per_session")
            )
        )

        self._save_view(
            sessions,
            "session_analytics",
            ["year", "month"]
        )