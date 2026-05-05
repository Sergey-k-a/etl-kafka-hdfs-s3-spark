from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SilverProcessor:
    """Обработка данных из Bronze в Silver слой (очистка, дедупликация, обогащение)"""
    
    def __init__(self, spark):
        self.spark = spark
        self.bronze_bucket = "bronze"
        self.silver_bucket = "silver"
        self.hdfs_base = "hdfs://172.22.0.2:9000"
    
    def process_events(self):
        """Полный цикл обработки событий: Bronze → Silver"""
        logger.info("=" * 50)
        logger.info("SILVER LAYER: Processing events")
        logger.info("=" * 50)
        
        # 1. Чтение из Bronze
        bronze_events = self._read_from_bronze()
        
        # 2. Очистка
        cleaned = self._clean_data(bronze_events)
        
        # 3. Дедупликация
        deduped = self._deduplicate(cleaned)
        
        # 4. Обогащение
        enriched = self._enrich_data(deduped)
        
        # 5. Сохранение
        self._save_to_silver(enriched)
        
        logger.info("✓ Silver layer processing complete")
        return enriched
    
    def _read_from_bronze(self):
        """Чтение сырых данных из Bronze слоя"""
        logger.info("📥 Reading from Bronze layer...")
        
        events_path = f"s3a://{self.bronze_bucket}/events/"
        df = self.spark.read.parquet(events_path)
        
        logger.info(f"   Loaded: {df.count():,} records")
        return df
    
    def _clean_data(self, df):
        """Очистка данных от NULL и некорректных значений"""
        logger.info("🧹 Cleaning data...")
        
        initial_count = df.count()
        
        cleaned = df \
            .filter(col("user_id").isNotNull()) \
            .filter(col("event_timestamp").isNotNull()) \
            .filter(col("action").isNotNull()) \
            .filter(col("session_id").isNotNull()) \
            .withColumn("event_timestamp", to_timestamp("event_timestamp")) \
            .filter(col("event_timestamp").isNotNull()) \
            .filter(year("event_timestamp") >= 2020) \
            .withColumn("action", lower(trim(col("action")))) \
            .withColumn("device", lower(trim(col("device")))) \
            .withColumn("browser", lower(trim(col("browser"))))
        
        removed = initial_count - cleaned.count()
        logger.info(f"   Records after cleaning: {cleaned.count():,} (removed {removed:,})")
        
        return cleaned
    
    def _deduplicate(self, df):
        """Удаление дубликатов"""
        logger.info("🔄 Removing duplicates...")
        
        initial_count = df.count()
        
        # Сначала удаляем полные дубликаты
        df = df.distinct()
        
        # Затем удаляем дубликаты по бизнес-ключу (оставляем последнюю запись)
        window_spec = Window.partitionBy(
            "session_id", "user_id", "event_timestamp", "action"
        ).orderBy(col("ingestion_timestamp").desc())
        
        deduped = df \
            .withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")
        
        removed = initial_count - deduped.count()
        logger.info(f"   Duplicates removed: {removed:,}")
        
        return deduped
    
    def _enrich_data(self, df):
        """Обогащение данных новыми признаками"""
        logger.info("✨ Enriching data...")
        
        enriched = df \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("event_date", to_date("event_timestamp")) \
            .withColumn("event_hour", hour("event_timestamp")) \
            .withColumn("event_dayofweek", dayofweek("event_timestamp")) \
            .withColumn("event_dayname", date_format("event_timestamp", "EEEE")) \
            .withColumn("event_weekofyear", weekofyear("event_timestamp")) \
            .withColumn("event_quarter", quarter("event_timestamp")) \
            .withColumn("is_weekend", 
                when(col("event_dayofweek").isin([1, 7]), True).otherwise(False)) \
            .withColumn("is_bot", 
                when(col("session_duration_sec") < 1, True).otherwise(False)) \
            .withColumn("has_product", 
                when(col("product_id").isNotNull(), True).otherwise(False)) \
            .withColumn("event_category",
                when(col("action").isin("page_view", "product_view"), "browsing")
                .when(col("action").isin("add_to_cart", "remove_from_cart"), "cart")
                .when(col("action").isin("checkout_start", "purchase"), "purchase")
                .when(col("action") == "search", "search")
                .when(col("action") == "review_write", "engagement")
                .otherwise("other")) \
            .withColumn("device_type",
                when(col("device") == "mobile", "mobile")
                .when(col("device") == "tablet", "tablet")
                .otherwise("desktop"))
        
        logger.info(f"   Added enrichment fields")
        return enriched
    
    def _save_to_silver(self, df):
        """Сохранение обработанных данных в Silver слой"""
        logger.info("💾 Saving to Silver layer...")
        
        # Сохранение в HDFS
        hdfs_path = f"{self.hdfs_base}/data/processed/events"
        df.write.mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .parquet(hdfs_path)
        logger.info(f"   ✓ HDFS: {hdfs_path}")
        
        # Сохранение в MinIO
        minio_path = f"s3a://{self.silver_bucket}/events/"
        df.write.mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .parquet(minio_path)
        logger.info(f"   ✓ MinIO: {minio_path}")
    
    def create_user_sessions(self, events_df):
        """Создание агрегированных пользовательских сессий"""
        logger.info("📊 Creating session aggregations...")
        
        sessions = events_df.groupBy("user_id", "session_id").agg(
            first("device_type").alias("device"),
            first("browser").alias("browser"),
            min("event_timestamp").alias("session_start"),
            max("event_timestamp").alias("session_end"),
            count("*").alias("total_events"),
            countDistinct("action").alias("unique_actions"),
            countDistinct("product_id").alias("unique_products"),
            sum(when(col("action") == "page_view", 1).otherwise(0)).alias("page_views"),
            sum(when(col("action") == "product_view", 1).otherwise(0)).alias("product_views"),
            sum(when(col("action") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
            sum(when(col("action") == "purchase", 1).otherwise(0)).alias("purchases_count"),
            sum(when(col("is_bot") == True, 1).otherwise(0)).alias("bot_events"),
            max("session_duration_sec").alias("session_duration_sec")
        ).withColumn("session_duration_min",
            round((unix_timestamp("session_end") - unix_timestamp("session_start")) / 60, 2)) \
         .withColumn("has_purchase",
            when(col("purchases_count") > 0, True).otherwise(False)) \
         .withColumn("engagement_score",
            col("page_views") * 1 +
            col("product_views") * 2 +
            col("add_to_cart_count") * 3 +
            col("purchases_count") * 5) \
         .withColumn("session_date", to_date("session_start")) \
         .withColumn("year", year("session_start")) \
         .withColumn("month", month("session_start")) \
         .withColumn("day", dayofmonth("session_start"))
        
        # Сохранение сессий
        hdfs_path = f"{self.hdfs_base}/data/processed/sessions"
        sessions.write.mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(hdfs_path)
        logger.info(f"   ✓ Sessions saved: {sessions.count():,} records")
        
        return sessions