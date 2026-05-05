from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoldAggregator:
    """Создание бизнес-витрин в Gold слое"""
    
    def __init__(self, spark):
        self.spark = spark
        self.gold_bucket = "gold"
        self.hdfs_base = "hdfs://172.22.0.2:9000"
    
    def create_hourly_metrics(self, events_df):
        """Витрина 1: Почасовые метрики активности"""
        logger.info("📊 Creating hourly activity metrics...")
        
        hourly = events_df.groupBy(
            "year", "month", "day", "event_hour"
        ).agg(
            countDistinct("user_id").alias("unique_users"),
            countDistinct("session_id").alias("unique_sessions"),
            count("*").alias("total_events"),
            sum(when(col("action") == "purchase", 1).otherwise(0)).alias("purchases"),
            sum(when(col("action") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart"),
            sum(when(col("is_bot") == True, 1).otherwise(0)).alias("bot_events"),
            sum(when(col("is_weekend") == True, 1).otherwise(0)).alias("weekend_events")
        ).withColumn("conversion_rate", 
            round(col("purchases") / col("unique_sessions") * 100, 2)) \
         .withColumn("bot_percentage", 
            round(col("bot_events") / col("total_events") * 100, 2)) \
         .withColumn("avg_events_per_user", 
            round(col("total_events") / col("unique_users"), 2))
        
        # Сохранение
        self._save_view(hourly, "hourly_metrics", ["year", "month"])
        
        logger.info(f"   ✓ Created: {hourly.count():,} hourly records")
        return hourly
    
    def create_product_analytics(self, events_df):
        """Витрина 2: Аналитика по продуктам"""
        logger.info("📦 Creating product analytics...")
        
        # Читаем справочник продуктов из Bronze
        products = self.spark.read.parquet("s3a://bronze/products/")
        
        analytics = events_df \
            .filter(col("product_id").isNotNull()) \
            .groupBy("product_id", "category") \
            .agg(
                count("*").alias("total_interactions"),
                sum(when(col("action") == "product_view", 1).otherwise(0)).alias("views"),
                sum(when(col("action") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart"),
                sum(when(col("action") == "purchase", 1).otherwise(0)).alias("purchases"),
                sum(when(col("action") == "remove_from_cart", 1).otherwise(0)).alias("removed_from_cart"),
                countDistinct("user_id").alias("unique_users"),
                countDistinct("session_id").alias("unique_sessions")
            ) \
            .withColumn("view_to_cart_rate", 
                round(col("add_to_cart") / col("views") * 100, 2)) \
            .withColumn("cart_to_purchase_rate", 
                round(col("purchases") / col("add_to_cart") * 100, 2)) \
            .withColumn("view_to_purchase_rate", 
                round(col("purchases") / col("views") * 100, 2)) \
            .withColumn("abandonment_rate", 
                round(col("removed_from_cart") / col("add_to_cart") * 100, 2)) \
            .withColumn("popularity_score",
                col("views") * 1 + 
                col("add_to_cart") * 3 + 
                col("purchases") * 5 - 
                col("removed_from_cart") * 2) \
            .join(products.select("product_id", "product_name", "price"), 
                  "product_id", "left")
        
        # Сохранение
        self._save_view(analytics, "product_analytics")
        
        logger.info(f"   ✓ Created: {analytics.count():,} product records")
        return analytics
    
    def create_user_segments(self, sessions_df, users_df=None):
        """Витрина 3: RFM сегментация пользователей"""
        logger.info("👥 Creating user segments (RFM analysis)...")
        
        # Если пользователи не переданы, читаем из Bronze
        if users_df is None:
            users_df = self.spark.read.parquet("s3a://bronze/users/")
        
        # Рассчитываем RFM метрики
        rfm = sessions_df.groupBy("user_id").agg(
            max("session_end").alias("last_activity"),
            countDistinct("session_id").alias("frequency"),
            sum("purchases_count").alias("monetary"),
            sum("total_events").alias("total_events"),
            avg("session_duration_min").alias("avg_session_duration"),
            sum("engagement_score").alias("total_engagement")
        )
        
        # Добавляем recency (дни с последней активности)
        current_date = lit("2024-12-31")
        rfm = rfm.withColumn("recency_days", 
            datediff(current_date, to_date("last_activity"))) \
            .filter(col("recency_days") >= 0)
        
        # RFM скоринг (квантили)
        rfm_scored = rfm \
            .withColumn("r_score", 
                ntile(4).over(Window.orderBy(col("recency_days").asc()))) \
            .withColumn("f_score", 
                ntile(4).over(Window.orderBy(col("frequency").desc()))) \
            .withColumn("m_score", 
                ntile(4).over(Window.orderBy(col("monetary").desc())))
        
        # Сегментация
        segments = rfm_scored.withColumn("segment",
            when((col("r_score") >= 3) & (col("f_score") >= 3) & (col("m_score") >= 3), "Champions")
            .when((col("r_score") >= 3) & (col("f_score") >= 2), "Loyal Customers")
            .when((col("r_score") >= 3) & (col("f_score") <= 2), "New Customers")
            .when((col("r_score") == 2) & (col("f_score") >= 2), "Potential Loyalists")
            .when((col("r_score") >= 3) & (col("f_score") <= 1), "Promising")
            .when((col("r_score") == 2) & (col("f_score") <= 2), "Needs Attention")
            .when((col("r_score") <= 1) & (col("f_score") >= 2), "At Risk")
            .when((col("r_score") <= 1) & (col("f_score") <= 1), "Lost")
            .otherwise("Unclassified")) \
            .join(users_df.select("user_id", "country", "membership_level"), 
                  "user_id", "left")
        
        # Сохранение
        self._save_view(segments, "user_segments")
        
        logger.info(f"   ✓ Created: {segments.count():,} user segments")
        return segments
    
    def _save_view(self, df, view_name, partitions=None):
        """Сохранение витрины в HDFS и MinIO"""
        # HDFS
        hdfs_path = f"{self.hdfs_base}/data/aggregated/{view_name}"
        writer_hdfs = df.write.mode("overwrite")
        if partitions:
            writer_hdfs = writer_hdfs.partitionBy(*partitions)
        writer_hdfs.parquet(hdfs_path)
        
        # MinIO
        minio_path = f"s3a://{self.gold_bucket}/{view_name}/"
        writer_minio = df.write.mode("overwrite")
        if partitions:
            writer_minio = writer_minio.partitionBy(*partitions)
        writer_minio.parquet(minio_path)
        
        logger.info(f"     💾 {view_name} → HDFS + MinIO")