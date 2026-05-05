from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BronzeLoader:
    """Загрузка сырых данных в Bronze слой (MinIO/S3)"""
    
    def __init__(self, spark):
        self.spark = spark
        self.bronze_bucket = "bronze"
    
    def load_events(self, input_path):
        """Загрузка событий из временных файлов в Bronze"""
        logger.info("Loading events to Bronze layer...")
        
        # Чтение JSON файлов
        events_df = self.spark.read.json(input_path)
        
        # Добавление метаданных и партиционирования
        enriched_df = events_df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", input_file_name()) \
            .withColumn("year", year(to_date(col("event_timestamp")))) \
            .withColumn("month", month(to_date(col("event_timestamp")))) \
            .withColumn("day", dayofmonth(to_date(col("event_timestamp"))))
        
        # Сохранение в MinIO с партиционированием по дате
        output_path = f"s3a://{self.bronze_bucket}/events/"
        enriched_df.write \
            .mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .parquet(output_path)
        
        count = enriched_df.count()
        logger.info(f"✓ {count:,} events loaded to {output_path}")
        
        return enriched_df
    
    def load_products(self, products_df):
        """Загрузка справочника товаров в Bronze"""
        logger.info("Loading products to Bronze layer...")
        
        enriched_df = products_df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("data_source", lit("generator"))
        
        output_path = f"s3a://{self.bronze_bucket}/products/"
        enriched_df.write.mode("overwrite").parquet(output_path)
        
        count = enriched_df.count()
        logger.info(f"✓ {count:,} products loaded to {output_path}")
        
        return enriched_df
    
    def load_users(self, users_df):
        """Загрузка справочника пользователей в Bronze"""
        logger.info("Loading users to Bronze layer...")
        
        enriched_df = users_df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("data_source", lit("generator"))
        
        output_path = f"s3a://{self.bronze_bucket}/users/"
        enriched_df.write.mode("overwrite").parquet(output_path)
        
        count = enriched_df.count()
        logger.info(f"✓ {count:,} users loaded to {output_path}")
        
        return enriched_df
    
    def load_orders(self, orders_df):
        """Загрузка заказов в Bronze"""
        logger.info("Loading orders to Bronze layer...")
        
        enriched_df = orders_df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("year", year(to_date(col("order_timestamp")))) \
            .withColumn("month", month(to_date(col("order_timestamp"))))
        
        output_path = f"s3a://{self.bronze_bucket}/orders/"
        enriched_df.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(output_path)
        
        count = enriched_df.count()
        logger.info(f"✓ {count:,} orders loaded to {output_path}")
        
        return enriched_df