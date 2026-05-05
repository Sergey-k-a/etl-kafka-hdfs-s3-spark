from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SilverProcessor:
    """
    Обработка данных из Bronze в Silver слой.
    Поддерживает инкрементальную и полную загрузку.
    
    Режимы работы:
    - full: Полная загрузка всех данных
    - incremental: Только новые данные с момента последней обработки
    - today: Только данные за сегодня
    - last_hours: Данные за последние N часов
    """
    
    def __init__(self, spark, mode="incremental", hours_back=None, bronze_format="parquet"):
        """
        Args:
            spark: SparkSession
            mode: "full", "incremental", "today", "last_hours"
            hours_back: для mode="last_hours" - сколько часов назад читать
            bronze_format: "parquet" (Spark Streaming) или "json" (Kafka Connect)
        """
        self.spark = spark
        self.mode = mode
        self.hours_back = hours_back or 1
        self.bronze_format = bronze_format 
        
    
        # Пути зависят от формата
        if bronze_format == "parquet":
            self.bronze_path = "s3a://bronze/events/"          # Spark Streaming
        else:
            self.bronze_path = "s3a://bronze/topics/ecommerce.user.events/"  # Kafka Connect


        self.silver_bucket = "silver"
        self.hdfs_base = "hdfs://172.22.0.2:9000"
        self.checkpoint_dir = f"{self.hdfs_base}/etl_checkpoint"

        
        # Статистика
        self.stats = {
            'bronze_count': 0,
            'cleaned_count': 0,
            'duplicates_removed': 0,
            'silver_count': 0,
            'sessions_count': 0,
            'processing_time_sec': 0
        }
    
    def _read_bronze(self):
        """Чтение из Bronze с автоопределением формата"""
        logger.info(f"📥 Reading Bronze ({self.bronze_format})...")
        
        if self.bronze_format == "parquet":
            df = self.spark.read.parquet(self.bronze_path)
        elif self.bronze_format == "json":
            df = self.spark.read.json(self.bronze_path)
        else:
            raise ValueError(f"Unknown format: {self.bronze_format}")
        
        return df

    def process_all(self):
        """Основной метод обработки Bronze → Silver"""
        
        import time
        start_time = time.time()
        
        logger.info("=" * 60)
        logger.info(f"SILVER LAYER Processing (mode: {self.mode})")
        logger.info("=" * 60)
        
        # --------------------------------------------------
        # Шаг 1: Чтение из Bronze (в зависимости от режима)
        # --------------------------------------------------
        if self.mode == "full":
            df = self._read_full()
        elif self.mode == "incremental":
            df = self._read_incremental()
        elif self.mode == "today":
            df = self._read_today()
        elif self.mode == "last_hours":
            df = self._read_last_hours(self.hours_back)
        else:
            logger.warning(f"Unknown mode '{self.mode}', using 'full'")
            df = self._read_full()
        
        self.stats['bronze_count'] = df.count()
        
        if self.stats['bronze_count'] == 0:
            logger.info("=" * 60)
            logger.info("✅ No new data to process")
            logger.info("=" * 60)
            return None, None
        
        # --------------------------------------------------
        # Шаг 2: Очистка
        # --------------------------------------------------
        cleaned = self._clean_data(df)
        self.stats['cleaned_count'] = cleaned.count()
        self.stats['duplicates_removed'] = self.stats['bronze_count'] - self.stats['cleaned_count']
        
        # --------------------------------------------------
        # Шаг 3: Дедупликация
        # --------------------------------------------------
        deduped = self._deduplicate(cleaned)
        
        # --------------------------------------------------
        # Шаг 4: Обогащение
        # --------------------------------------------------
        enriched = self._enrich_data(deduped)
        self.stats['silver_count'] = enriched.count()
        
        # --------------------------------------------------
        # Шаг 5: Сохранение в Silver
        # --------------------------------------------------
        self._save_to_silver(enriched)
        
        # --------------------------------------------------
        # Шаг 6: Создание сессий
        # --------------------------------------------------
        sessions = self._create_session_aggregates(enriched)
        self.stats['sessions_count'] = sessions.count() if sessions else 0
        
        # --------------------------------------------------
        # Шаг 7: Сохранение чекпоинта
        # --------------------------------------------------
        if self.mode in ("incremental", "today", "last_hours"):
            max_date = enriched.agg(max("event_date")).collect()[0][0]
            if max_date:
                self._save_checkpoint(str(max_date))
                logger.info(f"   Checkpoint saved: {max_date}")
        
        # --------------------------------------------------
        # Финиш
        # --------------------------------------------------
        self.stats['processing_time_sec'] = time.time() - start_time
        self._print_stats()
        
        return enriched, sessions
    
    # ==============================================
    # МЕТОДЫ ЧТЕНИЯ ИЗ BRONZE
    # ==============================================
    
    def _read_full(self):
        """Полное чтение всех данных из Bronze"""
        logger.info("📥 [Full Read] Reading ALL data from Bronze...")
        
        df = self._read_bronze()  # ← автоформат
        df = df.cache()

        #df = self.spark.read.json(self.bronze_path)
        
        # count = df.count()
        # logger.info(f"   Loaded: {count:,} records")
        
        return df
    
    def _read_incremental(self):
        """Инкрементальное чтение: только новые данные"""
        logger.info("📥 [Incremental Read] Reading new data only...")
        
        # Получаем дату последней успешной обработки
        last_date = self._get_last_processed_date()
        logger.info(f"   Last processed date: {last_date}")
        
        # Читаем все файлы, но фильтруем по дате
        # Spark оптимизирует это через partition pruning
        # df = self._read_bronze().filter(to_date(col("event_timestamp")) > lit(last_date))  # автоформат
        # df = df.cache()

        df = self.spark.read.json(self.bronze_path) \
            .filter(to_date(col("event_timestamp")) > lit(last_date))
        
        count = df.count()
        logger.info(f"   New records since {last_date}: {count:,}")
        
        return df
    
    def _read_today(self):
        """Чтение только за сегодняшний день"""
        today = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"📥 [Today Read] Reading data for: {today}")
        
        # df = self.spark.read.json(self.bronze_path) \
        #     .filter(to_date(col("event_timestamp")) == lit(today))
        
        df = self._read_bronze().filter(to_date(col("event_timestamp")) == lit(today))  # автоформат
        df = df.cache()
        
        #count = df.count()
        #logger.info(f"   Today's records: {count:,}")
        
        return df
    
    def _read_last_hours(self, hours=1):
        """Чтение данных за последние N часов"""
        cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()
        logger.info(f"📥 [Last {hours}h Read] Reading data since: {cutoff}")
        
        # df = self.spark.read.json(self.bronze_path) \
        #     .filter(col("event_timestamp") >= lit(cutoff))
        
        df = self._read_bronze().filter(col("event_timestamp") >= lit(cutoff))
        df = df.cache()
        
        #count = df.count()
        #logger.info(f"   Records in last {hours}h: {count:,}")
        
        return df
    
    # ==============================================
    # УПРАВЛЕНИЕ ЧЕКПОИНТАМИ
    # ==============================================
    
    def _get_last_processed_date(self):
        """Чтение даты последней успешной обработки из HDFS"""
        try:
            checkpoint_file = f"{self.checkpoint_dir}/last_processed_date.txt"
            df = self.spark.read.text(checkpoint_file)
            last_date = df.collect()[0][0].strip()
            logger.info(f"   ✓ Checkpoint found: {last_date}")
            return last_date
        except Exception as e:
            logger.warning(f"   No checkpoint found ({str(e)[:50]}...)")
            logger.info("   Starting from default date: 2024-01-01")
            return "2024-01-01"
    
    def _save_checkpoint(self, date_str):
        """Сохранение даты последней обработки в HDFS"""
        try:
            checkpoint_df = self.spark.createDataFrame(
                [(date_str,)], ["last_processed_date"]
            )
            
            checkpoint_file = f"{self.checkpoint_dir}/last_processed_date.txt"
            checkpoint_df.coalesce(1).write.mode("overwrite").text(checkpoint_file)
            
            logger.info(f"   ✓ Checkpoint saved: {date_str}")
            return True
        except Exception as e:
            logger.error(f"   ✗ Failed to save checkpoint: {e}")
            return False
    
    def reset_checkpoint(self):
        """Сброс чекпоинта (для перезапуска с начала)"""
        try:
            checkpoint_file = f"{self.checkpoint_dir}/last_processed_date.txt"
            # Удаляем через HDFS API
            self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem \
                .get(self.spark.sparkContext._jsc.hadoopConfiguration()) \
                .delete(self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(checkpoint_file), True)
            logger.info("✓ Checkpoint reset successfully")
            return True
        except:
            logger.warning("No checkpoint to reset")
            return False
    
    # ==============================================
    # ОЧИСТКА ДАННЫХ
    # ==============================================
    
    def _clean_data(self, df):
        """Очистка сырых данных от Kafka Connect"""
        logger.info("\n🧹 Cleaning data...")
        
        initial_count = df.count()
        
        cleaned = df \
            .drop(*[c for c in df.columns if c in ('test', 'timestamp', '_corrupt_record')]) \
            .filter(col("event_id").isNotNull()) \
            .filter(col("user_id").isNotNull()) \
            .filter(col("event_timestamp").isNotNull()) \
            .filter(col("action").isNotNull()) \
            .filter(col("session_id").isNotNull()) \
            .withColumn("event_timestamp_parsed", to_timestamp("event_timestamp")) \
            .drop("event_timestamp") \
            .withColumnRenamed("event_timestamp_parsed", "event_timestamp") \
            .filter(col("event_timestamp").isNotNull()) \
            .filter(year("event_timestamp") >= 2020) \
            .filter(year("event_timestamp") <= 2030) \
            .withColumn("action", lower(trim(col("action")))) \
            .withColumn("device", lower(trim(col("device")))) \
            .withColumn("browser", lower(trim(col("browser")))) \
            .withColumn("country", initcap(trim(col("country")))) \
            .withColumn("page_url", lower(trim(col("page_url")))) \
            .withColumn("price", col("price").cast("double")) \
            .withColumn("session_duration_sec", col("session_duration_sec").cast("int"))
        
        removed = initial_count - cleaned.count()
        logger.info(f"   Initial: {initial_count:,}")
        logger.info(f"   After cleaning: {cleaned.count():,} (removed {removed:,})")
        
        # Статистика по NULL
        # null_after = {
        #     c: cleaned.filter(col(c).isNull()).count()
        #     for c in ["user_id", "session_id", "event_timestamp", "action", "event_id"]
        # }
        # for col_name, null_count in null_after.items():
        #     if null_count > 0:
        #         logger.warning(f"   ⚠ {col_name}: {null_count} NULLs remaining")
        
        return cleaned
    
    # ==============================================
    # ДЕДУПЛИКАЦИЯ
    # ==============================================
    
    def _deduplicate(self, df):
        """Удаление дубликатов"""
        logger.info("\n🔄 Removing duplicates...")
        
        #initial_count = df.count()
        
        # 1. Удаляем полные дубликаты
        distinct_df = df.distinct()
        #full_dupes = initial_count - distinct_df.count()
        
        # 2. Удаляем дубликаты по event_id (оставляем последнее событие)
        window_spec = Window.partitionBy("event_id") \
            .orderBy(col("event_timestamp").desc())
        
        deduped = distinct_df \
            .withColumn("_row_num", row_number().over(window_spec)) \
            .filter(col("_row_num") == 1) \
            .drop("_row_num")
        
        #total_removed = initial_count - deduped.count()
        
        #logger.info(f"   Full duplicates: {full_dupes:,}")
        #logger.info(f"   Event ID duplicates: {total_removed - full_dupes:,}")
        #logger.info(f"   After dedup: {deduped.count():,}")
        
        return deduped
    
    # ==============================================
    # ОБОГАЩЕНИЕ ДАННЫХ
    # ==============================================
    
    def _enrich_data(self, df):
        """Обогащение данных новыми признаками"""
        logger.info("\n✨ Enriching data...")
        
        enriched = df \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("event_date", to_date("event_timestamp")) \
            .withColumn("event_hour", hour("event_timestamp")) \
            .withColumn("event_minute", minute("event_timestamp")) \
            .withColumn("event_dayofweek", dayofweek("event_timestamp")) \
            .withColumn("event_dayname", date_format("event_timestamp", "EEEE")) \
            .withColumn("event_week", weekofyear("event_timestamp")) \
            .withColumn("event_month", month("event_timestamp")) \
            .withColumn("event_year", year("event_timestamp")) \
            .withColumn("event_quarter", quarter("event_timestamp")) \
            .withColumn("is_weekend", 
                when(col("event_dayofweek").isin([1, 7]), True).otherwise(False)) \
            .withColumn("is_business_hours",
                when(col("event_hour").between(9, 17), True).otherwise(False)) \
            .withColumn("has_product", 
                when(col("product_id").isNotNull(), True).otherwise(False)) \
            .withColumn("is_bot", 
                when(col("session_duration_sec") < 1, True).otherwise(False)) \
            .withColumn("is_mobile",
                when(col("device") == "mobile", True).otherwise(False)) \
            .withColumn("action_category",
                when(col("action").isin("page_view"), "page_view")
                .when(col("action").isin("product_view"), "browsing")
                .when(col("action").isin("add_to_cart", "remove_from_cart"), "cart")
                .when(col("action").isin("checkout_start", "purchase"), "purchase")
                .when(col("action") == "search", "search")
                .when(col("action") == "click", "click")
                .when(col("action") == "review_write", "engagement")
                .otherwise("other")) \
            .withColumn("device_type",
                when(col("device").isin("mobile", "smartphone", "iphone", "android"), "mobile")
                .when(col("device").isin("tablet", "ipad"), "tablet")
                .otherwise("desktop")) \
            .withColumn("category",
                when(col("category").isNull(), lit("unknown"))
                .otherwise(lower(trim(col("category"))))) \
            .withColumn("product_name",
                when(col("product_name").isNull(), lit("unknown"))
                .otherwise(col("product_name"))) \
            .withColumn("price",
                when(col("price").isNull(), lit(0.0))
                .otherwise(col("price"))) \
            .withColumn("session_duration_sec",
                when(col("session_duration_sec").isNull(), lit(0))
                .otherwise(col("session_duration_sec"))) \
            .withColumn("year", col("event_year")) \
            .withColumn("month", col("event_month")) \
            .withColumn("day", dayofmonth("event_timestamp"))
        
        new_cols = [
            "processing_timestamp", "event_date", "event_hour", "event_dayname",
            "is_weekend", "is_business_hours", "has_product", "is_bot", "is_mobile",
            "action_category", "device_type"
        ]
        
        logger.info(f"   Added {len(new_cols)} enrichment fields")
        logger.info(f"   Total columns: {len(enriched.columns)}")
        
        return enriched
    
    # ==============================================
    # СОХРАНЕНИЕ
    # ==============================================
    
    def _save_to_silver(self, df):
        """Сохранение обработанных данных в HDFS и MinIO"""
        logger.info("\n💾 Saving to Silver layer...")
        
        # --- HDFS (основное хранилище) ---
        hdfs_path = f"{self.hdfs_base}/data/silver/events"
        
        df.write \
            .mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .option("compression", "snappy") \
            .parquet(hdfs_path)
        
        logger.info(f"   ✓ HDFS: {hdfs_path}")
        
        # --- MinIO Silver bucket ---
        minio_path = f"s3a://{self.silver_bucket}/events/"
        
        df.write \
            .mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .option("compression", "snappy") \
            .parquet(minio_path)
        
        logger.info(f"   ✓ MinIO: {minio_path}")
        
        # Статистика сохранения
        # logger.info(f"   Records: {df.count():,}")
        # logger.info(f"   Columns: {len(df.columns)}")
        logger.info(f"   Compression: snappy")
    
    # ==============================================
    # СОЗДАНИЕ СЕССИЙ
    # ==============================================
    
    def _create_session_aggregates(self, events_df):
        """Агрегация событий в сессии"""
        logger.info("\n📊 Creating session aggregations...")
        
        sessions = events_df \
            .groupBy("user_id", "username", "session_id") \
            .agg(
                # Информация о пользователе
                first("country").alias("country"),
                first("membership_level").alias("membership_level"),
                first("device_type").alias("primary_device"),
                first("browser").alias("primary_browser"),
                # Временные метки
                min("event_timestamp").alias("session_start"),
                max("event_timestamp").alias("session_end"),
                first("event_date").alias("session_date"),
                # Счетчики событий
                count("*").alias("total_events"),
                countDistinct("action").alias("unique_actions"),
                countDistinct("product_id").alias("unique_products"),
                # Детализация по типам событий
                sum(when(col("action") == "page_view", 1).otherwise(0)).alias("page_views"),
                sum(when(col("action") == "product_view", 1).otherwise(0)).alias("product_views"),
                sum(when(col("action") == "add_to_cart", 1).otherwise(0)).alias("cart_adds"),
                sum(when(col("action") == "remove_from_cart", 1).otherwise(0)).alias("cart_removes"),
                sum(when(col("action") == "click", 1).otherwise(0)).alias("clicks"),
                sum(when(col("action") == "search", 1).otherwise(0)).alias("searches"),
                sum(when(col("action") == "purchase", 1).otherwise(0)).alias("purchases"),
                # Взаимодействие с продуктами
                sum(when(col("has_product") == True, 1).otherwise(0)).alias("product_interactions"),
                sum("price").alias("total_price_viewed"),
                avg("price").alias("avg_price_viewed"),
                # Сессионные метрики
                first("session_duration_sec").alias("session_duration_sec"),
                avg("session_duration_sec").alias("avg_event_interval_sec"),
                # Флаги
                max("is_weekend").alias("is_weekend_session"),
                max("is_business_hours").alias("during_business_hours"),
                max("is_bot").alias("is_bot_session"),
                max("is_mobile").alias("is_mobile_session")
            ) \
            .withColumn("session_duration_min",
                round((unix_timestamp("session_end") - 
                       unix_timestamp("session_start")) / 60, 2)) \
            .withColumn("events_per_minute",
                round(col("total_events") / 
                      (col("session_duration_min") + 0.1), 2)) \
            .withColumn("engagement_score",
                col("page_views") * 1 +
                col("product_views") * 3 +
                col("cart_adds") * 5 +
                col("purchases") * 10 +
                col("clicks") * 0.5 +
                col("searches") * 2 -
                col("cart_removes") * 3) \
            .withColumn("has_purchase",
                when(col("purchases") > 0, True).otherwise(False)) \
            .withColumn("cart_abandoned",
                when((col("cart_adds") > 0) & (col("purchases") == 0), True)
                .otherwise(False)) \
            .withColumn("year", year("session_start")) \
            .withColumn("month", month("session_start"))
        
        # --- Сохранение сессий ---
        hdfs_path = f"{self.hdfs_base}/data/silver/sessions"
        sessions.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .option("compression", "snappy") \
            .parquet(hdfs_path)
        
        minio_path = f"s3a://{self.silver_bucket}/sessions/"
        sessions.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .option("compression", "snappy") \
            .parquet(minio_path)
        
        #logger.info(f"   ✓ Sessions saved: {sessions.count():,}")
        logger.info(f"   Sample sessions:")
        # sessions.select(
        #     "user_id", "session_id", "total_events",
        #     "session_duration_min", "engagement_score", "has_purchase"
        # ).show(5, truncate=False)
        
        return sessions
    
    # ==============================================
    # СТАТИСТИКА
    # ==============================================
    
    def _print_stats(self):
        """Вывод статистики обработки"""
        logger.info("\n" + "=" * 60)
        logger.info("SILVER LAYER PROCESSING SUMMARY")
        logger.info("=" * 60)
        
        logger.info(f"  Mode: {self.mode}")
        logger.info(f"  Bronze records read: {self.stats['bronze_count']:,}")
        logger.info(f"  After cleaning: {self.stats['cleaned_count']:,}")
        logger.info(f"  Duplicates/Invalid: {self.stats['duplicates_removed']:,}")
        logger.info(f"  Silver records: {self.stats['silver_count']:,}")
        logger.info(f"  Sessions created: {self.stats['sessions_count']:,}")
        logger.info(f"  Processing time: {self.stats['processing_time_sec']:.1f}s")
        logger.info(f"")
        logger.info(f"  Storage:")
        logger.info(f"    HDFS: {self.hdfs_base}/data/silver/")
        logger.info(f"    MinIO: s3a://{self.silver_bucket}/")
        logger.info("=" * 60)
    
    def get_stats(self):
        """Возвращает статистику последней обработки"""
        return self.stats