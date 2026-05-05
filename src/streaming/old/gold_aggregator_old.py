from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoldAggregator:
    """
    Создание бизнес-витрин из Silver слоя.
    
    Витрины:
    1. hourly_metrics - Почасовые метрики активности
    2. user_activity - Активность пользователей + RFM сегментация
    3. product_analytics - Аналитика по продуктам
    4. geo_analytics - Географическая аналитика
    5. device_analytics - Аналитика по устройствам
    6. session_analytics - Аналитика по сессиям
    7. conversion_funnel - Воронка конверсии
    """
    
    def __init__(self, spark, mode="full"):
        """
        Args:
            spark: SparkSession
            mode: "full" - пересоздать все, "incremental" - обновить
        """
        self.spark = spark
        self.mode = mode
        self.gold_bucket = "gold"
        self.hdfs_base = "hdfs://172.22.0.2:9000"
        
        # Пути к исходным данным Silver
        self.silver_events_path = f"{self.hdfs_base}/data/silver/events"
        self.silver_sessions_path = f"{self.hdfs_base}/data/silver/sessions"
        
        # Статистика
        self.stats = {}
    
    def create_all_views(self):
        """Создание всех бизнес-витрин"""
        logger.info("=" * 60)
        logger.info("GOLD LAYER: Creating Business Views")
        logger.info("=" * 60)
        
        import time
        start_time = time.time()
        
        # --------------------------------------------------
        # Загрузка данных из Silver слоя
        # --------------------------------------------------
        logger.info("\n📥 Reading from Silver layer...")
        
        try:
            events_df = self.spark.read.parquet(self.silver_events_path)
            logger.info(f"   ✓ Events loaded: {events_df.count():,}")
        except Exception as e:
            logger.error(f"   ✗ Cannot read events: {e}")
            logger.info("   Trying MinIO path...")
            events_df = self.spark.read.parquet("s3a://silver/events/")
            logger.info(f"   ✓ Events loaded from MinIO: {events_df.count():,}")
        
        try:
            sessions_df = self.spark.read.parquet(self.silver_sessions_path)
            logger.info(f"   ✓ Sessions loaded: {sessions_df.count():,}")
        except:
            logger.warning("   No sessions found, will create from events")
            sessions_df = None
        
        # --------------------------------------------------
        # Создание витрин
        # --------------------------------------------------
        
        # Витрина 1: Почасовые метрики
        hourly = self._create_hourly_metrics(events_df)
        self.stats['hourly_metrics'] = hourly.count() if hourly else 0
        
        # Витрина 2: Активность пользователей + RFM
        users = self._create_user_activity(events_df, sessions_df)
        self.stats['user_activity'] = users.count() if users else 0
        
        # Витрина 3: Аналитика по продуктам
        products = self._create_product_analytics(events_df)
        self.stats['product_analytics'] = products.count() if products else 0
        
        # Витрина 4: Географическая аналитика
        geo = self._create_geo_analytics(events_df)
        self.stats['geo_analytics'] = geo.count() if geo else 0
        
        # Витрина 5: Аналитика по устройствам
        devices = self._create_device_analytics(events_df)
        self.stats['device_analytics'] = devices.count() if devices else 0
        
        # Витрина 6: Аналитика по сессиям (если есть)
        if sessions_df is not None:
            sessions_stats = self._create_session_analytics(sessions_df)
            self.stats['session_analytics'] = sessions_stats.count() if sessions_stats else 0
        
        # Витрина 7: Воронка конверсии
        funnel = self._create_conversion_funnel(events_df)
        self.stats['conversion_funnel'] = funnel.count() if funnel else 0
        
        # --------------------------------------------------
        # Финиш
        # --------------------------------------------------
        elapsed = time.time() - start_time
        
        logger.info("\n" + "=" * 60)
        logger.info("GOLD LAYER SUMMARY")
        logger.info("=" * 60)
        for view, count in self.stats.items():
            logger.info(f"  ✓ {view}: {count:,} records")
        logger.info(f"  ⏱ Total time: {elapsed:.1f}s")
        logger.info(f"  💾 HDFS: {self.hdfs_base}/data/gold/")
        logger.info(f"  💾 MinIO: s3a://{self.gold_bucket}/")
        logger.info("=" * 60)
        
        return self.stats
    
    # ==============================================
    # ВСПОМОГАТЕЛЬНЫЙ МЕТОД СОХРАНЕНИЯ
    # ==============================================
    
    def _save_view(self, df, view_name, partitions=None):
        """
        Сохранение витрины в HDFS и MinIO Gold
        
        Args:
            df: DataFrame для сохранения
            view_name: Имя витрины
            partitions: Список колонок для партиционирования
        """
        hdfs_path = f"{self.hdfs_base}/data/gold/{view_name}"
        minio_path = f"s3a://{self.gold_bucket}/{view_name}/"
        
        write_mode = "overwrite" if self.mode == "full" else "append"
        
        writer = df.write.mode(write_mode).option("compression", "snappy")
        if partitions:
            writer = writer.partitionBy(*partitions)
        
        # Сохранение в HDFS
        writer.parquet(hdfs_path)
        
        # Сохранение в MinIO
        writer.parquet(minio_path)
        
        logger.info(f"     💾 {view_name}: {df.count():,} records saved")
    
    # ==============================================
    # ВИТРИНА 1: ПОЧАСОВЫЕ МЕТРИКИ
    # ==============================================
    
    def _create_hourly_metrics(self, df):
        """Почасовые метрики активности платформы"""
        logger.info("\n📊 [1/7] Creating hourly metrics...")
        
        hourly = df.groupBy(
            "event_year", "event_month", "event_date", "event_hour"
        ).agg(
            # Основные метрики
            countDistinct("user_id").alias("unique_users"),
            countDistinct("session_id").alias("unique_sessions"),
            count("*").alias("total_events"),
            
            # Разбивка по категориям
            sum(when(col("action_category") == "browsing", 1).otherwise(0)).alias("browsing_events"),
            sum(when(col("action_category") == "cart", 1).otherwise(0)).alias("cart_events"),
            sum(when(col("action_category") == "purchase", 1).otherwise(0)).alias("purchase_events"),
            sum(when(col("action_category") == "search", 1).otherwise(0)).alias("search_events"),
            sum(when(col("action_category") == "click", 1).otherwise(0)).alias("click_events"),
            
            # Продуктовые метрики
            sum(when(col("has_product") == True, 1).otherwise(0)).alias("product_interactions"),
            sum("price").alias("total_price_exposure"),
            avg("price").alias("avg_price"),
            
            # Технические метрики
            sum(when(col("is_bot") == True, 1).otherwise(0)).alias("bot_events"),
            sum(when(col("is_weekend") == True, 1).otherwise(0)).alias("weekend_events"),
            sum(when(col("is_mobile") == True, 1).otherwise(0)).alias("mobile_events"),
            
            # География
            countDistinct("country").alias("unique_countries"),
            
            # Длительность сессий
            avg("session_duration_sec").alias("avg_session_duration_sec")
        ) \
        .withColumn("conversion_rate", 
            round(col("purchase_events") / col("unique_sessions") * 100, 2)) \
        .withColumn("bot_percentage",
            round(col("bot_events") / col("total_events") * 100, 2)) \
        .withColumn("mobile_percentage",
            round(col("mobile_events") / col("total_events") * 100, 2)) \
        .withColumn("avg_events_per_user",
            round(col("total_events") / col("unique_users"), 2)) \
        .withColumn("avg_events_per_session",
            round(col("total_events") / col("unique_sessions"), 2)) \
        .withColumn("product_interaction_rate",
            round(col("product_interactions") / col("total_events") * 100, 2)) \
        .withColumn("generated_at", current_timestamp()) \
        .orderBy("event_year", "event_month", "event_date", "event_hour")
        
        self._save_view(hourly, "hourly_metrics", ["event_year", "event_month"])
        
        logger.info("   Sample hourly metrics:")
        hourly.select(
            "event_date", "event_hour", "unique_users", "total_events",
            "conversion_rate", "bot_percentage"
        ).show(5, truncate=False)
        
        return hourly
    
    # ==============================================
    # ВИТРИНА 2: АКТИВНОСТЬ ПОЛЬЗОВАТЕЛЕЙ + RFM
    # ==============================================
    
    def _create_user_activity(self, events_df, sessions_df=None):
        """Активность пользователей с RFM сегментацией"""
        logger.info("\n👥 [2/7] Creating user activity & RFM segmentation...")
        
        # Базовая активность пользователей
        user_activity = events_df.groupBy(
            "user_id", "username", "country", "membership_level"
        ).agg(
            # Активность
            countDistinct("session_id").alias("total_sessions"),
            count("*").alias("total_events"),
            min("event_timestamp").alias("first_activity"),
            max("event_timestamp").alias("last_activity"),
            countDistinct("event_date").alias("active_days"),
            
            # Действия
            sum(when(col("action_category") == "browsing", 1).otherwise(0)).alias("browsing_events"),
            sum(when(col("action_category") == "cart", 1).otherwise(0)).alias("cart_events"),
            sum(when(col("action_category") == "purchase", 1).otherwise(0)).alias("purchase_events"),
            sum(when(col("action_category") == "search", 1).otherwise(0)).alias("search_events"),
            sum(when(col("action_category") == "click", 1).otherwise(0)).alias("click_events"),
            
            # Продукты
            sum(when(col("has_product") == True, 1).otherwise(0)).alias("product_interactions"),
            countDistinct("product_id").alias("unique_products_viewed"),
            sum("price").alias("total_price_exposure"),
            avg("price").alias("avg_price_viewed"),
            max("price").alias("max_price_viewed"),
            
            # Устройства
            countDistinct("device_type").alias("device_diversity"),
            sum(when(col("is_mobile") == True, 1).otherwise(0)).alias("mobile_events"),
            
            # Длительность
            avg("session_duration_sec").alias("avg_session_duration_sec"),
            sum("session_duration_sec").alias("total_session_duration_sec")
        ) \
        .withColumn("active_days_span",
            datediff(col("last_activity"), col("first_activity"))) \
        .withColumn("events_per_day",
            round(col("total_events") / col("active_days"), 2)) \
        .withColumn("mobile_ratio",
            round(col("mobile_events") / col("total_events") * 100, 2)) \
        .withColumn("avg_price_viewed",
            round(col("avg_price_viewed"), 2)) \
        .withColumn("total_price_exposure",
            round(col("total_price_exposure"), 2))
        
        # --- RFM Сегментация ---
        current_date = lit(datetime.now().strftime("%Y-%m-%d"))
        
        # rfm = user_activity \
        #     .withColumn("recency_days",
        #         datediff(current_date, to_date(col("last_activity")))) \
        #     .withColumn("recency_score",
        #         ntile(4).over(Window.orderBy(col("recency_days").asc()))) \
        #     .withColumn("frequency_score",
        #         ntile(4).over(Window.orderBy(col("total_sessions").desc()))) \
        #     .withColumn("monetary_score",
        #         ntile(4).over(Window.orderBy(col("total_price_exposure").desc()))) \
        #     .withColumn("engagement_score",
        #         ntile(4).over(Window.orderBy(col("events_per_day").desc())))

        rfm = (user_activity 
            .withColumn("recency_days",
                datediff(current_date, to_date(col("last_activity")))) 
            # Recency
            .withColumn("recency_pct",
                percent_rank().over(Window.orderBy(col("recency_days").asc()))) 
            .withColumn("recency_score",
                when(col("recency_pct") >= 0.75, 4)
                .when(col("recency_pct") >= 0.50, 3)
                .when(col("recency_pct") >= 0.25, 2)
                .otherwise(1)) 
            # Frequency
            .withColumn("frequency_pct",
                percent_rank().over(Window.orderBy(col("total_sessions").asc()))) 
            .withColumn("frequency_score",
                when(col("frequency_pct") >= 0.75, 4)
                .when(col("frequency_pct") >= 0.50, 3)
                .when(col("frequency_pct") >= 0.25, 2)
                .otherwise(1)) 
            # Monetary
            .withColumn("monetary_pct",
                percent_rank().over(Window.orderBy(col("total_price_exposure").asc()))) 
            .withColumn("monetary_score",
                when(col("monetary_pct") >= 0.75, 4)
                .when(col("monetary_pct") >= 0.50, 3)
                .when(col("monetary_pct") >= 0.25, 2)
                .otherwise(1))
        )
        # Сегменты
        users_segmented = rfm.withColumn("rfm_segment",
            when((col("recency_score") >= 3) & (col("frequency_score") >= 3) & (col("monetary_score") >= 3), "Champions 🏆")
            .when((col("recency_score") >= 3) & (col("frequency_score") >= 2), "Loyal 💎")
            .when((col("recency_score") >= 3) & (col("frequency_score") <= 1), "New 🌱")
            .when((col("recency_score") == 2) & (col("frequency_score") >= 2), "Potential ⭐")
            .when((col("recency_score") == 2) & (col("frequency_score") <= 1), "Needs Attention ⚠️")
            .when((col("recency_score") <= 1) & (col("frequency_score") >= 2), "At Risk 🔴")
            .when((col("recency_score") <= 1) & (col("frequency_score") <= 1), "Lost 💀")
            .otherwise("Unclassified ❓"))
        
        self._save_view(users_segmented, "user_activity")
        
        # Вывод распределения сегментов
        logger.info("   User segments distribution:")
        users_segmented.groupBy("rfm_segment").count() \
            .withColumn("%", round(col("count") / users_segmented.count() * 100, 2)) \
            .orderBy("count", ascending=False) \
            .show(truncate=False)
        
        return users_segmented
    
    # ==============================================
    # ВИТРИНА 3: АНАЛИТИКА ПО ПРОДУКТАМ
    # ==============================================
    
    def _create_product_analytics(self, df):
        """Аналитика по продуктам"""
        logger.info("\n📦 [3/7] Creating product analytics...")
        
        products = df.filter(col("product_id").isNotNull()) \
            .groupBy("product_id", "product_name", "category") \
            .agg(
                # Взаимодействия
                count("*").alias("total_interactions"),
                sum(when(col("action") == "product_view", 1).otherwise(0)).alias("views"),
                sum(when(col("action") == "add_to_cart", 1).otherwise(0)).alias("cart_adds"),
                sum(when(col("action") == "remove_from_cart", 1).otherwise(0)).alias("cart_removes"),
                sum(when(col("action") == "click", 1).otherwise(0)).alias("clicks"),
                sum(when(col("action") == "purchase", 1).otherwise(0)).alias("purchases"),
                
                # Пользователи
                countDistinct("user_id").alias("unique_users_interested"),
                countDistinct("session_id").alias("unique_sessions"),
                
                # Цена
                avg("price").alias("avg_price"),
                min("price").alias("min_price"),
                max("price").alias("max_price"),
                sum("price").alias("total_price_exposure"),
                
                # Время
                avg("session_duration_sec").alias("avg_session_duration"),
                
                # Устройства
                sum(when(col("is_mobile") == True, 1).otherwise(0)).alias("mobile_views"),
                sum(when(col("is_weekend") == True, 1).otherwise(0)).alias("weekend_views")
            ) \
            .withColumn("view_to_cart_rate",
                round(col("cart_adds") / col("views") * 100, 2)) \
            .withColumn("cart_to_purchase_rate",
                round(col("purchases") / col("cart_adds") * 100, 2)) \
            .withColumn("view_to_purchase_rate",
                round(col("purchases") / col("views") * 100, 2)) \
            .withColumn("cart_abandonment_rate",
                round(col("cart_removes") / col("cart_adds") * 100, 2)) \
            .withColumn("mobile_view_rate",
                round(col("mobile_views") / col("total_interactions") * 100, 2)) \
            .withColumn("interest_score",
                col("views") * 1 +
                col("cart_adds") * 3 +
                col("clicks") * 0.5 +
                col("purchases") * 5 -
                col("cart_removes") * 2) \
            .withColumn("generated_at", current_timestamp()) \
            .orderBy("interest_score", ascending=False)
        
        self._save_view(products, "product_analytics")
        
        logger.info("   Top 5 products:")
        products.select(
            "product_name", "category", "views", "cart_adds",
            "view_to_cart_rate", "interest_score"
        ).show(5, truncate=False)
        
        return products
    
    # ==============================================
    # ВИТРИНА 4: ГЕОГРАФИЧЕСКАЯ АНАЛИТИКА
    # ==============================================
    
    def _create_geo_analytics(self, df):
        """Географическая аналитика"""
        logger.info("\n🌍 [4/7] Creating geographic analytics...")
        
        geo = df.groupBy("country").agg(
            # Пользователи
            countDistinct("user_id").alias("unique_users"),
            countDistinct("session_id").alias("total_sessions"),
            count("*").alias("total_events"),
            
            # Действия
            sum(when(col("action_category") == "purchase", 1).otherwise(0)).alias("purchases"),
            sum(when(col("action_category") == "cart", 1).otherwise(0)).alias("cart_interactions"),
            sum(when(col("action_category") == "browsing", 1).otherwise(0)).alias("browsing"),
            
            # Продукты
            sum(when(col("has_product") == True, 1).otherwise(0)).alias("product_views"),
            sum("price").alias("total_price_exposure"),
            
            # Устройства
            sum(when(col("is_mobile") == True, 1).otherwise(0)).alias("mobile_events"),
            sum(when(col("is_weekend") == True, 1).otherwise(0)).alias("weekend_events"),
            
            # Сессии
            avg("session_duration_sec").alias("avg_session_sec"),
            
            # Разнообразие
            countDistinct("device_type").alias("device_diversity"),
            countDistinct("browser").alias("browser_diversity")
        ) \
        .withColumn("events_per_user",
            round(col("total_events") / col("unique_users"), 2)) \
        .withColumn("purchase_rate",
            round(col("purchases") / col("total_events") * 100, 2)) \
        .withColumn("mobile_ratio",
            round(col("mobile_events") / col("total_events") * 100, 2)) \
        .withColumn("weekend_ratio",
            round(col("weekend_events") / col("total_events") * 100, 2)) \
        .withColumn("avg_price_exposure_per_user",
            round(col("total_price_exposure") / col("unique_users"), 2)) \
        .withColumn("generated_at", current_timestamp()) \
        .orderBy("unique_users", ascending=False)
        
        self._save_view(geo, "geo_analytics")
        
        logger.info("   Top 5 countries:")
        geo.select(
            "country", "unique_users", "total_events",
            "purchase_rate", "mobile_ratio"
        ).show(5, truncate=False)
        
        return geo
    
    # ==============================================
    # ВИТРИНА 5: АНАЛИТИКА ПО УСТРОЙСТВАМ
    # ==============================================
    
    def _create_device_analytics(self, df):
        """Аналитика по устройствам и браузерам"""
        logger.info("\n📱 [5/7] Creating device analytics...")
        
        devices = df.groupBy("device_type", "browser").agg(
            countDistinct("user_id").alias("unique_users"),
            countDistinct("session_id").alias("total_sessions"),
            count("*").alias("total_events"),
            
            sum(when(col("action_category") == "purchase", 1).otherwise(0)).alias("purchases"),
            sum(when(col("has_product") == True, 1).otherwise(0)).alias("product_views"),
            sum(when(col("is_weekend") == True, 1).otherwise(0)).alias("weekend_events"),
            sum(when(col("is_business_hours") == True, 1).otherwise(0)).alias("business_hours_events"),
            
            avg("session_duration_sec").alias("avg_session_duration"),
            sum("price").alias("total_price_exposure"),
            countDistinct("country").alias("unique_countries")
        ) \
        .withColumn("purchase_rate",
            round(col("purchases") / col("total_events") * 100, 2)) \
        .withColumn("product_view_rate",
            round(col("product_views") / col("total_events") * 100, 2)) \
        .withColumn("weekend_ratio",
            round(col("weekend_events") / col("total_events") * 100, 2)) \
        .withColumn("business_hours_ratio",
            round(col("business_hours_events") / col("total_events") * 100, 2)) \
        .withColumn("avg_price_per_event",
            round(col("total_price_exposure") / col("total_events"), 2)) \
        .withColumn("generated_at", current_timestamp()) \
        .orderBy("total_events", ascending=False)
        
        self._save_view(devices, "device_analytics")
        
        logger.info("   Device stats:")
        devices.groupBy("device_type").agg(
            sum("total_events").alias("events"),
            round(avg("purchase_rate"), 2).alias("avg_purchase_rate"),
            round(avg("avg_session_duration"), 2).alias("avg_session_sec")
        ).show(truncate=False)
        
        return devices
    
    # ==============================================
    # ВИТРИНА 6: АНАЛИТИКА ПО СЕССИЯМ
    # ==============================================
    
    def _create_session_analytics(self, sessions_df):
        """Аналитика по сессиям"""
        logger.info("\n📈 [6/7] Creating session analytics...")
        
        session_stats = sessions_df.groupBy(
            "session_date", "country", "primary_device"
        ).agg(
            countDistinct("session_id").alias("total_sessions"),
            countDistinct("user_id").alias("unique_users"),
            
            avg("total_events").alias("avg_events_per_session"),
            avg("session_duration_min").alias("avg_duration_min"),
            avg("engagement_score").alias("avg_engagement"),
            
            sum("page_views").alias("total_page_views"),
            sum("product_views").alias("total_product_views"),
            sum("cart_adds").alias("total_cart_adds"),
            sum("purchases").alias("total_purchases"),
            
            sum(when(col("has_purchase") == True, 1).otherwise(0)).alias("sessions_with_purchase"),
            sum(when(col("cart_abandoned") == True, 1).otherwise(0)).alias("abandoned_carts"),
            sum(when(col("is_bot_session") == True, 1).otherwise(0)).alias("bot_sessions")
        ) \
        .withColumn("conversion_rate",
            round(col("sessions_with_purchase") / col("total_sessions") * 100, 2)) \
        .withColumn("cart_abandonment_rate",
            round(col("abandoned_carts") / (col("total_cart_adds") + 0.01) * 100, 2)) \
        .withColumn("bot_rate",
            round(col("bot_sessions") / col("total_sessions") * 100, 2)) \
        .withColumn("year", year("session_date")) \
        .withColumn("month", month("session_date")) \
        .withColumn("generated_at", current_timestamp()) \
        .orderBy("session_date", ascending=False)
        
        self._save_view(session_stats, "session_analytics", ["year", "month"])
        
        logger.info("   Session overview:")
        session_stats.select(
            "session_date", "total_sessions", "avg_duration_min",
            "conversion_rate", "cart_abandonment_rate"
        ).show(5, truncate=False)
        
        return session_stats
    
    # ==============================================
    # ВИТРИНА 7: ВОРОНКА КОНВЕРСИИ
    # ==============================================
    
    def _create_conversion_funnel(self, df):
        """Воронка конверсии по дням"""
        logger.info("\n🔄 [7/7] Creating conversion funnel...")
        
        funnel = df.groupBy("event_date").agg(
            # Этапы воронки
            countDistinct("user_id").alias("visitors"),
            countDistinct("session_id").alias("sessions"),
            sum(when(col("action_category") == "browsing", 1).otherwise(0)).alias("browsing_events"),
            sum(when(col("action") == "product_view", 1).otherwise(0)).alias("product_views"),
            sum(when(col("action") == "add_to_cart", 1).otherwise(0)).alias("cart_adds"),
            sum(when(col("action") == "remove_from_cart", 1).otherwise(0)).alias("cart_removes"),
            sum(when(col("action") == "purchase", 1).otherwise(0)).alias("purchases"),
            countDistinct(when(col("action") == "purchase", col("session_id"))).alias("purchase_sessions"),
            
            # Дополнительно
            sum("price").alias("total_revenue"),
            avg("session_duration_sec").alias("avg_session_sec"),
            sum(when(col("is_weekend") == True, 1).otherwise(0)).alias("weekend_events")
        ) \
        .withColumn("visitor_to_session_rate",
            round(col("sessions") / col("visitors") * 100, 2)) \
        .withColumn("session_to_view_rate",
            round(col("product_views") / col("sessions") * 100, 2)) \
        .withColumn("view_to_cart_rate",
            round(col("cart_adds") / col("product_views") * 100, 2)) \
        .withColumn("cart_abandonment_rate",
            round(col("cart_removes") / col("cart_adds") * 100, 2)) \
        .withColumn("cart_to_purchase_rate",
            round(col("purchases") / col("cart_adds") * 100, 2)) \
        .withColumn("overall_conversion_rate",
            round(col("purchase_sessions") / col("sessions") * 100, 2)) \
        .withColumn("avg_revenue_per_visitor",
            round(col("total_revenue") / col("visitors"), 2)) \
        .withColumn("year", year("event_date")) \
        .withColumn("month", month("event_date")) \
        .withColumn("generated_at", current_timestamp()) \
        .orderBy("event_date")
        
        self._save_view(funnel, "conversion_funnel", ["year", "month"])
        
        logger.info("   Funnel overview (averages):")
        funnel.agg(
            round(avg("visitor_to_session_rate"), 2).alias("avg_visit_to_session"),
            round(avg("view_to_cart_rate"), 2).alias("avg_view_to_cart"),
            round(avg("cart_to_purchase_rate"), 2).alias("avg_cart_to_purchase"),
            round(avg("overall_conversion_rate"), 2).alias("avg_conversion")
        ).show()
        
        return funnel