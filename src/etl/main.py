#!/usr/bin/env python3
"""
Main ETL Pipeline для Data Lakehouse
Запуск: spark-submit --master spark://172.22.0.4:7077 src/main.py
"""
import sys
import os
#sys.path.append('/app/src')
sys.path.insert(0, '/app/src')

from pyspark.sql.functions import *
from pyspark.sql.types import *

from generators.data_generator import ECommerceDataGenerator
from etl.bronze_loader import BronzeLoader
from etl.silver_processor import SilverProcessor
from etl.gold_aggregator import GoldAggregator
from utils.spark_config import create_spark_session, test_connections
from pyspark.sql.functions import *
import time
from datetime import datetime
import numpy as np
import pandas as pd
import json


def print_header(title):
    """Вывод заголовка секции"""
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def main(is_generate = True, is_test = True, is_bronze = True, is_silver = True, is_gold = True, is_result = True, is_statStorage = True):
    """Главная функция ETL пайплайна"""
    
    total_start = time.time()
    
    print_header("🚀 DATA LAKEHOUSE ETL PIPELINE")
    print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Spark Master: spark://172.22.0.4:7077")
    print(f"MinIO: http://172.22.0.8:9302")
    print(f"HDFS: hdfs://172.22.0.2:9000")
    
    # ============================================
    # Инициализация Spark
    # ============================================
    print_header("[INIT] Initializing Spark session...")
    
    spark = create_spark_session(app_name="ETL-Pipeline-Full")
    
    if is_test: test_connections(spark)
    
    # ============================================
    # STEP 1: Генерация данных
    # ============================================
    
    if is_generate:
        print_header("[STEP 1/4] Generating synthetic e-commerce data...")
    
        step_start = time.time()
        generator = ECommerceDataGenerator(seed=42)
        print("  📦 Generating products...")
        products_df = generator.generate_products(num_products=10)
        
        print("  👥 Generating users...")
        users_df = generator.generate_users(num_users=10)
        
        print("  🔄 Generating sessions...")
        sessions_df = generator.generate_sessions(
            products_df, users_df, 
            num_sessions=50,
            start_date='2024-01-01',
            end_date='2024-12-31'
        )
        
        print("  🛒 Generating orders...")
        orders_df = generator.generate_orders(sessions_df, products_df, users_df)
        
        gen_time = time.time() - step_start
        total_records = len(products_df) + len(users_df) + len(sessions_df) + len(orders_df)
        
        print(f"\n  ✅ Generated {total_records:,} records in {gen_time:.1f}s")
        print(f"     Products: {len(products_df):,}")
        print(f"     Users:    {len(users_df):,}")
        # print(f"     Sessions: {len(sessions_df):,}")
        print(f"     Orders:   {len(orders_df):,}")

    
    # ============================================
    # STEP 2: Bronze Layer - Загрузка в MinIO
    # ============================================
    if is_bronze:
        print_header("[STEP 2/4] Loading data to Bronze layer (MinIO)...")
        
        step_start = time.time()
        
        # Конвертация Pandas → Spark

        # schema = StructType([
        #     StructField("product_id", StringType(), True),
        #     StructField("name", StringType(), True),
        #     StructField("category", StringType(), True),
        #     StructField("subcategory", StringType(), True),
        #     StructField("price", StringType(), True),
        #     StructField("stock_quantity", StringType(), True),
        #     StructField("rating", StringType(), True),
        #     StructField("created_date", StringType(), True),
        #     ])

        
        products_spark = spark.createDataFrame(products_df)
        products_spark.show(5)
        products_spark.printSchema()
        
        
        

        users_spark = spark.createDataFrame(users_df)
        sessions_spark = spark.createDataFrame(sessions_df)
        orders_spark = spark.createDataFrame(orders_df)

        

        # Сохранение событий во временные файлы (имитация потока)
        print("  💾 Saving temp files...")
        tmp_events_path = "/tmp/events_raw"
        sessions_spark.coalesce(2).write.mode("overwrite").json(tmp_events_path)
        print(f"     ✓ Temp events saved to {tmp_events_path}")
        
        # Загрузка в Bronze через loader
        loader = BronzeLoader(spark)
        
        print("  📥 Loading events to Bronze...")
        bronze_events = loader.load_events(tmp_events_path)
        
        print("  📦 Loading products to Bronze...")
        bronze_products = loader.load_products(products_spark)
        
        print("  👥 Loading users to Bronze...")
        bronze_users = loader.load_users(users_spark)
        
        bronze_time = time.time() - step_start
        
        print(f"\n  ✅ Bronze layer loaded in {bronze_time:.1f}s")
        print(f"     Events:   {bronze_events.count():,} → s3a://bronze/events/")
        print(f"     Products: {bronze_products.count():,} → s3a://bronze/products/")
        print(f"     Users:    {bronze_users.count():,} → s3a://bronze/users/")
    
    # ============================================
    # STEP 3: Silver Layer - Обработка
    # ============================================
    if is_silver:
        print_header("[STEP 3/4] Processing data to Silver layer...")
        
        step_start = time.time()
        processor = SilverProcessor(spark)
        
        print("  🧹 Cleaning and enriching events...")
        silver_events = processor.process_events()
        
        print("  📊 Creating session aggregates...")
        silver_sessions = processor.create_user_sessions(silver_events)
        
        silver_time = time.time() - step_start
        
        print(f"\n  ✅ Silver layer processed in {silver_time:.1f}s")
        print(f"     Events:   {silver_events.count():,} → HDFS + MinIO")
        print(f"     Sessions: {silver_sessions.count():,} → HDFS + MinIO")
        
    # ============================================
    # STEP 4: Gold Layer - Бизнес-витрины
    # ============================================
    if is_gold:
        print_header("[STEP 4/4] Creating business views in Gold layer...")
        
        step_start = time.time()
        aggregator = GoldAggregator(spark)
        
        print("  📊 Creating hourly metrics...")
        hourly_metrics = aggregator.create_hourly_metrics(silver_events)
        
        print("  📦 Creating product analytics...")
        product_analytics = aggregator.create_product_analytics(silver_events)
        
        print("  👥 Creating user segments (RFM)...")
        user_segments = aggregator.create_user_segments(silver_sessions, bronze_users)
        
        gold_time = time.time() - step_start
        
        print(f"\n  ✅ Gold layer created in {gold_time:.1f}s")
        print(f"     Hourly metrics:   {hourly_metrics.count():,} records")
        print(f"     Product analytics: {product_analytics.count():,} products")
        print(f"     User segments:    {user_segments.count():,} users")
    
    # ============================================
    # STEP 5: Результаты
    # ============================================
    if is_result:
        print_header("📊 PIPELINE RESULTS")
        
        print("\n  📊 Hourly Metrics (sample):")
        hourly_metrics.orderBy("year", "month", "day", "event_hour").show(5, truncate=False)
        
        print("\n  📦 Top 5 Products by Popularity:")
        product_analytics \
            .orderBy("popularity_score", ascending=False) \
            .select("product_name", "category", "popularity_score", "view_to_purchase_rate") \
            .show(5, truncate=False)
        
        print("\n  👥 User Segments Distribution:")
        user_segments \
            .groupBy("segment") \
            .count() \
            .withColumn("percentage", round(col("count") / user_segments.count() * 100, 2)) \
            .orderBy("count", ascending=False) \
            .show(truncate=False)
    
    # ============================================
    # STEP 6: Статистика хранилища
    # ============================================
    if is_statStorage:
        print_header("💾 STORAGE STATISTICS")
        
        try:
            # Проверка MinIO Bronze
            bronze_events_verify = spark.read.parquet("s3a://bronze/events/")
            bronze_products_verify = spark.read.parquet("s3a://bronze/products/")
            print(f"  ✅ Bronze (MinIO):")
            print(f"     s3a://bronze/events/   - {bronze_events_verify.count():,} records")
            print(f"     s3a://bronze/products/ - {bronze_products_verify.count():,} records")
            print(f"     s3a://bronze/users/    - {spark.read.parquet('s3a://bronze/users/').count():,} records")
        except Exception as e:
            print(f"  ⚠️ MinIO check: {str(e)[:80]}")
        
        try:
            # Проверка HDFS Silver
            hdfs_events = spark.read.parquet("hdfs://172.22.0.2:9000/data/processed/events")
            hdfs_sessions = spark.read.parquet("hdfs://172.22.0.2:9000/data/processed/sessions")
            print(f"  ✅ Silver (HDFS):")
            print(f"     /data/processed/events   - {hdfs_events.count():,} records")
            print(f"     /data/processed/sessions - {hdfs_sessions.count():,} records")
        except Exception as e:
            print(f"  ⚠️ HDFS check: {str(e)[:80]}")
        
        try:
            # Проверка MinIO Gold
            gold_hourly = spark.read.parquet("s3a://gold/hourly_metrics/")
            print(f"  ✅ Gold (MinIO):")
            print(f"     s3a://gold/hourly_metrics/    - {gold_hourly.count():,} records")
            print(f"     s3a://gold/product_analytics/ - Done")
            print(f"     s3a://gold/user_segments/     - Done")
        except Exception as e:
            print(f"  ⚠️ Gold check: {str(e)[:80]}")
    
    # ============================================
    # Финал
    # ============================================
    total_time = time.time() - total_start
    
    print_header("✅ PIPELINE COMPLETED SUCCESSFULLY")
    
    
    
    spark.stop()
    print("Spark session stopped. Goodbye!")


if __name__ == "__main__":
    main()