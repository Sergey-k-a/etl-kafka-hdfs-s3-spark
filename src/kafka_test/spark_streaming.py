from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum, count, avg, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("KafkaStreamProcessor") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
    .config("spark.jars", "/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/jars/kafka-clients-3.5.0.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Схема данных
schema = StructType([
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("value", DoubleType()),
    StructField("page", StringType())
])

print("="*50)
print("Starting Spark Structured Streaming...")
print("="*50)

# Читаем поток из Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "shkafka:29092") \
    .option("subscribe", "raw-events") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Парсим JSON
parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
 .withColumn("event_time", to_timestamp(col("timestamp")))

# Агрегация за 30-секундные окна
analytics = parsed \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window(col("event_time"), "30 seconds"),
        col("event_type")
    ) \
    .agg(
        count("*").alias("total_events"),
        _sum("value").alias("total_value"),
        avg("value").alias("avg_value")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("event_type"),
        col("total_events"),
        col("total_value"),
        col("avg_value")
    )

# Вывод в консоль
console_query = analytics.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .start()

# Сохранение агрегатов в HDFS Parquet
hdfs_query = analytics.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://shnamenode:9000/data/analytics/") \
    .option("checkpointLocation", "/tmp/spark-checkpoint-hdfs") \
    .partitionBy("event_type") \
    .trigger(processingTime="30 seconds") \
    .start()

print("Streaming queries started!")
print("Waiting for data...")

# Держим процесс живым
spark.streams.awaitAnyTermination()