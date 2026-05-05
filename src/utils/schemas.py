from pyspark.sql.types import *

# Схема для сырых событий
event_schema = StructType([
    StructField("session_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("action", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("device", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("session_duration_sec", IntegerType(), True)
])

# Схема для обработанных данных
processed_event_schema = StructType([
    StructField("session_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("action", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("device", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("session_duration_sec", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("hour", IntegerType(), True)
])