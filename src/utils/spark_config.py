from pyspark.sql import SparkSession
import os

def create_spark_session(app_name="ETL-Pipeline"):
    """Создание Spark сессии с поддержкой MinIO (S3) и HDFS"""
    
    # Сборка JAR файлов
    jars = [
        "/jars/hadoop-aws-3.3.4.jar",
        "/jars/aws-java-sdk-bundle-1.12.262.jar"
    ]
    jars_string = ",".join(jars)
    
    spark = (SparkSession.builder
        .appName("ETL-Silver-Gold")
        .config("spark.hadoop.fs.s3a.endpoint", "http://172.22.0.8:9302")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # HDFS
        .config("spark.hadoop.fs.defaultFS", "hdfs://172.22.0.2:9000")
        # Оптимизация
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    # spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    # spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
    
    def safe_conf(spark, key):
        """Безопасное получение конфигурации"""
        try:
            return spark.conf.get(key)
        except:
            return "НЕ ЗАДАН!"

    print("=" * 60)
    print("🔧 ПРОВЕРКА КОНФИГУРАЦИИ SPARK:")
    print(f"openCostInBytes: {safe_conf(spark, 'spark.sql.files.openCostInBytes')}")
    print(f"maxPartitionBytes: {safe_conf(spark, 'spark.sql.files.maxPartitionBytes')}")
    print(f"s3a endpoint: {safe_conf(spark, 'spark.hadoop.fs.s3a.endpoint')}")
    print(f"fadvise: {safe_conf(spark, 'spark.hadoop.fs.s3a.experimental.input.fadvise')}")
    print(f"cores max: {safe_conf(spark, 'spark.cores.max')}")
    print(f"executor memory: {safe_conf(spark, 'spark.executor.memory')}")
    print("=" * 60)

    spark.sparkContext.setLogLevel("WARN")
    print(f"✓ Spark {spark.version} initialized")
    print(f"  Master: {spark.sparkContext.master}")
    
    return spark

def create_spark_session_test(app_name="ETL-Pipeline"):
    """Создание Spark сессии с поддержкой MinIO (S3) и HDFS"""
    
    spark = (SparkSession.builder
        .appName(app_name)
        # Базовые настройки Spark
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.files.openCostInBytes", "33554432") 
        .config("spark.sql.files.maxPartitionBytes", "268435456")
        .config("spark.sql.sources.parallelPartitionDiscovery.threshold", "32")
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "8")
        
        # MINIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://shminio:9302")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        
        # Увеличиваем пул соединений
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
        # Оптимизация для Parquet - случайное чтение
        .config("spark.hadoop.fs.s3a.experimental.input.fadvise", "random")
        .config("spark.hadoop.fs.s3a.readahead.range", "256K")
        # Кэширование метаданных S3
        .config("spark.hadoop.fs.s3a.metadatastore.authoritative", "true")
        
        #  HDFS 
        .config("spark.hadoop.fs.defaultFS", "hdfs://shnamenode:9000")
        
        # УПРАВЛЕНИЕ ПАМЯТЬЮ
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.3")
        
        .getOrCreate()
    )
    
    def safe_conf(spark, key):
        """Безопасное получение конфигурации"""
        try:
            return spark.conf.get(key)
        except:
            return "НЕ ЗАДАН!"

    print("=" * 60)
    print("🔧 ПРОВЕРКА КОНФИГУРАЦИИ SPARK:")
    print(f"openCostInBytes: {safe_conf(spark, 'spark.sql.files.openCostInBytes')}")
    print(f"maxPartitionBytes: {safe_conf(spark, 'spark.sql.files.maxPartitionBytes')}")
    print(f"s3a endpoint: {safe_conf(spark, 'spark.hadoop.fs.s3a.endpoint')}")
    print(f"fadvise: {safe_conf(spark, 'spark.hadoop.fs.s3a.experimental.input.fadvise')}")
    print(f"cores max: {safe_conf(spark, 'spark.cores.max')}")
    print(f"executor memory: {safe_conf(spark, 'spark.executor.memory')}")
    print("=" * 60)
    
    # Устанавливаем уровень логирования
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def test_connections(spark):
    """Тестирование подключений к MinIO и HDFS"""
    print("\n🔍 Testing connections...")
    
    # Тест MinIO
    try:
        spark.range(1).write.mode("overwrite").json("s3a://bronze/_test")
        print("  ✓ MinIO (s3a://bronze/)")
    except Exception as e:
        print(f"  ✗ MinIO: {str(e)[:80]}")
        raise
    
    # Тест HDFS
    try:
        spark.range(1).write.mode("overwrite").parquet("hdfs://172.22.0.2:9000/_test")
        print("  ✓ HDFS (hdfs://172.22.0.2:9000)")
    except Exception as e:
        print(f"  ✗ HDFS: {str(e)[:80]}")
        raise
    
    print("  ✅ All connections OK\n")