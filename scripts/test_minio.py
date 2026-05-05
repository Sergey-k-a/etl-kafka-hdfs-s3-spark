#!/usr/bin/env python3
"""
Тестовый скрипт для проверки работы Spark с MinIO
"""

from pyspark.sql import SparkSession

def test_minio_connection():
    """Тест соединения с MinIO через Spark"""
    
    print("🧪 Тестирование Spark + MinIO соединения...")
    
    try:
        # Создаем SparkSession с настройками MinIO
        spark = SparkSession.builder \
            .appName("MinIO Connection Test") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://172.22.0.8:9302") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .getOrCreate()
        
        print("✅ SparkSession успешно создан")
        
        # Тест: создаем простой DataFrame
        test_data = [("test", 1), ("data", 2), ("spark", 3)]
        df = spark.createDataFrame(test_data, ["key", "value"])
        
        print(f"✅ DataFrame создан. Кол-во строк: {df.count()}")
        
        # Пробуем записать в MinIO
        try:
            output_path = "s3a://temp/spark_test_output/"
            df.write.mode("overwrite").csv(output_path)
            print(f"✅ Успешно записали в MinIO: {output_path}")
            
            # Пробуем прочитать обратно
            read_df = spark.read.csv(output_path)
            print(f"✅ Успешно прочитали из MinIO. Строк: {read_df.count()}")
            
        except Exception as e:
            print(f"⚠️ Ошибка при работе с MinIO: {e}")
            print("  Проверьте:")
            print("  1. Доступность MinIO по адресу: http://172.22.0.8:9302")
            print("  2. Существование бакета 'temp'")
            print("  3. Наличие JAR файлов в /opt/spark/jars/")
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка создания SparkSession: {e}")
        print("  Возможные причины:")
        print("  1. Отсутствуют JAR файлы для S3")
        print("  2. Проблемы с сетью")
        print("  3. Неправильные настройки Spark")
        return False

if __name__ == "__main__":
    test_minio_connection()