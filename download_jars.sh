#!/bin/bash

# Создаём папку для JAR-файлов, если её нет
mkdir -p jars

echo "Начинаю загрузку зависимостей..."

# Функция для скачивания с проверкой
download_jar() {
  local url=$1
  local filename=$(basename "$url")
  echo "Скачиваю $filename ..."
  wget -q --show-progress -P jars/ "$url" || curl -L -o "jars/$filename" "$url"
}

download_jar "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
download_jar "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
download_jar "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
download_jar "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.0/kafka-clients-3.6.0.jar"
download_jar "https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar"
download_jar "https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar"
download_jar "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar"
download_jar "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar"

echo "Готово! Все JAR-файлы загружены в папку jars/"