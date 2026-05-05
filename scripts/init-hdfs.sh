#!/bin/bash

# Ожидание запуска NameNode
/scripts/wait-for-it.sh sh_namenode:9870 -- echo "NameNode is up"

# Инициализация HDFS директорий
hdfs dfs -mkdir -p /data/raw/sales
hdfs dfs -mkdir -p /data/reference
hdfs dfs -mkdir -p /data/processed/sales
hdfs dfs -mkdir -p /data/aggregated/sales
hdfs dfs -mkdir -p /data/checkpoints/etl

# Установка прав
hdfs dfs -chmod -R 755 /data

echo "HDFS directories initialized successfully"

# Загрузка тестовых данных, если они есть
if [ -d "/data/sample" ]; then
    echo "Loading sample data to HDFS..."
    hdfs dfs -put /data/sample/sales.csv /data/raw/sales/ 2>/dev/null || true
    hdfs dfs -put /data/sample/customers.csv /data/reference/ 2>/dev/null || true
    echo "Sample data loaded to HDFS"
fi
