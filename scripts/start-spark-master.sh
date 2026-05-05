#!/bin/bash
set -e

echo "=== Starting Spark Master ==="

# Копирование JAR'ов
if [ -d "/jars" ] && [ "$(ls -A /jars/*.jar 2>/dev/null)" ]; then
    echo "Copying JARs..."
    cp -v /jars/*.jar $SPARK_HOME/jars/ 2>/dev/null || true
fi

echo "Launching Master on 172.22.0.4..."
exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master \
    --host 172.22.0.4 \
    --port ${SPARK_MASTER_PORT:-7077} \
    --webui-port ${SPARK_MASTER_WEBUI_PORT:-8080}