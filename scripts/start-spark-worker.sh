#!/bin/bash
set -e

WEBUI_PORT=${1:-8081}
IP=${2:-172.22.0.5}

echo "=== Starting Spark Worker on $IP:$WEBUI_PORT ==="

# Копирование JAR'ов
if [ -d "/jars" ] && [ "$(ls -A /jars/*.jar 2>/dev/null)" ]; then
    echo "Copying JARs..."
    cp -v /jars/*.jar $SPARK_HOME/jars/ 2>/dev/null || true
fi

# Ожидание Master
echo "Waiting for Spark Master..."
until curl -f http://172.22.0.4:8080 >/dev/null 2>&1; do
    sleep 2
done

echo "Launching Worker..."
exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
    --webui-port $WEBUI_PORT \
    --host $IP \
    spark://172.22.0.4:${SPARK_MASTER_PORT:-7077}