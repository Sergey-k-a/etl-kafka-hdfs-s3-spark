#!/bin/bash
set -e

echo "=== Starting Jupyter Lab ==="

# Копирование JAR'ов для S3A/MinIO
if [ -d "/jars" ] && [ "$(ls -A /jars/*.jar 2>/dev/null)" ]; then
    echo "Copying JARs to Spark..."
    cp -v /jars/*.jar $SPARK_HOME/jars/ 2>/dev/null || true
fi

# Создание рабочих директорий
mkdir -p /app/notebooks /app/logs /app/data

echo "Launching Jupyter Lab..."
exec jupyter lab \
    --ip=0.0.0.0 \
    --port=8888 \
    --no-browser \
    --allow-root \
    --NotebookApp.token="${JUPYTER_TOKEN:-etl2024}" \
    --notebook-dir=/app/notebooks