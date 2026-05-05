#!/bin/bash
# Production ETL запуск на Spark кластере
# Запускать: docker-compose exec spark-master bash /app/run_etl_production.sh

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "========================================"
echo "🚀 PRODUCTION ETL PIPELINE"
echo "========================================"

# Конфигурация кластера
SPARK_MASTER="spark://172.22.0.4:7077"
NUM_EXECUTORS=2
EXECUTOR_CORES=2
EXECUTOR_MEMORY="2g"
DRIVER_MEMORY="2g"
TOTAL_EXECUTOR_CORES=$((NUM_EXECUTORS * EXECUTOR_CORES))

echo ""
echo -e "${BLUE}Cluster Configuration:${NC}"
echo "  Master URL:     $SPARK_MASTER"
echo "  Executors:      $NUM_EXECUTORS (workers)"
echo "  Cores/Executor: $EXECUTOR_CORES"
echo "  Total Cores:    $TOTAL_EXECUTOR_CORES"
echo "  Executor Memory: $EXECUTOR_MEMORY"
echo "  Driver Memory:   $DRIVER_MEMORY"
echo ""

# Проверка доступности сервисов
echo -e "${BLUE}Checking services...${NC}"

# Проверка HDFS
if curl -s -f http://172.22.0.2:9870 > /dev/null 2>&1; then
    echo -e "  ${GREEN}✓${NC} HDFS NameNode (172.22.0.2:9870)"
else
    echo -e "  ${RED}✗${NC} HDFS NameNode - not accessible"
    exit 1
fi

# Проверка MinIO
if curl -s -f http://172.22.0.8:9302/minio/health/live > /dev/null 2>&1; then
    echo -e "  ${GREEN}✓${NC} MinIO (172.22.0.8:9302)"
else
    echo -e "  ${RED}✗${NC} MinIO - not accessible"
    exit 1
fi

# Проверка Spark Master
if curl -s -f http://172.22.0.4:8080 > /dev/null 2>&1; then
    echo -e "  ${GREEN}✓${NC} Spark Master (172.22.0.4:8080)"
else
    echo -e "  ${RED}✗${NC} Spark Master - not accessible"
    exit 1
fi

# Проверка живых воркеров
ALIVE_WORKERS=$(curl -s http://172.22.0.4:8080/json/ 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    workers = data.get('workers', [])
    alive = [w for w in workers if w.get('state') == 'ALIVE']
    print(len(alive))
except:
    print(0)
" 2>/dev/null || echo "0")

echo -e "  Alive workers: ${GREEN}${ALIVE_WORKERS}${NC}"

if [ "$ALIVE_WORKERS" -lt 1 ]; then
    echo -e "  ${RED}✗${NC} No alive workers! Check: docker-compose ps"
    echo "  Workers should be at: 172.22.0.5:8081 and 172.22.0.6:8081"
    exit 1
fi

# Проверка наличия исходных данных
echo ""
echo -e "${BLUE}Checking input data...${NC}"

if [ -f /app/data/raw/events/sessions.json ] || [ -d /app/data/raw/events/ ]; then
    EVENT_COUNT=$(ls /app/data/raw/events/*.json 2>/dev/null | wc -l)
    echo -e "  ${GREEN}✓${NC} Event files found: $EVENT_COUNT"
else
    echo -e "  ${YELLOW}⚠${NC}  No event files in /app/data/raw/events/"
    echo "  Run data generator first:"
    echo "  docker-compose exec jupyter python /app/src/generate_data.py"
fi

if [ -f /app/data/raw/products/products.csv ]; then
    echo -e "  ${GREEN}✓${NC} Products file found"
else
    echo -e "  ${RED}✗${NC} Products file not found"
fi

if [ -f /app/data/raw/users/users.csv ]; then
    echo -e "  ${GREEN}✓${NC} Users file found"
else
    echo -e "  ${RED}✗${NC} Users file not found"
fi

# Создание директории для логов в HDFS
echo ""
echo -e "${BLUE}Preparing HDFS...${NC}"
hdfs dfs -mkdir -p /spark-logs 2>/dev/null || true
hdfs dfs -mkdir -p /data/processed 2>/dev/null || true
hdfs dfs -mkdir -p /data/aggregated 2>/dev/null || true
echo -e "  ${GREEN}✓${NC} HDFS directories ready"

# Запуск Spark приложения
echo ""
echo -e "${BLUE}========================================"
echo "STARTING SPARK JOB"
echo -e "========================================${NC}"
echo ""

# Опции Spark для мониторинга
SPARK_CONFS=(
    --conf "spark.sql.adaptive.enabled=true"
    --conf "spark.sql.adaptive.coalescePartitions.enabled=true"
    --conf "spark.sql.adaptive.skewJoin.enabled=true"
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer"
    --conf "spark.sql.parquet.compression.codec=snappy"
    --conf "spark.sql.shuffle.partitions=8"
    --conf "spark.default.parallelism=8"
    --conf "spark.eventLog.enabled=true"
    --conf "spark.eventLog.dir=hdfs://172.22.0.2:9000/spark-logs"
    --conf "spark.ui.port=4040"
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///opt/spark/conf/log4j.properties"
)

JARS="/jars/hadoop-aws-3.3.4.jar,/jars/aws-java-sdk-bundle-1.12.262.jar"

# Запуск
spark-submit \
    --master "$SPARK_MASTER" \
    --conf spark.driver.host=shspark-master \
    --deploy-mode client \
    --executor-memory "$EXECUTOR_MEMORY" \
    --executor-cores "$EXECUTOR_CORES"  \
    --total-executor-cores "$TOTAL_EXECUTOR_CORES" \
    --driver-memory "$DRIVER_MEMORY" \
    --name "ETL-Pipeline-Production-$(date +%Y%m%d_%H%M%S)" \
    --num-executors "$NUM_EXECUTORS" \
    "${SPARK_CONFS[@]}" \
    --jars "$JARS" \
    /app/src/etl/main.py
    
    #--executor-memory "$EXECUTOR_MEMORY" \
    #--executor-cores "$EXECUTOR_CORES" \
    #--deploy-mode client \
    #--total-executor-cores "$TOTAL_EXECUTOR_CORES" \
    #--driver-memory "$DRIVER_MEMORY" \

EXIT_CODE=$?

# Проверка результата
echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}========================================"
    echo "✅ ETL PIPELINE COMPLETED SUCCESSFULLY!"
    echo -e "========================================${NC}"
    echo ""
    echo "📊 Monitor your results:"
    echo "  Spark Master UI:  http://localhost:8080"
    echo "  Spark Job UI:     http://localhost:4040"
    echo "  MinIO Console:    http://localhost:9006"
    echo "  HDFS NameNode:    http://localhost:9870"
    echo "  Jupyter:          http://localhost:8888"
    echo ""
    echo "💾 Data locations:"
    echo "  Bronze (MinIO):   s3a://bronze/"
    echo "  Silver (HDFS):    hdfs://172.22.0.2:9000/data/processed/"
    echo "  Gold (MinIO):     s3a://gold/"
    echo ""
    
    # Показываем статистику
    echo "📈 Quick stats:"
    echo -n "  Bronze events: "
    hdfs dfs -ls -R /data/processed/events/ 2>/dev/null | grep ".parquet" | wc -l | xargs echo "files"
    
else
    echo -e "${RED}========================================"
    echo "❌ ETL PIPELINE FAILED"
    echo -e "========================================${NC}"
    echo ""
    echo "🔍 Debug steps:"
    echo "  1. Check Spark Master UI: http://localhost:8080"
    echo "  2. Check logs: docker-compose logs spark-master"
    echo "  3. Check workers: docker-compose logs spark-worker-1"
    echo "  4. Check HDFS: docker-compose exec namenode hdfs dfs -ls /"
fi

exit $EXIT_CODE