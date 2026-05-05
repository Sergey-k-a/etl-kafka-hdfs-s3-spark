.PHONY: etl_streaming etl_batch etl

# Переменные
SPARK_MASTER := spark://172.22.0.4:7077
JARS := /jars/hadoop-aws-3.3.4.jar,/jars/aws-java-sdk-bundle-1.12.262.jar,/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar
master_cluster :=  spark://172.22.0.4:7077
master_local :=  local[4]

etl_parquet_local:
	docker-compose exec spark-master bash -c '\
		unset HADOOP_HOME SPARK_HOME; \
		spark-submit --master "local[4]" --driver-memory 2g \
			--jars /jars/hadoop-aws-3.3.4.jar,/jars/aws-java-sdk-bundle-1.12.262.jar \
			/app/src/streaming/main.py parquet'

## Batch ETL из Parquet (Spark Streaming)
etl_parquet_cluster: 
	docker-compose exec spark-master spark-submit \
		--master $(master_cluster) \
		--driver-memory 2g \
		--executor-memory 2g \
		--executor-cores 2 \
		--total-executor-cores 4 \
		--conf spark.cores.max=4 \
		--conf spark.driver.bindAddress=0.0.0.0 \
		--conf spark.driver.host=shspark-master \
		--jars $(JARS) \
    /app/src/streaming/main.py parquet

# Batch ETL из JSON (Kafka Connect)
etl_json_cluster: 
	docker-compose exec spark-master spark-submit \
		--master $(master_cluster) \
		--driver-memory 2g \
		--executor-memory 2g \
		--executor-cores 2 \
		--total-executor-cores 4 \
		--conf spark.cores.max=4 \
		--conf spark.driver.bindAddress=0.0.0.0 \
		--conf spark.driver.host=shspark-master \
		--jars $(JARS) \
    /app/src/streaming/main.py json

# Запустить генератор данных в Kafka
etl_streaming_generate_on:
	docker-compose exec spark-master python3 /app/src/streaming/event_producer.py

# Запустить Spark Streaming (Kafka → Bronze)
etl_streaming_EL: 
	docker-compose exec spark-master spark-submit \
		--master $(SPARK_MASTER) \
		--deploy-mode client \
		--executor-memory 1g \
		--executor-cores 1 \
		--total-executor-cores 2 \
		--driver-memory 1g \
		--conf spark.driver.host=shspark-master \
		--conf spark.cores.max=2 \
		--conf spark.sql.shuffle.partitions=4 \
		--conf spark.dynamicAllocation.enabled=false \
		--conf spark.locality.wait=0s \
		--jars $(JARS) \
		/app/src/streaming/stream_processor.py


# Проверка
check-kafka:
	@echo "Checking Kafka topics..."
	docker exec shkafka kafka-topics --bootstrap-server localhost:29092 --list

check-spark:
	@echo "Spark Master UI: http://localhost:8080"
	curl -s http://localhost:8080/json/ | python3 -m json.tool 2>/dev/null || echo "Cannot connect to Spark Master"


# Просмотр логов
logs-spark:
	docker-compose logs -f spark-master spark-worker-1 spark-worker-2

logs-kafka:
	docker-compose logs -f kafka zookeeper

# Полный перезапуск
restart: clean up

# Очистка
clean:
	docker-compose down -v

# Создание бакетов ...
init-storage:
	@echo "Creating MinIO buckets..."
	docker-compose exec minio mc alias set local http://localhost:9302 minioadmin minioadmin 2>/dev/null || true
	@for bucket in bronze silver gold logs; do \
		docker-compose exec minio mc mb local/$$bucket 2>/dev/null || true; \
	done
	@echo "Creating HDFS directories..."
	docker-compose exec namenode bash -c 'hdfs dfs -mkdir -p /tmp/spark-checkpoints' 2>/dev/null || true
	docker-compose exec namenode bash -c 'hdfs dfs -mkdir -p /etl_checkpoint' 2>/dev/null || true
	docker-compose exec namenode bash -c 'hdfs dfs -chmod -R 777 /tmp /etl_checkpoint ' 2>/dev/null || true
	@echo "✓ Storage initialized"

