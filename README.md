# 🏗️ Data Lakehouse ETL Pipeline

Real-time потоковая обработка e-commerce данных: Kafka → MinIO/HDFS → Silver → Gold

---

# Архитектура

Kafka → Kafka Connect (JSON) → MinIO Bronze
Kafka → Spark Streaming (Parquet) → MinIO Bronze
MinIO Bronze → Batch ETL (Spark) → HDFS Silver + MinIO Silver → HDFS Gold (4 витрины)

---

# Сервисы

Spark Master  :8080  Управление кластером
MinIO         :9006  S3-хранилище (minioadmin/minioadmin)
HDFS NameNode :9870  Распределённая ФС
Kafka         :9092  Брокер сообщений
Kafka Connect :8083  S3 Sink коннектор
Kafka UI      :8084  Мониторинг
Jupyter       :8888  Разработка (token: etl2024)


# Слои данных

Bronze — MinIO — JSON + Parquet — Сырые события из Kafka
Silver — HDFS + MinIO — Parquet (Snappy) — Очищенные и обогащённые данные
Gold   — HDFS — Parquet (Snappy) — 4 бизнес-витрины

Чтение:   ETL читает сырые данные из S3 (MinIO)
Запись:   Silver пишется в HDFS + MinIO, Gold — в HDFS

Gold витрины:
  hourly_metrics      — почасовые метрики
  user_activity       — активность пользователей
  product_analytics   — аналитика по продуктам
  session_analytics   — агрегация сессий


# Порядок запуска

# 1 Скачать JAR-зависимости и plugin
bash setup_kafka_connect.sh
bash download_jars.sh

# 2 Поднять инфраструктуру
docker-compose build
docker-compose up -d
sleep 60

# 3 Проверить статус
docker-compose ps

# 4 Инициализировать хранилища
make init-storage

# 5 Зарегистрировать Kafka Connect
make -f makefile.kafka-connect etl_streaming_connector_registration

# 6 Запустить генератор данных
make etl_streaming_generate_on

# 7 Запустить Spark Streaming (отдельный терминал)
make etl_streaming_EL

# 8 Запустить Batch ETL
make etl_parquet_cluster   # Parquet (Spark Streaming)
make etl_json_cluster      # JSON (Kafka Connect)

# 9 Проверить результаты
docker-compose exec namenode hdfs dfs -ls -R /data/



## Makefile команды

make init-storage                  Создать бакеты MinIO и папки HDFS
make etl_streaming_generate_on     Генератор событий Kafka
make etl_streaming_EL              Spark Streaming (Kafka→Bronze)
make etl_parquet_cluster           Batch ETL из Parquet
make etl_json_cluster              Batch ETL из JSON



# Остановка

docker-compose down         Остановить сервисы
docker-compose down -v      Полная очистка с данными



# Особенности

- Два подхода: Kafka Connect (zero-code) + Spark Streaming (трансформации)
- Инкрементальная загрузка с чекпоинтами в HDFS
- Дедупликация по event_id (идемпотентность)
- Partition pruning при чтении из S3
- S3 для скорости + HDFS для надёжности

# Стек

Apache Spark 3.5 | Kafka 7.6 | MinIO | HDFS 3.2 | Docker | Python 3.9 | PySpark | Kafka Connect