# Зайти в контейнер
docker-compose exec spark-master bash

# Найти spark-submit
which spark-submit || find / -name spark-submit 2>/dev/null  (export PATH=$PATH:/opt/spark/bin)

# Добавить в PATH (обычно один из этих путей)
export PATH=$PATH:/opt/spark/bin
export PATH=$PATH:/usr/local/spark/bin
export PATH=$PATH:$SPARK_HOME/bin

spark-master /opt/spark/bin/spark-submit \
    --master "local[2]" \
    --driver-memory 2g \
    --jars /jars/hadoop-aws-3.3.4.jar,/jars/aws-java-sdk-bundle-1.12.262.jar \
    /app/src/main_local.py

# Теперь запустить
spark-submit --master spark://172.22.0.4:7077 \
    --jars /jars/hadoop-aws-3.3.4.jar,/jars/aws-java-sdk-bundle-1.12.262.jar \
    /app/src/streaming/main.py

# Проверка ресурсов
curl -s http://localhost:8080/json/ | python -m json.tool | grep -E "(cores|memory|host)"

spark-submit \
    --master spark://172.22.0.4:7077 \
    --deploy-mode client \
    --executor-memory 2g \
    --executor-cores 2 \
    --total-executor-cores 4 \
    --driver-memory 2g \
    --conf spark.driver.host=shspark-master \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.locality.wait=0s \
    --jars /jars/hadoop-aws-3.3.4.jar,/jars/aws-java-sdk-bundle-1.12.262.jar \
    /app/src/streaming/main.py

spark-submit \
    --master spark://172.22.0.4:7077 \
    --deploy-mode client \
    --executor-memory 2g \
    --executor-cores 2 \
    --total-executor-cores 4 \
    --driver-memory 2g \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.locality.wait=0s \
    --jars /jars/hadoop-aws-3.3.4.jar,/jars/aws-java-sdk-bundle-1.12.262.jar \
    /app/src/streaming/main.py    

spark-submit \
    --master spark://172.22.0.4:7077 \
    --deploy-mode cluster \
    --executor-memory 2g \
    --executor-cores 2 \
    --total-executor-cores 4 \
    --driver-memory 2g \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.locality.wait=0s \
    --jars /jars/hadoop-aws-3.3.4.jar,/jars/aws-java-sdk-bundle-1.12.262.jar \
    /app/src/streaming/main.py

    --conf spark.driver.host=shspark-master \