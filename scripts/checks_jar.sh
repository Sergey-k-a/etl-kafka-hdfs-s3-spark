# scripts/check_jars.sh
#!/bin/bash

echo "🔍 Проверка JAR файлов для S3/MinIO..."
echo "========================================"

JAR_DIR="/opt/spark/jars"

echo "1. Проверяем наличие hadoop-aws:"
ls -la $JAR_DIR/hadoop-aws*.jar 2>/dev/null || echo "❌ Не найден hadoop-aws JAR"

echo ""
echo "2. Проверяем наличие aws-java-sdk:"
ls -la $JAR_DIR/aws-java-sdk*.jar 2>/dev/null || echo "❌ Не найден aws-java-sdk JAR"

echo ""
echo "3. Проверяем размер JAR файлов:"
find $JAR_DIR -name "*aws*.jar" -o -name "*s3*.jar" -o -name "*hadoop-aws*.jar" | xargs ls -lh 2>/dev/null || echo "Файлы не найдены"

echo ""
echo "4. Общее количество JAR файлов:"
ls -1 $JAR_DIR | wc -l

echo "========================================"