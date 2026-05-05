#!/bin/bash
# Автоматическая установка Kafka Connect S3 Sink плагина

PLUGIN_DIR="./kafka-connect-plugins_test"
PLUGIN_URL="https://d2p6pa21dvn84.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3/versions/10.5.4/confluentinc-kafka-connect-s3-10.5.4.zip"

mkdir -p "$PLUGIN_DIR"

if [ ! -f "$PLUGIN_DIR/confluentinc-kafka-connect-s3-10.5.4/lib/kafka-connect-s3-10.5.4.jar" ]; then
    echo "Downloading Kafka Connect S3 plugin..."
    wget -q -O /tmp/s3-plugin.zip "$PLUGIN_URL"
    unzip -q -o /tmp/s3-plugin.zip -d "$PLUGIN_DIR"
    rm /tmp/s3-plugin.zip
    echo "✓ Plugin installed"
else
    echo "✓ Plugin already installed"
fi