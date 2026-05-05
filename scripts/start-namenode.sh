#!/bin/bash
set -e

echo "=== Starting NameNode ==="
mkdir -p /hadoop/dfs/name

if [ ! -d /hadoop/dfs/name/current ]; then
    echo "Formatting NameNode..."
    hdfs namenode -format -force || { echo "ERROR: Format failed!"; exit 1; }
fi

echo "Starting NameNode service..."
exec hdfs namenode \
    -D fs.defaultFS=hdfs://172.22.0.2:9000 \
    -D dfs.namenode.rpc-address=172.22.0.2:9000 \
    -D dfs.namenode.http-address=172.22.0.2:9870 \
    -D dfs.namenode.servicerpc-address=172.22.0.2:9869 \
    -D dfs.namenode.lifeline.rpc-address=172.22.0.2:9868 \
    -D dfs.namenode.datanode.registration.ip-hostname-check=false \
    -D dfs.client.use.datanode.hostname=false \
    -D dfs.datanode.use.datanode.hostname=false \
    -D hadoop.security.token.service.use_ip=true