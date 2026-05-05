#!/bin/bash
set -e

echo "=== Starting DataNode ==="
mkdir -p /hadoop/dfs/data

echo "Waiting for NameNode..."
for i in {1..30}; do
    if nc -z 172.22.0.2 9000; then
        echo "NameNode is ready!"
        break
    fi
    [ $i -eq 30 ] && { echo "ERROR: NameNode timeout!"; exit 1; }
    sleep 2
done

exec hdfs datanode \
    -D dfs.datanode.data.dir=/hadoop/dfs/data \
    -D dfs.datanode.address=0.0.0.0:9866 \
    -D dfs.datanode.http.address=0.0.0.0:9864 \
    -D dfs.datanode.ipc.address=0.0.0.0:9867