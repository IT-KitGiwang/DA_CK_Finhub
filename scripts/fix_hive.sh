#!/bin/bash
# Kill all RunJar (Hive) processes
for pid in $(jps | awk '/RunJar/ {print $1}'); do
    echo "Killing RunJar PID: $pid"
    kill -9 $pid
done
sleep 3

# Verify they are dead
echo "=== Current Java Processes ==="
jps

# Clean and reinit metastore
rm -rf /opt/hive/metastore_db
/opt/hive/bin/schematool -dbType derby -initSchema 2>&1 | tail -3
chown -R dack15:dack15 /opt/hive/metastore_db

# Start metastore
echo "=== Starting Metastore ==="
nohup /opt/hive/bin/hive --service metastore > /opt/hive/logs/metastore.log 2>&1 &

# Wait for metastore to be ready on port 9083
echo "Waiting for Metastore to start on port 9083..."
for i in $(seq 1 30); do
    if cat /proc/net/tcp | grep -q ':2383'; then
        echo "Metastore is UP on port 9083!"
        break
    fi
    sleep 2
done

# Start HiveServer2
echo "=== Starting HiveServer2 ==="
nohup /opt/hive/bin/hive --service hiveserver2 > /opt/hive/logs/hiveserver2.log 2>&1 &

# Wait for HiveServer2 to be ready on port 10000
echo "Waiting for HiveServer2 to start on port 10000..."
for i in $(seq 1 60); do
    if cat /proc/net/tcp | grep -q ':2710'; then
        echo "HiveServer2 is UP on port 10000!"
        break
    fi
    sleep 2
done

echo "=== Final Status ==="
jps
cat /proc/net/tcp | head -20
