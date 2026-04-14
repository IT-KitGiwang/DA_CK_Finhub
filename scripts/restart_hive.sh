#!/bin/bash
# Kill all old Hive processes
kill -9 1379 2251 2>/dev/null
sleep 3

# Clean Derby locks
rm -f /opt/hive/metastore_db/*.lck

# Set RAM
export HIVE_OPTS="-Xmx1024m"

# Start Metastore first
echo "Starting Metastore..."
nohup /opt/hive/bin/hive --service metastore > /opt/hive/logs/metastore.log 2>&1 &

# Wait until port 9083 is open
echo "Waiting for Metastore on port 9083..."
for i in $(seq 1 60); do
    if cat /proc/net/tcp 2>/dev/null | grep -q ':2383'; then
        echo "Metastore is UP!"
        break
    fi
    sleep 2
done

# Start HiveServer2
echo "Starting HiveServer2..."
nohup /opt/hive/bin/hive --service hiveserver2 > /opt/hive/logs/hiveserver2.log 2>&1 &

# Wait until port 10000 is open
echo "Waiting for HiveServer2 on port 10000..."
for i in $(seq 1 90); do
    if cat /proc/net/tcp 2>/dev/null | grep -q ':2710'; then
        echo "HiveServer2 is UP!"
        break
    fi
    sleep 2
done

echo "=== Final JPS ==="
jps
