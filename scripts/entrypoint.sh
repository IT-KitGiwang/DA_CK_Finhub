#!/bin/bash

# Khởi động sshd service de Hadoop Master co the ket noi cac Slave
sudo service ssh start

# Lay mode tu bien moi truong
MODE=${NODE_TYPE:-slave}

# Cấp lại quyền cho thư mục data bị Docker mount mặc định làm root
sudo chown -R dack15:dack15 /opt/hadoop/data

# Cau hinh IP cua NameNode cho Spark Environment
echo "export SPARK_MASTER_HOST=master" > /opt/spark/conf/spark-env.sh
echo "export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64" >> /opt/spark/conf/spark-env.sh

if [ "$MODE" = "master" ]; then
    echo "--- Khởi chạy Master Node ---"
    
    # Kiem tra va Format NameNode neu chua format (HDFS name dir chua co data)
    if [ ! -d "/opt/hadoop/data/nameNode/current" ]; then
        echo "Formatting NameNode..."
        $HADOOP_HOME/bin/hdfs namenode -format -force
    fi
    
    # Khoi tao DFS va YARN tu Master
    echo "Starting Hadoop HDFS (NameNode + DataNodes)..."
    $HADOOP_HOME/sbin/start-dfs.sh
    
    echo "Starting Hadoop YARN (ResourceManager + NodeManagers)..."
    $HADOOP_HOME/sbin/start-yarn.sh
    
    # Khoi chay Job History Server
    $HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver

    # Đảm bảo HDFS đã lên (chờ tối đa 30s)
    echo "Waiting for HDFS to leave safe mode..."
    $HADOOP_HOME/bin/hdfs dfsadmin -safemode wait || sleep 10

    # Tạo các thư mục cần thiết trên HDFS
    echo "Creating HDFS directories..."
    $HADOOP_HOME/bin/hdfs dfs -mkdir -p /spark-logs
    $HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/hive/warehouse
    $HADOOP_HOME/bin/hdfs dfs -mkdir -p /tmp/hive
    $HADOOP_HOME/bin/hdfs dfs -chmod -R 777 /user/hive
    $HADOOP_HOME/bin/hdfs dfs -chmod -R 777 /tmp/hive
    $HADOOP_HOME/bin/hdfs dfs -chmod -R 777 /spark-logs

    # [FIX #5] Cấp RAM 1024MB TRỰC TIẾP trong shell — Hive 4.x KHÔNG đọc hive-env.sh
    export HADOOP_HEAPSIZE=1024
    export HADOOP_CLIENT_OPTS="-Xmx1024m -XX:+UseG1GC"

    # ==============================================================
    # BƯỚC 1: Khởi động METASTORE với conf RIÊNG (không có hive.metastore.uris)
    # ==============================================================
    echo "Setting up Metastore config (isolated, NO thrift URI)..."
    mkdir -p /tmp/metastore_conf
    cat > /tmp/metastore_conf/hive-site.xml << 'EOF'
<?xml version="1.0"?>
<configuration>
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:derby:;databaseName=/opt/hive/metastore_db;create=true</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>org.apache.derby.jdbc.EmbeddedDriver</value>
    </property>
    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>hdfs://master:9000/user/hive/warehouse</value>
    </property>
    <property>
        <name>hive.metastore.schema.verification</name>
        <value>false</value>
    </property>
    <property>
        <name>hive.server2.enable.doAs</name>
        <value>false</value>
    </property>
</configuration>
EOF

    # Xóa lock file của Derby
    rm -f /opt/hive/metastore_db/*.lck 2>/dev/null || true
    sudo chown -R dack15:dack15 /opt/hive/metastore_db 2>/dev/null || true

    # [FIX #3] Init schema: kiểm tra bằng schematool -info thay vì chỉ check folder
    if ! HIVE_CONF_DIR=/tmp/metastore_conf $HIVE_HOME/bin/schematool -dbType derby -info > /dev/null 2>&1; then
        echo "Metastore schema missing or corrupt. Re-initializing..."
        rm -rf /opt/hive/metastore_db
        HIVE_CONF_DIR=/tmp/metastore_conf $HIVE_HOME/bin/schematool -dbType derby -initSchema 2>&1
    fi

    # Khởi động Metastore với conf RIÊNG
    echo "Starting Hive Metastore (isolated conf)..."
    HIVE_CONF_DIR=/tmp/metastore_conf nohup $HIVE_HOME/bin/hive --service metastore \
        > /opt/hive/logs/metastore.log 2>&1 &
    
    # ==============================================================
    # BƯỚC 2: Chờ Metastore mở cổng 9083
    # ==============================================================
    echo "Waiting for Hive Metastore on port 9083 (max 120s)..."
    METASTORE_UP=false
    for i in $(seq 1 60); do
        if nc -z localhost 9083 2>/dev/null; then
            echo ">>> Metastore is UP on port 9083! (after ${i}*2s)"
            METASTORE_UP=true
            break
        fi
        sleep 2
    done

    if [ "$METASTORE_UP" = "false" ]; then
        echo "ERROR: Metastore did not start. Log:"
        cat /opt/hive/logs/metastore.log
    fi

    # ==============================================================
    # [FIX FINAL] Dùng Spark Thrift Server thay cho HiveServer2
    # Spark Thrift Server cung cấp CÙNG giao thức Thrift trên port 10000
    # nhưng ổn định hơn trên Hive 4.x (không bị jline shutdown loop)
    # Superset kết nối y hệt: hive://dack15@master:10000/default?auth=NOSASL
    # ==============================================================
    echo "Starting Spark Thrift Server on port 10000..."
    $SPARK_HOME/sbin/start-thriftserver.sh \
        --master local[2] \
        --name "Spark-Thrift-Server" \
        --conf spark.sql.hive.metastore.version=4.0.0 \
        --conf spark.sql.hive.metastore.jars=path \
        --conf spark.sql.hive.metastore.jars.path=file:///opt/hive/lib/* \
        --hiveconf hive.metastore.uris=thrift://master:9083 \
        --hiveconf hive.metastore.warehouse.dir=hdfs://master:9000/user/hive/warehouse \
        --hiveconf hive.server2.thrift.port=10000 \
        --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
        --hiveconf hive.server2.transport.mode=binary \
        --hiveconf hive.server2.authentication=NOSASL \
        --hiveconf hive.server2.enable.doAs=false \
        --hiveconf hive.metastore.schema.verification=false

    # Khoi chay Spark Master
    echo "Starting Spark Master..."
    $SPARK_HOME/sbin/start-master.sh
    
    # Tao signal giu Container
    echo "Master started successfully. Listening logs..."
    tail -f $HADOOP_HOME/logs/* $SPARK_HOME/logs/*
    
elif [ "$MODE" = "slave" ]; then
    echo "--- Khởi chạy Slave Node ($HOSTNAME) ---"
    
    # Spark Worker ket noi toi master theo uri spark://master:7077
    echo "Starting Spark Worker..."
    $SPARK_HOME/sbin/start-worker.sh spark://master:7077
    
    echo "Slave started successfully. Wait for commands from master. Listening logs..."
    tail -f $HADOOP_HOME/logs/* $SPARK_HOME/logs/*

else
    echo "Khong hieu NODE_TYPE=$MODE. Phat tin hieu stop."
    exit 1
fi
