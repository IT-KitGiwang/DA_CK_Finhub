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

    # Đảm bảo HDFS đã lên để Spark có thể tạo thư mục log trên HDFS
    echo "Wait 10s for HDFS to initialize before creating Spark History folder..."
    sleep 10
    $HADOOP_HOME/bin/hdfs dfs -mkdir -p /spark-logs
    $HADOOP_HOME/bin/hdfs dfs -chown -R dack15:dack15 /spark-logs

    # Xóa lock file của Derby nếu tồn tại (tránh lỗi database is read-only)
    rm -f /opt/hive/metastore_db/*.lck || true
    sudo chown -R dack15:dack15 /opt/hive/metastore_db || true

    # Cấu hình RAM cho Hive
    export HIVE_OPTS="-Xmx1024m"

    # Init Hive Metastore
    if [ ! -d "/opt/hive/metastore_db" ]; then
        echo "Initializing Hive Metastore Schema..."
        $HIVE_HOME/bin/schematool -dbType derby -initSchema || true
    fi

    echo "Starting Hive Metastore..."
    nohup $HIVE_HOME/bin/hive --service metastore > /opt/hive/logs/metastore.log 2>&1 &
    
    echo "Starting HiveServer2..."
    nohup $HIVE_HOME/bin/hive --service hiveserver2 > /opt/hive/logs/hiveserver2.log 2>&1 &

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
