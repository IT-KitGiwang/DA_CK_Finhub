export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_LOG_DIR=$HADOOP_HOME/logs

# Bỏ qua warning về user root (tuy user hiện tại là dack15 rồi)
export HDFS_NAMENODE_USER=dack15
export HDFS_DATANODE_USER=dack15
export HDFS_SECONDARYNAMENODE_USER=dack15
export YARN_RESOURCEMANAGER_USER=dack15
export YARN_NODEMANAGER_USER=dack15
