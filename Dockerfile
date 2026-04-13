FROM ubuntu:22.04

USER root

# Cài đặt OpenJDK 21 và các công cụ cơ bản
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-21-jdk curl wget vim ssh rsync sudo netcat && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Thiết lập biến môi trường
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV HADOOP_VERSION=3.5.0
ENV SPARK_VERSION=4.1.0
ENV SCALA_VERSION=3.8
ENV HIVE_VERSION=4.2.0
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ENV HIVE_HOME=/opt/hive
ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$HIVE_HOME/bin

# Tạo user dack15
RUN useradd -m -s /bin/bash dack15 && \
    echo "dack15 ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Tải và giải nén Hadoop (Xóa luôn thư mục docs để giảm dung lượng image trống)
RUN wget -q "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ && \
    mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm hadoop-${HADOOP_VERSION}.tar.gz && \
    rm -rf ${HADOOP_HOME}/share/doc && \
    mkdir -p /opt/hadoop/logs

# Tải và giải nén Spark (with Hadoop 3 - Xóa examples và data mẫu)
# Chú ý: Dùng link archive thay vì downloads để tránh lỗi 404 khi Spark cập nhật bản vá.
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    rm -rf ${SPARK_HOME}/examples ${SPARK_HOME}/data && \
    mkdir -p /opt/spark/logs

# Tải và giải nén Apache Hive
RUN wget -q "https://downloads.apache.org/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz" && \
    tar -xzf apache-hive-${HIVE_VERSION}-bin.tar.gz -C /opt/ && \
    mv /opt/apache-hive-${HIVE_VERSION}-bin ${HIVE_HOME} && \
    rm apache-hive-${HIVE_VERSION}-bin.tar.gz

# Tải bổ sung derbytools (Hive 4.x thiếu file này để chạy Derby embedded)
RUN wget -q "https://repo1.maven.org/maven2/org/apache/derby/derbytools/10.17.1.0/derbytools-10.17.1.0.jar" -P ${HIVE_HOME}/lib/

# Loại bỏ SLF4J bị trùng lặp giữa Hadoop và Hive để tránh lỗi khởi động Hive
RUN rm -f ${HIVE_HOME}/lib/log4j-slf4j-impl-*.jar || true

# Tạo thư mục logs cho Hive
RUN mkdir -p ${HIVE_HOME}/logs

# Phân quyền cho dack15
RUN chown -R dack15:dack15 /opt/hadoop /opt/spark /opt/hive

USER dack15
WORKDIR /home/dack15

# Cấu hình SSH passwordless cho dack15 (Hadoop cần để liên lạc giữa các node)
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    echo "Host *\n  StrictHostKeyChecking no\n  UserKnownHostsFile=/dev/null" > ~/.ssh/config

# Copy thư mục cài đặt mặc định để entrypoint map config
COPY --chown=dack15:dack15 config/ /opt/hadoop/etc/hadoop/
COPY --chown=dack15:dack15 config/spark/ /opt/spark/conf/

# Copy Hive configuration (hive-site.xml)
COPY --chown=dack15:dack15 config/hive-site.xml /opt/hive/conf/
COPY --chown=dack15:dack15 config/hive-site.xml /opt/spark/conf/

# Loại bỏ ký tự Windows CRLF (\r) trong các file cấu hình để Linux Hadoop/Spark đọc không bị lỗi
RUN find /opt/hadoop/etc/hadoop/ -type f -exec sed -i 's/\r$//' {} + && \
    find /opt/spark/conf/ -type f -exec sed -i 's/\r$//' {} + && \
    find /opt/hive/conf/ -type f -exec sed -i 's/\r$//' {} +

# Chép các script xử lý Python vào container
COPY --chown=dack15:dack15 *.py /home/dack15/

# Chép entrypoint script
COPY --chown=dack15:dack15 entrypoint.sh /home/dack15/entrypoint.sh
RUN sed -i 's/\r$//' /home/dack15/*.py && \
    sed -i 's/\r$//' /home/dack15/entrypoint.sh && \
    chmod +x /home/dack15/entrypoint.sh

# Chạy Entrypoint
ENTRYPOINT ["/home/dack15/entrypoint.sh"]
