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
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin

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

# Phân quyền cho dack15
RUN chown -R dack15:dack15 /opt/hadoop /opt/spark

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

# Loại bỏ ký tự Windows CRLF (\r) trong file workers để Linux Hadoop đọc không bị lỗi (hostname contains invalid characters)
RUN sed -i 's/\r$//' /opt/hadoop/etc/hadoop/workers && \
    sed -i 's/\r$//' /opt/spark/conf/workers

# Chép entrypoint script
COPY --chown=dack15:dack15 entrypoint.sh /home/dack15/entrypoint.sh
RUN sed -i 's/\r$//' /home/dack15/entrypoint.sh && \
    chmod +x /home/dack15/entrypoint.sh

# Chạy Entrypoint
ENTRYPOINT ["/home/dack15/entrypoint.sh"]
