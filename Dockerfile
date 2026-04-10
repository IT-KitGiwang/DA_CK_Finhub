FROM ubuntu:24.04
RUN apt-get update && apt-get install -y \
openjdk-21-jdk \
openssh-server \
openssh-client \
wget \
curl \
python3 \
python3-pip \
sudo \
nano \
&& rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV HADOOP_VERSION=3.4.3
ENV SPARK_VERSION=4.1.1
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin
RUN useradd -m -s /bin/bash dack15 && \
echo "dack15:dack15" | chpasswd && \
adduser dack15 sudo && \
echo "dack15 ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers
USER dack15
WORKDIR /home/dack15

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
chmod 0600 ~/.ssh/authorized_keys

RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
sudo mv hadoop-${HADOOP_VERSION} /opt/hadoop && \
sudo chown -R dack15:dack15 /opt/hadoop && \
rm hadoop-${HADOOP_VERSION}.tar.gz

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
sudo mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
sudo chown -R dack15:dack15 /opt/spark && \
rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

RUN sudo service ssh start
EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]