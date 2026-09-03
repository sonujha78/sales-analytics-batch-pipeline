FROM eclipse-temurin:8-jdk-jammy

ENV HADOOP_VERSION=3.2.1
ENV SQOOP_VERSION=1.4.7
ENV HADOOP_HOME=/opt/hadoop
ENV SQOOP_HOME=/opt/sqoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$SQOOP_HOME/bin

RUN apt-get update && apt-get install -y wget curl netcat procps && rm -rf /var/lib/apt/lists/*

# Hadoop client (needed for HDFS access, but not running any daemons)
RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt \
    && mv /opt/hadoop-${HADOOP_VERSION} $HADOOP_HOME \
    && rm hadoop-${HADOOP_VERSION}.tar.gz

# Sqoop
RUN wget -q https://archive.apache.org/dist/sqoop/${SQOOP_VERSION}/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz \
    && tar -xzf sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz -C /opt \
    && mv /opt/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0 $SQOOP_HOME \
    && rm sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz

# MySQL JDBC driver
RUN wget -q https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar \
    -O $SQOOP_HOME/lib/mysql-connector-java.jar

ENV HADOOP_CLASSPATH=$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*

WORKDIR /opt/sqoop

CMD ["sleep", "infinity"]

# Sqoop 1.4.7 needs commons-lang 2.x, not bundled by default
RUN wget -q https://repo1.maven.org/maven2/commons-lang/commons-lang/2.6/commons-lang-2.6.jar \
    -O $SQOOP_HOME/lib/commons-lang-2.6.jar

# Sqoop's launch scripts reference this dir even when empty; missing it causes
# YARN container localization to fail with FileNotFoundException
RUN mkdir -p /opt/sqoop/libjars

# libjars must contain at least one file, else Sqoop's glob-based staging
# silently fails to create the HDFS staging dir, causing YARN container
# localization to fail with FileNotFoundException on the libjars symlink
RUN mkdir -p /opt/sqoop/libjars && cp /opt/sqoop/lib/commons-lang-2.6.jar /opt/sqoop/libjars/
