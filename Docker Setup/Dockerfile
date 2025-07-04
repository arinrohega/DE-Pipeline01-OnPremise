#-------------------VERSION DEL CONTAINER----------
FROM apache/airflow:2.8.1-python3.10

#-----INSTALAR DEPENDENCIAS DE SPARK Y HDFS----------
USER root

RUN apt-get update && \
    apt-get install -y \
        gcc \
        python3-dev \
        openjdk-17-jdk \
        build-essential \
        krb5-user \
        libkrb5-dev \
        libsasl2-dev \
        libsasl2-modules-gssapi-mit && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

#------PIP INSTALLS-----------------------------
USER airflow

RUN pip install --no-cache-dir pyspark==3.5.1

RUN pip install --no-cache-dir delta-spark==3.2.0 --no-deps

RUN pip install apache-airflow-providers-apache-spark --constraint \
  "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"
 
RUN pip install apache-airflow-providers-apache-hdfs --constraint \
    "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"

#------AIRFLOW CONNECTIONS----------------------
RUN airflow connections add \
    --conn-type webhdfs \
    --conn-host hadoop-namenode \
    --conn-port 9870 \
    --conn-extra '{"proxy_user": "usuario"}' \
    webhdfs_default || true

RUN airflow connections add \
    --conn-type spark \
    --conn-host spark://spark \
    --conn-port 7077 \
    spark-conn || true
