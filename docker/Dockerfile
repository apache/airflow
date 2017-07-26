FROM ubuntu:16.04

# install deps
RUN apt-get update -y && apt-get install -y \
        wget \
        python-dev \
        python-pip \
        libczmq-dev \
        libcurlpp-dev \
        curl \
        libssl-dev \
        git \
        inetutils-telnet \
        bind9utils

RUN pip install -U setuptools && \
    pip install -U pip

RUN pip install kubernetes && \
    pip install cryptography && \
    pip install psycopg2==2.7.1

# install airflow
COPY airflow.tar.gz /tmp/airflow.tar.gz
RUN pip install /tmp/airflow.tar.gz

# prep airflow
ENV AIRFLOW_HOME=/root/airflow
ENV AIRFLOW__CORE__EXECUTOR=KubernetesExecutor

COPY bootstrap.sh /bootstrap.sh
RUN chmod +x /bootstrap.sh

ENTRYPOINT ["/bootstrap.sh"]
