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
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://root:root@postgres-airflow:5432/airflow

# Don't read the raw revisions git-sync puts in the volume, we always want to read from the symlink
# A hack, how else can we do this
RUN mkdir -p $AIRFLOW_HOME/dags && echo "rev-[a-zA-Z0-9]+" > $AIRFLOW_HOME/dags/.airflowignore


COPY bootstrap.sh /bootstrap.sh
RUN chmod +x /bootstrap.sh

ENTRYPOINT ["/bootstrap.sh"]
