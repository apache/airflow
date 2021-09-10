FROM ghcr.io/masami10/airflow/airflow-base:latest

COPY requirements-airflow-prod.txt /tmp/requirements.txt

COPY dags/ /default/dags/

COPY plugins/ /default/plugins/

ENV RUNTIME_ENV prod
ARG AIRFLOW_DEFAULT=/default

USER root

RUN set -ex \
    && chown -R airflow:airflow ${AIRFLOW_DEFAULT}/plugins/ \
    && chown -R airflow:airflow ${AIRFLOW_DEFAULT}/dags/

USER airflow

RUN set -ex \
    && ls -al  ${AIRFLOW_DEFAULT}/plugins/ \
    && sed -i 's/rbac = False/rbac = True/g' ${AIRFLOW_DEFAULT}/airflow.cfg \
    && cat ${AIRFLOW_DEFAULT}/airflow.cfg \
    && pip install -r /tmp/requirements.txt --user -i https://pypi.douban.com/simple
