FROM docker.pkg.github.com/masami10/airflow/airflow-base:latest

COPY requirements-airflow-prod.txt /tmp/requirements.txt

COPY dags/ /usr/local/airflow/dags/

COPY plugins/ /usr/local/airflow/plugins/

ENV RUNTIME_ENV prod

USER root

ENV PYTHONPATH "${PYTHONPATH}:/usr/local/airflow/plugins"

RUN set -ex \
    && chown -R airflow:airflow /usr/local/airflow/plugins/ \
    && chown -R airflow:airflow /usr/local/airflow/dags/


USER airflow

RUN set -ex \
    && ls -al  /usr/local/airflow/plugins/ \
    && sed -i 's/rbac = False/rbac = True/g' ${AIRFLOW_HOME}/airflow.cfg \
    && cat ${AIRFLOW_HOME}/airflow.cfg \
    && pip install -r /tmp/requirements.txt --user -i https://pypi.douban.com/simple
