FROM python:3.6-slim
COPY setup.* /opt/airflow/
COPY airflow /opt/airflow/airflow

ARG AIRFLOW_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS="all"
ARG PYTHON_DEPS=""
ARG buildDeps="freetds-dev libkrb5-dev libsasl2-dev libssl-dev libffi-dev libpq-dev git"
ARG APT_DEPS="$buildDeps libsasl2-dev freetds-bin build-essential default-libmysqlclient-dev apt-utils curl rsync netcat locales"

WORKDIR /opt/airflow
RUN set -x \
    && export SLUGIFY_USES_TEXT_UNIDECODE=yes \
    && apt update \
    && if [ -n "${APT_DEPS}" ]; then apt install -y $APT_DEPS; fi \
    && if [ -n "${PYTHON_DEPS}" ]; then pip install --no-cache-dir ${PYTHON_DEPS}; fi \
    && pip install --no-cache-dir -e .[$AIRFLOW_DEPS] \
    && apt purge --auto-remove -yqq $buildDeps \
    && apt autoremove -yqq --purge \
    && apt clean

WORKDIR $AIRFLOW_HOME
RUN mkdir -p $AIRFLOW_HOME
COPY scripts/docker/entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["--help"]
