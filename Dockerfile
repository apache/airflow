FROM python:3.6
COPY setup.* /opt/airflow/
COPY airflow /opt/airflow/airflow

ARG AIRFLOW_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS="all"
ARG PYTHON_DEPS=""
ARG APT_DEPS="libsasl2-dev"

RUN set -x \
    && export SLUGIFY_USES_TEXT_UNIDECODE=yes \
    && apt update \
    && if [ -n "${APT_DEPS}" ]; then apt install -y $APT_DEPS; fi \
    && if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi \
    && cd /opt/airflow \
    && pip install -e .[$AIRFLOW_DEPS] \
    && apt-get autoremove -yqq --purge \
    && apt-get clean

RUN mkdir -p $AIRFLOW_HOME
COPY scripts/docker-entrypoint.sh /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["--help"]
