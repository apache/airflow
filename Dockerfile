FROM python:3.6
RUN mkdir /opt/airflow
ADD setup.* /opt/airflow/
ADD airflow /opt/airflow/airflow
RUN cd /opt/airflow
ENV SLUGIFY_USES_TEXT_UNIDECODE=yes
RUN apt update && apt install libsasl2-dev
RUN cd /opt/airflow && pip install -e .[all]
RUN useradd -ms /bin/bash airflow

USER airflow
WORKDIR /home/airflow
ENV AIRFLOW_HOME=/home/airflow/airflow
RUN mkdir $AIRFLOW_HOME
ADD scripts/docker-entrypoint.sh /docker-entrypoint.sh
CMD ["/docker-entrypoint.sh"]
