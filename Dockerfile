FROM python:3.6-jessie

ENV AIRFLOW_COMPONENTS "async,crypto,devel,gcp_api,kubernetes,ldap,password,postgres"
ADD . airflow

RUN SLUGIFY_USES_TEXT_UNIDECODE=yes \
        pip3 install --no-cache-dir -U airflow/.[${AIRFLOW_COMPONENTS}]


