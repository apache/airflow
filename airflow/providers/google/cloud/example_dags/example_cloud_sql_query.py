#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Example Airflow DAG that performs query in a Cloud SQL instance.

This DAG relies on the following OS environment variables

* GCP_PROJECT_ID - Google Cloud Platform project for the Cloud SQL instance
* GCP_REGION - Google Cloud region where the database is created
*
* GCSQL_POSTGRES_INSTANCE_NAME - Name of the postgres Cloud SQL instance
* GCSQL_POSTGRES_USER - Name of the postgres database user
* GCSQL_POSTGRES_PASSWORD - Password of the postgres database user
* GCSQL_POSTGRES_PUBLIC_IP - Public IP of the Postgres database
* GCSQL_POSTGRES_PUBLIC_PORT - Port of the postgres database
*
* GCSQL_MYSQL_INSTANCE_NAME - Name of the postgres Cloud SQL instance
* GCSQL_MYSQL_USER - Name of the mysql database user
* GCSQL_MYSQL_PASSWORD - Password of the mysql database user
* GCSQL_MYSQL_PUBLIC_IP - Public IP of the mysql database
* GCSQL_MYSQL_PUBLIC_PORT - Port of the mysql database
"""
import os
import subprocess
from os.path import expanduser
from urllib.parse import quote_plus

from airflow import models
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLExecuteQueryOperator
from airflow.utils.dates import days_ago

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCP_REGION = os.environ.get('GCP_REGION', 'europe-west-1b')

GCSQL_POSTGRES_INSTANCE_NAME_QUERY = os.environ.get('GCSQL_POSTGRES_INSTANCE_NAME_QUERY', 'testpostgres')
GCSQL_POSTGRES_DATABASE_NAME = os.environ.get('GCSQL_POSTGRES_DATABASE_NAME', 'postgresdb')
GCSQL_POSTGRES_USER = os.environ.get('GCSQL_POSTGRES_USER', 'postgres_user')
GCSQL_POSTGRES_PASSWORD = os.environ.get('GCSQL_POSTGRES_PASSWORD', 'password')
GCSQL_POSTGRES_PUBLIC_IP = os.environ.get('GCSQL_POSTGRES_PUBLIC_IP', '0.0.0.0')
GCSQL_POSTGRES_PUBLIC_PORT = os.environ.get('GCSQL_POSTGRES_PUBLIC_PORT', 5432)
GCSQL_POSTGRES_CLIENT_CERT_FILE = os.environ.get(
    'GCSQL_POSTGRES_CLIENT_CERT_FILE', ".key/postgres-client-cert.pem"
)
GCSQL_POSTGRES_CLIENT_KEY_FILE = os.environ.get(
    'GCSQL_POSTGRES_CLIENT_KEY_FILE', ".key/postgres-client-key.pem"
)
GCSQL_POSTGRES_SERVER_CA_FILE = os.environ.get('GCSQL_POSTGRES_SERVER_CA_FILE', ".key/postgres-server-ca.pem")

GCSQL_MYSQL_INSTANCE_NAME_QUERY = os.environ.get('GCSQL_MYSQL_INSTANCE_NAME_QUERY', 'testmysql')
GCSQL_MYSQL_DATABASE_NAME = os.environ.get('GCSQL_MYSQL_DATABASE_NAME', 'mysqldb')
GCSQL_MYSQL_USER = os.environ.get('GCSQL_MYSQL_USER', 'mysql_user')
GCSQL_MYSQL_PASSWORD = os.environ.get('GCSQL_MYSQL_PASSWORD', 'password')
GCSQL_MYSQL_PUBLIC_IP = os.environ.get('GCSQL_MYSQL_PUBLIC_IP', '0.0.0.0')
GCSQL_MYSQL_PUBLIC_PORT = os.environ.get('GCSQL_MYSQL_PUBLIC_PORT', 3306)
GCSQL_MYSQL_CLIENT_CERT_FILE = os.environ.get('GCSQL_MYSQL_CLIENT_CERT_FILE', ".key/mysql-client-cert.pem")
GCSQL_MYSQL_CLIENT_KEY_FILE = os.environ.get('GCSQL_MYSQL_CLIENT_KEY_FILE', ".key/mysql-client-key.pem")
GCSQL_MYSQL_SERVER_CA_FILE = os.environ.get('GCSQL_MYSQL_SERVER_CA_FILE', ".key/mysql-server-ca.pem")

SQL = [
    'CREATE TABLE IF NOT EXISTS TABLE_TEST (I INTEGER)',
    'CREATE TABLE IF NOT EXISTS TABLE_TEST (I INTEGER)',  # shows warnings logged
    'INSERT INTO TABLE_TEST VALUES (0)',
    'CREATE TABLE IF NOT EXISTS TABLE_TEST2 (I INTEGER)',
    'DROP TABLE TABLE_TEST',
    'DROP TABLE TABLE_TEST2',
]


# [START howto_operator_cloudsql_query_connections]

HOME_DIR = expanduser("~")


def get_absolute_path(path):
    """
    Returns absolute path.
    """
    if path.startswith("/"):
        return path
    else:
        return os.path.join(HOME_DIR, path)


postgres_kwargs = dict(
    user=quote_plus(GCSQL_POSTGRES_USER),
    password=quote_plus(GCSQL_POSTGRES_PASSWORD),
    public_port=GCSQL_POSTGRES_PUBLIC_PORT,
    public_ip=quote_plus(GCSQL_POSTGRES_PUBLIC_IP),
    project_id=quote_plus(GCP_PROJECT_ID),
    location=quote_plus(GCP_REGION),
    instance=quote_plus(GCSQL_POSTGRES_INSTANCE_NAME_QUERY),
    database=quote_plus(GCSQL_POSTGRES_DATABASE_NAME),
    client_cert_file=quote_plus(get_absolute_path(GCSQL_POSTGRES_CLIENT_CERT_FILE)),
    client_key_file=quote_plus(get_absolute_path(GCSQL_POSTGRES_CLIENT_KEY_FILE)),
    server_ca_file=quote_plus(get_absolute_path(GCSQL_POSTGRES_SERVER_CA_FILE)),
)

# The connections below are created using one of the standard approaches - via environment
# variables named AIRFLOW_CONN_* . The connections can also be created in the database
# of AIRFLOW (using command line or UI).

# Postgres: connect via proxy over TCP
os.environ['AIRFLOW_CONN_PROXY_POSTGRES_TCP'] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=postgres&"
    "project_id={project_id}&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=True&"
    "sql_proxy_use_tcp=True".format(**postgres_kwargs)
)

# Postgres: connect via proxy over UNIX socket (specific proxy version)
os.environ['AIRFLOW_CONN_PROXY_POSTGRES_SOCKET'] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=postgres&"
    "project_id={project_id}&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=True&"
    "sql_proxy_version=v1.13&"
    "sql_proxy_use_tcp=False".format(**postgres_kwargs)
)

# Postgres: connect directly via TCP (non-SSL)
os.environ['AIRFLOW_CONN_PUBLIC_POSTGRES_TCP'] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=postgres&"
    "project_id={project_id}&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=False&"
    "use_ssl=False".format(**postgres_kwargs)
)

# Postgres: connect directly via TCP (SSL)
os.environ['AIRFLOW_CONN_PUBLIC_POSTGRES_TCP_SSL'] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=postgres&"
    "project_id={project_id}&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=False&"
    "use_ssl=True&"
    "sslcert={client_cert_file}&"
    "sslkey={client_key_file}&"
    "sslrootcert={server_ca_file}".format(**postgres_kwargs)
)

mysql_kwargs = dict(
    user=quote_plus(GCSQL_MYSQL_USER),
    password=quote_plus(GCSQL_MYSQL_PASSWORD),
    public_port=GCSQL_MYSQL_PUBLIC_PORT,
    public_ip=quote_plus(GCSQL_MYSQL_PUBLIC_IP),
    project_id=quote_plus(GCP_PROJECT_ID),
    location=quote_plus(GCP_REGION),
    instance=quote_plus(GCSQL_MYSQL_INSTANCE_NAME_QUERY),
    database=quote_plus(GCSQL_MYSQL_DATABASE_NAME),
    client_cert_file=quote_plus(get_absolute_path(GCSQL_MYSQL_CLIENT_CERT_FILE)),
    client_key_file=quote_plus(get_absolute_path(GCSQL_MYSQL_CLIENT_KEY_FILE)),
    server_ca_file=quote_plus(get_absolute_path(GCSQL_MYSQL_SERVER_CA_FILE)),
)

# MySQL: connect via proxy over TCP (specific proxy version)
os.environ['AIRFLOW_CONN_PROXY_MYSQL_TCP'] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=mysql&"
    "project_id={project_id}&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=True&"
    "sql_proxy_version=v1.13&"
    "sql_proxy_use_tcp=True".format(**mysql_kwargs)
)

# MySQL: connect via proxy over UNIX socket using pre-downloaded Cloud Sql Proxy binary
try:
    sql_proxy_binary_path = subprocess.check_output(['which', 'cloud_sql_proxy']).decode('utf-8').rstrip()
except subprocess.CalledProcessError:
    sql_proxy_binary_path = "/tmp/anyhow_download_cloud_sql_proxy"

os.environ['AIRFLOW_CONN_PROXY_MYSQL_SOCKET'] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=mysql&"
    "project_id={project_id}&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=True&"
    "sql_proxy_binary_path={sql_proxy_binary_path}&"
    "sql_proxy_use_tcp=False".format(sql_proxy_binary_path=quote_plus(sql_proxy_binary_path), **mysql_kwargs)
)

# MySQL: connect directly via TCP (non-SSL)
os.environ['AIRFLOW_CONN_PUBLIC_MYSQL_TCP'] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=mysql&"
    "project_id={project_id}&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=False&"
    "use_ssl=False".format(**mysql_kwargs)
)

# MySQL: connect directly via TCP (SSL) and with fixed Cloud Sql Proxy binary path
os.environ['AIRFLOW_CONN_PUBLIC_MYSQL_TCP_SSL'] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=mysql&"
    "project_id={project_id}&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=False&"
    "use_ssl=True&"
    "sslcert={client_cert_file}&"
    "sslkey={client_key_file}&"
    "sslrootcert={server_ca_file}".format(**mysql_kwargs)
)

# Special case: MySQL: connect directly via TCP (SSL) and with fixed Cloud Sql
# Proxy binary path AND with missing project_id

os.environ['AIRFLOW_CONN_PUBLIC_MYSQL_TCP_SSL_NO_PROJECT_ID'] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=mysql&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=False&"
    "use_ssl=True&"
    "sslcert={client_cert_file}&"
    "sslkey={client_key_file}&"
    "sslrootcert={server_ca_file}".format(**mysql_kwargs)
)


# [END howto_operator_cloudsql_query_connections]

# [START howto_operator_cloudsql_query_operators]

connection_names = [
    "proxy_postgres_tcp",
    "proxy_postgres_socket",
    "public_postgres_tcp",
    "public_postgres_tcp_ssl",
    "proxy_mysql_tcp",
    "proxy_mysql_socket",
    "public_mysql_tcp",
    "public_mysql_tcp_ssl",
    "public_mysql_tcp_ssl_no_project_id",
]

tasks = []


with models.DAG(
    dag_id='example_gcp_sql_query', schedule_interval=None, start_date=days_ago(1), tags=['example'],
) as dag:
    prev_task = None

    for connection_name in connection_names:
        task = CloudSQLExecuteQueryOperator(
            gcp_cloudsql_conn_id=connection_name, task_id="example_gcp_sql_task_" + connection_name, sql=SQL
        )
        tasks.append(task)
        if prev_task:
            prev_task >> task
        prev_task = task

# [END howto_operator_cloudsql_query_operators]
