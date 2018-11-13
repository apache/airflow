# -*- coding: utf-8 -*-
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

* PROJECT_ID - Google Cloud Platform project for the Cloud SQL instance
* LOCATION - Google Cloud location where the database is created
*
* POSTGRES_INSTANCE_NAME - Name of the postgres Cloud SQL instance
* POSTGRES_USER - Name of the postgres database user
* POSTGRES_PASSWORD - Password of the postgres database user
* POSTGRES_PROXY_PORT - Local port number for proxy connections for postgres
* POSTGRES_PUBLIC_IP - Public IP of the Postgres database
* POSTGRES_PUBLIC_PORT - Port of the postgres database
*
* MYSQL_INSTANCE_NAME - Name of the postgres Cloud SQL instance
* MYSQL_USER - Name of the mysql database user
* MYSQL_PASSWORD - Password of the mysql database user
* MYSQL_PROXY_PORT - Local port number for proxy connections for mysql
* MYSQL_PUBLIC_IP - Public IP of the mysql database
* MYSQL_PUBLIC_PORT - Port of the mysql database
"""

import os
import subprocess

from six.moves.urllib.parse import quote_plus

import airflow
from airflow import models
from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator

# [START howto_operator_cloudsql_query_arguments]

PROJECT_ID = os.environ.get('PROJECT_ID', 'example-project')
LOCATION = os.environ.get('REGION', 'europe-west-1')

POSTGRES_INSTANCE_NAME = os.environ.get('POSTGRES_INSTANCE_NAME', 'testpostgres')
POSTGRES_DATABASE_NAME = os.environ.get('POSTGRES_DATABASE_NAME', 'postgresdb')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'postgres_user')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'password')
POSTGRES_PUBLIC_IP = os.environ.get('POSTGRES_PUBLIC_IP', '0.0.0.0')
POSTGRES_PUBLIC_PORT = os.environ.get('POSTGRES_PUBLIC_PORT', 5432)
POSTGRES_CLIENT_CERT_FILE = os.environ.get('POSTGRES_CLIENT_CERT_FILE',
                                           "/tmp/client-cert.pem")
POSTGRES_CLIENT_KEY_FILE = os.environ.get('POSTGRES_CLIENT_KEY_FILE',
                                          "/tmp/client-key.pem")
POSTGRES_SERVER_CA_FILE = os.environ.get('POSTGRES_SERVER_CA_FILE',
                                         "/tmp/server-ca.pem")

MYSQL_INSTANCE_NAME = os.environ.get('MYSQL_INSTANCE_NAME', 'testmysql')
MYSQL_DATABASE_NAME = os.environ.get('MYSQL_DATABASE_NAME', 'mysqldb')
MYSQL_USER = os.environ.get('MYSQL_USER', 'mysql_user')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'password')
MYSQL_PUBLIC_IP = os.environ.get('MYSQL_PUBLIC_IP', '0.0.0.0')
MYSQL_PUBLIC_PORT = os.environ.get('MYSQL_PUBLIC_PORT', 3306)
MYSQL_CLIENT_CERT_FILE = os.environ.get('MYSQL_CLIENT_CERT_FILE',
                                        "/tmp/client-cert.pem")
MYSQL_CLIENT_KEY_FILE = os.environ.get('MYSQL_CLIENT_KEY_FILE',
                                       "/tmp/client-key.pem")
MYSQL_SERVER_CA_FILE = os.environ.get('MYSQL_SERVER_CA_FILE',
                                      "/tmp/server-ca.pem")

SQL = [
    'CREATE TABLE IF NOT EXISTS TABLE_TEST (I INTEGER)',
    'CREATE TABLE IF NOT EXISTS TABLE_TEST (I INTEGER)',  # shows warnings logged
    'INSERT INTO TABLE_TEST VALUES (0)',
    'CREATE TABLE IF NOT EXISTS TABLE_TEST2 (I INTEGER)',
    'DROP TABLE TABLE_TEST',
    'DROP TABLE TABLE_TEST2',
]

# [END howto_operator_cloudsql_query_arguments]
default_args = {
    'start_date': airflow.utils.dates.days_ago(1)
}


# [START howto_operator_cloudsql_query_connections]

postgres_kwargs = dict(
    user=quote_plus(POSTGRES_USER),
    password=quote_plus(POSTGRES_PASSWORD),
    public_port=POSTGRES_PUBLIC_PORT,
    public_ip=quote_plus(POSTGRES_PUBLIC_IP),
    project_id=quote_plus(PROJECT_ID),
    location=quote_plus(LOCATION),
    instance=quote_plus(POSTGRES_INSTANCE_NAME),
    database=quote_plus(POSTGRES_DATABASE_NAME),
    client_cert_file=quote_plus(POSTGRES_CLIENT_CERT_FILE),
    client_key_file=quote_plus(POSTGRES_CLIENT_KEY_FILE),
    server_ca_file=quote_plus(POSTGRES_SERVER_CA_FILE)
)

# The connections below are created using one of the standard approaches - via environment
# variables named AIRFLOW_CONN_* . The connections can also be created in the database
# of AIRFLOW (using command line or UI).

# Postgres: connect via proxy over TCP
os.environ['AIRFLOW_CONN_PROXY_POSTGRES_TCP'] = \
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?" \
    "database_type=postgres&" \
    "project_id={project_id}&" \
    "location={location}&" \
    "instance={instance}&" \
    "use_proxy=True&" \
    "sql_proxy_use_tcp=True".format(**postgres_kwargs)

# Postgres: connect via proxy over UNIX socket (specific proxy version)
os.environ['AIRFLOW_CONN_PROXY_POSTGRES_SOCKET'] = \
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?" \
    "database_type=postgres&" \
    "project_id={project_id}&" \
    "location={location}&" \
    "instance={instance}&" \
    "use_proxy=True&" \
    "sql_proxy_version=v1.13&" \
    "sql_proxy_use_tcp=False".format(**postgres_kwargs)

# Postgres: connect directly via TCP (non-SSL)
os.environ['AIRFLOW_CONN_PUBLIC_POSTGRES_TCP'] = \
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?" \
    "database_type=postgres&" \
    "project_id={project_id}&" \
    "location={location}&" \
    "instance={instance}&" \
    "use_proxy=False&" \
    "use_ssl=False".format(**postgres_kwargs)

# Postgres: connect directly via TCP (SSL)
os.environ['AIRFLOW_CONN_PUBLIC_POSTGRES_TCP_SSL'] = \
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?" \
    "database_type=postgres&" \
    "project_id={project_id}&" \
    "location={location}&" \
    "instance={instance}&" \
    "use_proxy=False&" \
    "use_ssl=True&" \
    "sslcert={client_cert_file}&" \
    "sslkey={client_key_file}&" \
    "sslrootcert={server_ca_file}"\
    .format(**postgres_kwargs)

mysql_kwargs = dict(
    user=quote_plus(MYSQL_USER),
    password=quote_plus(MYSQL_PASSWORD),
    public_port=MYSQL_PUBLIC_PORT,
    public_ip=quote_plus(MYSQL_PUBLIC_IP),
    project_id=quote_plus(PROJECT_ID),
    location=quote_plus(LOCATION),
    instance=quote_plus(MYSQL_INSTANCE_NAME),
    database=quote_plus(MYSQL_DATABASE_NAME),
    client_cert_file=quote_plus(MYSQL_CLIENT_CERT_FILE),
    client_key_file=quote_plus(MYSQL_CLIENT_KEY_FILE),
    server_ca_file=quote_plus(MYSQL_SERVER_CA_FILE)
)

# MySQL: connect via proxy over TCP (specific proxy version)
os.environ['AIRFLOW_CONN_PROXY_MYSQL_TCP'] = \
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?" \
    "database_type=mysql&" \
    "project_id={project_id}&" \
    "location={location}&" \
    "instance={instance}&" \
    "use_proxy=True&" \
    "sql_proxy_version=v1.13&" \
    "sql_proxy_use_tcp=True".format(**mysql_kwargs)

# MySQL: connect via proxy over UNIX socket using pre-downloaded Cloud Sql Proxy binary
try:
    sql_proxy_binary_path = subprocess.check_output(
        ['which', 'cloud_sql_proxy']).rstrip()
except subprocess.CalledProcessError:
    sql_proxy_binary_path = "/tmp/anyhow_download_cloud_sql_proxy"

os.environ['AIRFLOW_CONN_PROXY_MYSQL_SOCKET'] = \
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?" \
    "database_type=mysql&" \
    "project_id={project_id}&" \
    "location={location}&" \
    "instance={instance}&" \
    "use_proxy=True&" \
    "sql_proxy_binary_path={sql_proxy_binary_path}&" \
    "sql_proxy_use_tcp=False".format(
        sql_proxy_binary_path=quote_plus(sql_proxy_binary_path), **mysql_kwargs)

# MySQL: connect directly via TCP (non-SSL)
os.environ['AIRFLOW_CONN_PUBLIC_MYSQL_TCP'] = \
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?" \
    "database_type=mysql&" \
    "project_id={project_id}&" \
    "location={location}&" \
    "instance={instance}&" \
    "use_proxy=False&" \
    "use_ssl=False".format(**mysql_kwargs)

# MySQL: connect directly via TCP (SSL) and with fixed Cloud Sql Proxy binary path
os.environ['AIRFLOW_CONN_PUBLIC_MYSQL_TCP_SSL'] = \
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?" \
    "database_type=mysql&" \
    "project_id={project_id}&" \
    "location={location}&" \
    "instance={instance}&" \
    "use_proxy=False&" \
    "use_ssl=True&" \
    "sslcert={client_cert_file}&" \
    "sslkey={client_key_file}&" \
    "sslrootcert={server_ca_file}".format(**mysql_kwargs)

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
    "public_mysql_tcp_ssl"
]

tasks = []

with models.DAG(
    dag_id='example_gcp_sql_query',
    default_args=default_args,
    schedule_interval=None
) as dag:
    for connection_name in connection_names:
        tasks.append(
            CloudSqlQueryOperator(
                gcp_cloudsql_conn_id=connection_name,
                task_id="example_gcp_sql_task_" + connection_name,
                sql=SQL
            )
        )
# [END howto_operator_cloudsql_query_operators]
