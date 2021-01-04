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
This is an example DAG for the use of the SqliteOperator.
In this example, we create two tasks that execute in sequence.
The first task calls an sql command, defined in the SQLite operator,
which when triggered, is performed on the connected sqlite database.
The second task is similar but instead calls the SQL command from an external file.
"""

from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.utils.dates import days_ago

default_args = {'owner': 'airflow'}

dag = DAG(
    dag_id='example_sqlite',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(2),
    tags=['example'],
)

# [START howto_operator_sqlite]

# Example of creating a task that calls a common CREATE TABLE sql command.
create_table_sqlite_task = SqliteOperator(
    task_id='create_table_sqlite',
    sqlite_conn_id='sqlite_conn_id',
    sql=r"""
    CREATE TABLE table_name (
        column_1 string,
        column_2 string,
        column_3 string
    );
    """,
    dag=dag,
)

# [END howto_operator_sqlite]

# [START howto_operator_sqlite_external_file]

# Example of creating a task that calls an sql command from an external file.
external_create_table_sqlite_task = SqliteOperator(
    task_id='create_table_sqlite_external_file',
    sqlite_conn_id='sqlite_conn_id',
    sql='/scripts/create_table.sql',
    dag=dag,
)

# [END howto_operator_sqlite_external_file]

create_table_sqlite_task >> external_create_table_sqlite_task
