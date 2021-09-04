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
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.utils.dates import days_ago

dag = DAG(
    dag_id='example_sqlite',
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


def insert_sqlite_hook():
    sqlite_hook = SqliteHook("sqlite_default")
    sqlite_hook.get_conn()

    rows = [('James', '11'), ('James', '22'), ('James', '33')]
    target_fields = ['first_name', 'last_name']
    sqlite_hook.insert_rows(table='Customer', rows=rows, target_fields=target_fields)


def replace_sqlite_hook():
    sqlite_hook = SqliteHook("sqlite_default")
    sqlite_hook.get_conn()

    rows = [('James', '11'), ('James', '22'), ('James', '33')]
    target_fields = ['first_name', 'last_name']
    sqlite_hook.insert_rows(table='Customer', rows=rows, target_fields=target_fields, replace=True)


insert_sqlite_task = PythonOperator(task_id="insert_sqlite_task", python_callable=insert_sqlite_hook)
replace_sqlite_task = PythonOperator(task_id="replace_sqlite_task", python_callable=replace_sqlite_hook)

# [START howto_operator_sqlite_external_file]

# Example of creating a task that calls an sql command from an external file.
external_create_table_sqlite_task = SqliteOperator(
    task_id='create_table_sqlite_external_file',
    sqlite_conn_id='sqlite_conn_id',
    sql='/scripts/create_table.sql',
    dag=dag,
)

# [END howto_operator_sqlite_external_file]

create_table_sqlite_task >> external_create_table_sqlite_task >> insert_sqlite_task >> replace_sqlite_task
