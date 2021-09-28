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

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook


@task(task_id='es_print_tables')
def show_tables():
    """
    show_tables queries elasticsearch to list available tables
    """
    es = ElasticsearchHook(elasticsearch_conn_id='production-es')

    # Handle ES conn with context manager
    with es.get_conn() as es_conn:
        tables = es_conn.execute('SHOW TABLES')
        for table, *_ in tables:
            print(f"table: {table}")
    return True


# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG(
    'elasticsearch_dag',
    start_date=datetime(2021, 8, 30),
    max_active_runs=1,
    schedule_interval=timedelta(days=1),
    default_args={'retries': 1},  # Default setting applied to all tasks
    catchup=False,
) as dag:

    show_tables()
