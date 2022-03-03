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
from airflow.providers.apache.zeppelin.operators.zeppelin_operator import ZeppelinOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2001, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'zeppelin_example_dag', max_active_runs=1, schedule_interval='0 0 * * *', default_args=default_args
) as dag:

    python_task = ZeppelinOperator(task_id='python_note', conn_id='zeppelin_default', note_id='2EYDJKFFY')

    spark_scala_task = ZeppelinOperator(
        task_id='spark_scala_note', conn_id='zeppelin_default', note_id='2A94M5J1Z'
    )

    pyspark_task = ZeppelinOperator(task_id='pyspark_note', conn_id='zeppelin_default', note_id='2EWM84JXA')

    python_task >> spark_scala_task >> pyspark_task
