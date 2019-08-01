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

from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    "example_log_onetask",
    default_args={
        "owner": "airflow",
        "start_date": airflow.utils.dates.days_ago(1),
    },
    schedule_interval='*/10 * * * *',
    dagrun_timeout=timedelta(minutes=4),
    catchup=False
)


def my_py_command(ds, **kwargs):
    kwargs['ti'].xcom_push(key='log_urls', value=[('Log Alpha', 'yahoo.com'), ('Log Omega', 'google.com')])


run_this = PythonOperator(
    task_id='log_first',
    provide_context=True,
    python_callable=my_py_command,
    params={"miff": "agg"},
    dag=dag,
)
