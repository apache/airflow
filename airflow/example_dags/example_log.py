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
    "example_log",
    default_args={
        "owner": "airflow",
        "start_date": airflow.utils.dates.days_ago(1),
    },
    schedule_interval='*/1 * * * *',
    dagrun_timeout=timedelta(minutes=4),
    catchup=False
)


def my_py_command(ds, **kwargs):
    # Print out the "foo" param passed in via
    # `airflow test example_passing_params_via_test_command run_this <date>
    # -tp '{"foo":"bar"}'`
    if kwargs["test_mode"]:
        print(" 'foo' was passed in via test={} command : kwargs[params][foo] \
               = {}".format(kwargs["test_mode"], kwargs["params"]["foo"]))
    # Print out the value of "miff", passed in below via the Python Operator
    print(" 'miff' was passed in via task params = {}".format(kwargs["params"]["miff"]))
    kwargs['ti'].xcom_push(key='log_urls', value=[('Log A', 'yahoo.com'), ('Log B', 'google.com')])

def py_command_2(ds, **kwargs):
    kwargs['ti'].xcom_push(key='log_urls', value=[('Log 1', 'zoop.broop')])

my_templated_command = """
    echo " 'foo was passed in via Airflow CLI Test command with value {{ params.foo }} "
    echo " 'miff was passed in via BashOperator with value {{ params.miff }} "
"""

run_this = PythonOperator(
    task_id='log_first',
    provide_context=True,
    python_callable=my_py_command,
    params={"miff": "agg"},
    dag=dag,
)

also_run_this = PythonOperator(
    task_id='log_second',
    provide_context=True,
    python_callable=py_command_2,
    params={"miff": "agg"},
    dag=dag,
)

run_this >> also_run_this
