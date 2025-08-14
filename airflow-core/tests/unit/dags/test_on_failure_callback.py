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
from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.exceptions import AirflowFailException

DEFAULT_DATE = datetime(2016, 1, 1)

args = {
    "owner": "airflow",
    "start_date": DEFAULT_DATE,
}

dag = DAG(dag_id="test_on_failure_callback", schedule=None, default_args=args)


def write_data_to_callback(context):
    msg = " ".join([str(k) for k in context["ti"].key.primary]) + f" fired callback with pid: {os.getpid()}"
    with open(os.environ.get("AIRFLOW_CALLBACK_FILE"), "a+") as f:
        f.write(msg)


def task_function(ti):
    raise AirflowFailException()


PythonOperator(
    task_id="test_on_failure_callback_task",
    on_failure_callback=write_data_to_callback,
    python_callable=task_function,
    dag=dag,
)

BashOperator(
    task_id="bash_sleep",
    on_failure_callback=write_data_to_callback,
    bash_command="touch $AIRFLOW_CALLBACK_FILE; sleep 10",
    dag=dag,
)
