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
from __future__ import annotations

import warnings
from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator

DEFAULT_DATE = datetime(2016, 1, 1)

default_args = {"owner": "airflow", "start_date": DEFAULT_DATE, "run_as_user": "airflow_test_user"}

dag = DAG(dag_id="impersonation_subdag", default_args=default_args)


def print_today():
    print(f"Today is {datetime.utcnow()}")


subdag = DAG("impersonation_subdag.test_subdag_operation", default_args=default_args)


PythonOperator(python_callable=print_today, task_id="exec_python_fn", dag=subdag)


BashOperator(task_id="exec_bash_operator", bash_command='echo "Running within SubDag"', dag=subdag)


with warnings.catch_warnings(record=True):
    subdag_operator = SubDagOperator(
        task_id="test_subdag_operation", subdag=subdag, mode="reschedule", poke_interval=1, dag=dag
    )
