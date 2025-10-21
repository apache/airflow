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

from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.sdk import DAG, dag as dag_decorator, safe_dag  # type: ignore[attr-defined]

with safe_dag():
    success_dag_1 = DAG("success_dag_1", start_date=datetime(2024, 1, 1))
    BashOperator(task_id="task1", bash_command="echo hello", dag=success_dag_1)

with safe_dag():
    # Invalid DAG parameter
    with DAG("failing_dag_1", start_date="invalid_date_format") as dag:  # type: ignore[arg-type]
        BashOperator(task_id="task2", bash_command="echo 'This should not execute'")

with safe_dag():
    success_dag_2 = DAG("success_dag_2", start_date=datetime(2024, 1, 1))
    BashOperator(task_id="task3", bash_command="echo world", dag=success_dag_2)

with safe_dag():
    # Missing required parameter - bash_command
    @dag_decorator("failing_dag_2", start_date=datetime(2024, 1, 1))
    def broken_task():
        BashOperator(task_id="task4")

    broken_task()
