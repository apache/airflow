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
from airflow.sdk import DAG
from airflow.sdk.definitions.dag import safe_dag

with safe_dag():
    success_dag_1 = DAG("success_dag_1", start_date=datetime(2024, 1, 1))
    task1 = BashOperator(task_id="task1", bash_command="echo hello", dag=success_dag_1)

with safe_dag():
    # Invalid operator parameter
    failing_dag_1 = DAG("failing_dag_1", start_date=datetime(2024, 1, 1))
    task2 = BashOperator(task_id="task2", bash_command=123, dag=failing_dag_1)

with safe_dag():
    success_dag_2 = DAG("success_dag_2", start_date=datetime(2024, 1, 1))
    task3 = BashOperator(task_id="task3", bash_command="echo world", dag=success_dag_2)

with safe_dag():
    # Missing required parameter - bash_command
    failing_dag_2 = DAG("failing_dag_2", start_date=datetime(2024, 1, 1))
    task4 = BashOperator(task_id="task4", dag=failing_dag_2)
