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

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

# 測試 DAG 1: 基本 DAG
with DAG(
    "test_dag_simple",
    default_args=default_args,
    schedule="0 0 * * *",
    tags=["test", "example"],
    doc_md="### 簡單測試 DAG",
) as dag1:
    task1 = EmptyOperator(task_id="task_1")
    task2 = BashOperator(task_id="task_2", bash_command='echo "Running task 2"')
    task1 >> task2

# 測試 DAG 2: 帶參數的 DAG
with DAG(
    "test_dag_parametrized",
    default_args=default_args,
    schedule="0 6 * * *",
    tags=["test", "parametrized"],
    params={
        "partner_id": "default_partner",
        "data_date": datetime.now().strftime("%Y-%m-%d"),
    },
) as dag2:
    task_a = EmptyOperator(task_id="task_a")
    task_b = EmptyOperator(task_id="task_b")
    task_a >> task_b

# 測試 DAG 3: 長時間運行的 DAG
with DAG(
    "test_dag_long_running",
    default_args=default_args,
    schedule="0 12 * * *",
    tags=["test", "long-running"],
) as dag3:
    long_task = BashOperator(task_id="long_running_task", bash_command='sleep 3600 && echo "Done"')
