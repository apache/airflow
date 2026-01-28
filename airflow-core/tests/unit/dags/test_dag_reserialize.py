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
from time import sleep

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def sleep_task():
    sleep(1)


with DAG(
    "test_dag_reserialize",
    start_date=datetime(2026, 1, 20),
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    task_b = PythonOperator(task_id="bear", python_callable=sleep_task)
