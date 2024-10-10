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

from datetime import timedelta

import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

exec_date = pendulum.datetime(2021, 1, 1)
fut_start_date = pendulum.datetime(2021, 2, 1)
with DAG(
    dag_id="test_dagrun_states_root_future",
    schedule=timedelta(days=1),
    catchup=True,
    start_date=exec_date,
) as dag:
    EmptyOperator(task_id="current")
    PythonOperator(
        task_id="future",
        python_callable=lambda: print("hello"),
        start_date=fut_start_date,
    )
