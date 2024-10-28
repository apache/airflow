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

import datetime

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

args = {"owner": "airflow", "retries": 3, "start_date": datetime.datetime(2022, 1, 1)}


def create_dag(suffix):
    dag = DAG(
        dag_id=f"test_for_db_queries_counter__{suffix}",
        default_args=args,
        schedule="0 0 * * *",
        dagrun_timeout=datetime.timedelta(minutes=60),
    )

    with dag:
        EmptyOperator(task_id="test_task")
    return dag


# 26 queries for parsing file with one DAG, 17 queries more for each new dag.
# As a result 94 queries for this file with 5 DAGs.
for i in range(0, 5):
    globals()[f"dag_{i}"] = create_dag(f"dag_{i}")
