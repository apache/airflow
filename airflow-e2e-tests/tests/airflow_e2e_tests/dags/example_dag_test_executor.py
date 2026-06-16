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

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="example_dag_test_executor",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["e2e", "dag_test"],
) as dag:
    task_start = EmptyOperator(task_id="task_start")
    task_end = EmptyOperator(task_id="task_end")

    task_start >> task_end

if __name__ == "__main__":
    print("Running local dag.test() with executor=True configuration...")
    dag.test(use_executor=True)
