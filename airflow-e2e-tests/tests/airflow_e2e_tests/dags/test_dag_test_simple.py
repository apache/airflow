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
"""Simple test DAG for testing dag.test() method."""

from __future__ import annotations

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task


@task
def task_1():
    """First task in the pipeline."""
    return "task_1_completed"


@task
def task_2(value):
    """Second task that receives value from task_1."""
    print(f"Received value: {value}")
    return "task_2_completed"


@task
def task_3(value):
    """Third task that receives value from task_2."""
    print(f"Received value: {value}")
    return "task_3_completed"


with DAG(
    "test_dag_test_simple",
    schedule=None,
    catchup=False,
    tags=["test", "dag_test"],
) as dag:
    bash_task = BashOperator(
        task_id="bash_task",
        bash_command='echo "Bash task executed successfully"',
    )

    python_task_1 = task_1()
    python_task_2 = task_2(python_task_1)
    python_task_3 = task_3(python_task_2)

    bash_task >> python_task_1 >> python_task_2 >> python_task_3

