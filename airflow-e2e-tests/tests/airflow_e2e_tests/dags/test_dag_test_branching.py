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
"""Test DAG with branching logic for testing dag.test() method."""

from __future__ import annotations

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.sdk import DAG, task


@task
def branch_task():
    """Task that determines the branch."""
    return True


@task
def true_branch():
    """Task executed when condition is True."""
    return "true_branch_executed"


@task
def false_branch():
    """Task executed when condition is False."""
    return "false_branch_executed"


@task
def join_task():
    """Task that joins after branching."""
    return "join_task_executed"


def _branch_func(value):
    """Branch function that returns task_id based on value."""
    if value:
        return "true_branch"
    return "false_branch"


with DAG(
    "test_dag_test_branching",
    schedule=None,
    catchup=False,
    tags=["test", "dag_test", "branching"],
) as dag:
    start_task = BashOperator(
        task_id="start_task",
        bash_command='echo "Starting branching test"',
    )

    branch_condition = branch_task()
    branch_op = BranchPythonOperator(
        task_id="branch",
        python_callable=_branch_func,
        op_args=[branch_condition],
    )

    true_task = true_branch()
    false_task = false_branch()
    join = join_task()

    start_task >> branch_condition >> branch_op
    branch_op >> [true_task, false_task] >> join

