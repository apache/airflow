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
"""Test DAG with XCom passing for testing dag.test() method."""

from __future__ import annotations

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

EXPECTED_VALUE = {"key": "value", "number": 42, "list": [1, 2, 3]}


@task
def push_xcom():
    """Push XCom value."""
    return EXPECTED_VALUE


@task
def pull_and_validate_xcom(value):
    """Pull and validate XCom value."""
    assert value == EXPECTED_VALUE, f"Expected {EXPECTED_VALUE}, got {value}"
    return "xcom_validated"


@task
def push_string_xcom():
    """Push string XCom."""
    return "test_string_value"


@task
def pull_string_xcom(value):
    """Pull string XCom."""
    assert value == "test_string_value", f"Expected 'test_string_value', got {value}"
    return "string_xcom_validated"


with DAG(
    "test_dag_test_xcom",
    schedule=None,
    catchup=False,
    tags=["test", "dag_test", "xcom"],
) as dag:
    bash_push = BashOperator(
        task_id="bash_push",
        bash_command='echo "bash_xcom_value"',
    )

    bash_pull = BashOperator(
        task_id="bash_pull",
        bash_command='echo "Pulling bash XCom: {{ ti.xcom_pull(task_ids="bash_push") }}"',
    )

    python_push = push_xcom()
    python_validate = pull_and_validate_xcom(python_push)

    string_push = push_string_xcom()
    string_validate = pull_string_xcom(string_push)

    bash_push >> bash_pull
    python_push >> python_validate
    string_push >> string_validate

