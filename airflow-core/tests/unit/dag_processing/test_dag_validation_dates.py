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
"""Test that DAGs with templated start_date/end_date fail validation."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from airflow.dag_processing.dagbag import DagBag


@pytest.mark.db_test
def test_dag_with_templated_dates_in_task_fails():
    """Test that DAGs with templated start_date/end_date in tasks fail validation."""
    test_dag_path = Path(__file__).parent.parent / "dags" / "test_dag_validation_string_dates.py"
    dagbag = DagBag(dag_folder=os.fspath(test_dag_path.parent), include_examples=False)

    assert "string_start_end_dates_dag" not in dagbag.dag_ids

    dag_file = os.fspath(test_dag_path)
    assert dag_file in dagbag.import_errors

    error_msg = dagbag.import_errors[dag_file]
    assert "must be a datetime object" in error_msg


@pytest.mark.db_test
def test_dag_with_templated_dates_in_task_error_message():
    """Test that the error message is clear about templated dates not being allowed."""
    test_dag_path = Path(__file__).parent.parent / "dags" / "test_dag_validation_string_dates.py"
    dagbag = DagBag(dag_folder=os.fspath(test_dag_path.parent), include_examples=False)

    dag_file = os.fspath(test_dag_path)
    assert dag_file in dagbag.import_errors

    error_msg = dagbag.import_errors[dag_file]
    assert "Templated dates" in error_msg or "must be a datetime object" in error_msg
    assert "crash_task" in error_msg or "start_date" in error_msg


@pytest.mark.db_test
def test_dag_with_args_dict_still_loads():
    """Test that DAGs with templated values in args (not task attributes) still load."""
    test_dag_path = Path(__file__).parent.parent / "dags" / "test_dag_validation_args_dict_bad.py"
    dagbag = DagBag(dag_folder=os.fspath(test_dag_path.parent), include_examples=False)

    assert "args_dict_bad" in dagbag.dag_ids
    assert os.fspath(test_dag_path) not in dagbag.import_errors


@pytest.mark.db_test
def test_dag_with_args_equals_still_loads():
    """Test that DAGs with templated values in args list still load."""
    test_dag_path = Path(__file__).parent.parent / "dags" / "test_dag_validation_args_equals_bad.py"
    dagbag = DagBag(dag_folder=os.fspath(test_dag_path.parent), include_examples=False)

    assert "args_equals_bad" in dagbag.dag_ids
    assert os.fspath(test_dag_path) not in dagbag.import_errors


@pytest.mark.db_test
def test_dag_with_valid_dates_still_works():
    """Test that DAGs with proper datetime objects still work."""
    from datetime import datetime

    from airflow import DAG
    from airflow.operators.empty import EmptyOperator

    with DAG(
        dag_id="test_valid_dates",
        start_date=datetime(2025, 1, 1),
        end_date=datetime(2025, 12, 31),
        schedule="@daily",
    ):
        EmptyOperator(task_id="task1")

    assert True


if __name__ == "__main__":
    test_dag_with_templated_dates_in_task_fails()
    test_dag_with_templated_dates_in_task_error_message()
    test_dag_with_args_dict_still_loads()
    test_dag_with_args_equals_still_loads()
    test_dag_with_valid_dates_still_works()
    print("All tests passed!")
