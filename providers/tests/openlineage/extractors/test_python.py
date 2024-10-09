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

import inspect
import os
import warnings
from datetime import datetime
from unittest.mock import patch

import pytest
from openlineage.client.facet_v2 import source_code_job

from airflow import DAG
from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.operators.python import PythonOperator
from airflow.providers.openlineage.extractors.python import PythonExtractor

from dev.tests_common.test_utils.compat import BashOperator

pytestmark = pytest.mark.db_test

dag = DAG(
    dag_id="test_dummy_dag",
    description="Test dummy DAG",
    schedule="*/2 * * * *",
    start_date=datetime(2020, 1, 8),
    catchup=False,
    max_active_runs=1,
)


python_task_getcwd = PythonOperator(task_id="python-task", python_callable=os.getcwd, dag=dag)

bash_task = BashOperator(task_id="bash-task", bash_command="ls -halt && exit 0", dag=dag)

python_task_getcwd >> bash_task


def callable():
    print(10)


CODE = "def callable():\n    print(10)\n"


def test_extract_source_code():
    code = inspect.getsource(callable)
    assert code == CODE


@patch("airflow.providers.openlineage.conf.is_source_enabled")
def test_extract_operator_code_disabled(mocked_source_enabled):
    mocked_source_enabled.return_value = False
    operator = PythonOperator(task_id="taskid", python_callable=callable, op_args=(1, 2), op_kwargs={"a": 1})
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", AirflowProviderDeprecationWarning)
        result = PythonExtractor(operator).extract()
    assert "sourceCode" not in result.job_facets
    assert "unknownSourceAttribute" in result.run_facets
    unknown_items = result.run_facets["unknownSourceAttribute"]["unknownItems"]
    assert len(unknown_items) == 1
    assert unknown_items[0]["name"] == "PythonOperator"
    assert "python_callable" not in unknown_items[0]["properties"]
    assert "op_args" not in unknown_items[0]["properties"]
    assert "op_kwargs" not in unknown_items[0]["properties"]
    assert "task_id" in unknown_items[0]["properties"]


@patch("airflow.providers.openlineage.conf.is_source_enabled")
def test_extract_operator_code_enabled(mocked_source_enabled):
    mocked_source_enabled.return_value = True
    operator = PythonOperator(task_id="taskid", python_callable=callable, op_args=(1, 2), op_kwargs={"a": 1})
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", AirflowProviderDeprecationWarning)
        result = PythonExtractor(operator).extract()
    assert result.job_facets["sourceCode"] == source_code_job.SourceCodeJobFacet("python", CODE)
    assert "unknownSourceAttribute" in result.run_facets
    unknown_items = result.run_facets["unknownSourceAttribute"]["unknownItems"]
    assert len(unknown_items) == 1
    assert unknown_items[0]["name"] == "PythonOperator"
    assert "python_callable" not in unknown_items[0]["properties"]
    assert "op_args" not in unknown_items[0]["properties"]
    assert "op_kwargs" not in unknown_items[0]["properties"]
    assert "task_id" in unknown_items[0]["properties"]
