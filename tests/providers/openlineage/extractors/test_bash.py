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

import warnings
from datetime import datetime
from unittest.mock import patch

import pytest
from openlineage.client.facet_v2 import source_code_job

from airflow import DAG
from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.openlineage.extractors.bash import BashExtractor
from airflow.providers.standard.core.operators.bash import BashOperator

pytestmark = pytest.mark.db_test

with DAG(
    dag_id="test_dummy_dag",
    description="Test dummy DAG",
    schedule="*/2 * * * *",
    start_date=datetime(2020, 1, 8),
    catchup=False,
    max_active_runs=1,
) as dag:
    bash_task = BashOperator(task_id="bash-task", bash_command="ls -halt && exit 0", dag=dag)


@patch("airflow.providers.openlineage.conf.is_source_enabled")
def test_extract_operator_bash_command_disabled(mocked_source_enabled):
    mocked_source_enabled.return_value = False
    operator = BashOperator(task_id="taskid", bash_command="exit 0;", env={"A": "1"}, append_env=True)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", AirflowProviderDeprecationWarning)
        result = BashExtractor(operator).extract()
    assert "sourceCode" not in result.job_facets
    assert "unknownSourceAttribute" in result.run_facets
    unknown_items = result.run_facets["unknownSourceAttribute"]["unknownItems"]
    assert len(unknown_items) == 1
    assert unknown_items[0]["name"] == "BashOperator"
    assert "bash_command" not in unknown_items[0]["properties"]
    assert "env" not in unknown_items[0]["properties"]
    assert "append_env" not in unknown_items[0]["properties"]
    assert "task_id" in unknown_items[0]["properties"]


@patch("airflow.providers.openlineage.conf.is_source_enabled")
def test_extract_operator_bash_command_enabled(mocked_source_enabled):
    mocked_source_enabled.return_value = True
    operator = BashOperator(task_id="taskid", bash_command="exit 0;", env={"A": "1"}, append_env=True)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", AirflowProviderDeprecationWarning)
        result = BashExtractor(operator).extract()
    assert result.job_facets["sourceCode"] == source_code_job.SourceCodeJobFacet("bash", "exit 0;")
    assert "unknownSourceAttribute" in result.run_facets
    unknown_items = result.run_facets["unknownSourceAttribute"]["unknownItems"]
    assert len(unknown_items) == 1
    assert unknown_items[0]["name"] == "BashOperator"
    assert "bash_command" not in unknown_items[0]["properties"]
    assert "env" not in unknown_items[0]["properties"]
    assert "append_env" not in unknown_items[0]["properties"]
    assert "task_id" in unknown_items[0]["properties"]
