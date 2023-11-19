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

import os
from datetime import datetime
from unittest.mock import patch

import pytest
from openlineage.client.facet import SourceCodeJobFacet

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.openlineage.extractors.bash import BashExtractor
from airflow.providers.openlineage.utils.utils import is_source_enabled
from tests.test_utils.config import conf_vars

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


@pytest.fixture(autouse=True, scope="function")
def clear_cache():
    is_source_enabled.cache_clear()


def test_extract_operator_bash_command_disables_without_env():
    operator = BashOperator(task_id="taskid", bash_command="exit 0")
    extractor = BashExtractor(operator)
    assert "sourceCode" not in extractor.extract().job_facets


@patch.dict(os.environ, {"OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE": "False"})
def test_extract_operator_bash_command_enables_on_true_env():
    operator = BashOperator(task_id="taskid", bash_command="exit 0")
    extractor = BashExtractor(operator)
    assert extractor.extract().job_facets["sourceCode"] == SourceCodeJobFacet("bash", "exit 0")


@conf_vars({("openlineage", "disable_source_code"): "False"})
def test_extract_operator_bash_command_enables_on_true_conf():
    operator = BashOperator(task_id="taskid", bash_command="exit 0")
    extractor = BashExtractor(operator)
    assert extractor.extract().job_facets["sourceCode"] == SourceCodeJobFacet("bash", "exit 0")


@patch.dict(
    os.environ,
    {k: v for k, v in os.environ.items() if k != "OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE"},
    clear=True,
)
def test_extract_dag_bash_command_disabled_without_env():
    extractor = BashExtractor(bash_task)
    assert "sourceCode" not in extractor.extract().job_facets


@patch.dict(os.environ, {"OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE": "False"})
def test_extract_dag_bash_command_enables_on_true_env():
    extractor = BashExtractor(bash_task)
    print(extractor.extract().job_facets)
    assert extractor.extract().job_facets["sourceCode"] == SourceCodeJobFacet("bash", "ls -halt && exit 0")


@conf_vars({("openlineage", "disable_source_code"): "False"})
def test_extract_dag_bash_command_enables_on_true_conf():
    extractor = BashExtractor(bash_task)
    assert extractor.extract().job_facets["sourceCode"] == SourceCodeJobFacet("bash", "ls -halt && exit 0")


@patch.dict(os.environ, {"OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE": "True"})
def test_extract_dag_bash_command_env_disables_on_true():
    extractor = BashExtractor(bash_task)
    assert "sourceCode" not in extractor.extract().job_facets


@conf_vars({("openlineage", "disable_source_code"): "true"})
def test_extract_dag_bash_command_conf_disables_on_true():
    extractor = BashExtractor(bash_task)
    assert "sourceCode" not in extractor.extract().job_facets


@patch.dict(os.environ, {"OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE": "asdftgeragdsfgawef"})
def test_extract_dag_bash_command_env_does_not_disable_on_random_string():
    extractor = BashExtractor(bash_task)
    assert extractor.extract().job_facets["sourceCode"] == SourceCodeJobFacet("bash", "ls -halt && exit 0")


@conf_vars({("openlineage", "disable_source_code"): "asdftgeragdsfgawef"})
def test_extract_dag_bash_command_conf_does_not_disable_on_random_string():
    extractor = BashExtractor(bash_task)
    assert extractor.extract().job_facets["sourceCode"] == SourceCodeJobFacet("bash", "ls -halt && exit 0")
