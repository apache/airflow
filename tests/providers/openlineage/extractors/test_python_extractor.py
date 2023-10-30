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
from datetime import datetime
from unittest.mock import patch

import pytest
from openlineage.client.facet import SourceCodeJobFacet

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.openlineage.extractors.python import PythonExtractor
from airflow.providers.openlineage.utils.utils import is_source_enabled
from tests.test_utils.config import conf_vars

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


@pytest.fixture(autouse=True, scope="function")
def clear_cache():
    is_source_enabled.cache_clear()


def test_extract_source_code():
    code = inspect.getsource(callable)
    assert code == CODE


def test_extract_operator_code_disables_on_no_env():
    operator = PythonOperator(task_id="taskid", python_callable=callable)
    extractor = PythonExtractor(operator)
    assert "sourceCode" not in extractor.extract().job_facets


@patch.dict(
    os.environ,
    {"AIRFLOW__OPENLINEAGE__DISABLED_FOR_OPERATORS": "airflow.operators.python.PythonOperator"},
)
def test_python_extraction_disabled_operator():
    operator = PythonOperator(task_id="taskid", python_callable=callable)
    extractor = PythonExtractor(operator)
    metadata = extractor.extract()
    assert metadata is None


@patch.dict(os.environ, {"OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE": "False"})
def test_extract_operator_code_enables_on_false_env():
    operator = PythonOperator(task_id="taskid", python_callable=callable)
    extractor = PythonExtractor(operator)
    assert extractor.extract().job_facets["sourceCode"] == SourceCodeJobFacet("python", CODE)


@conf_vars({("openlineage", "disable_source_code"): "False"})
def test_extract_operator_code_enables_on_false_conf():
    operator = PythonOperator(task_id="taskid", python_callable=callable)
    extractor = PythonExtractor(operator)
    assert extractor.extract().job_facets["sourceCode"] == SourceCodeJobFacet("python", CODE)


def test_extract_dag_code_disables_on_no_env():
    extractor = PythonExtractor(python_task_getcwd)
    assert "sourceCode" not in extractor.extract().job_facets


@patch.dict(os.environ, {"OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE": "False"})
def test_extract_dag_code_enables_on_true_env():
    extractor = PythonExtractor(python_task_getcwd)
    assert extractor.extract().job_facets["sourceCode"] == SourceCodeJobFacet(
        "python", "<built-in function getcwd>"
    )


@conf_vars({("openlineage", "disable_source_code"): "False"})
def test_extract_dag_code_enables_on_true_conf():
    extractor = PythonExtractor(python_task_getcwd)
    assert extractor.extract().job_facets["sourceCode"] == SourceCodeJobFacet(
        "python", "<built-in function getcwd>"
    )


@conf_vars({("openlineage", "disable_source_code"): "False"})
@patch.dict(os.environ, {"OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE": "False"})
def test_extract_dag_code_conf_precedence():
    extractor = PythonExtractor(python_task_getcwd)
    assert extractor.extract().job_facets["sourceCode"] == SourceCodeJobFacet(
        "python", "<built-in function getcwd>"
    )


@patch.dict(os.environ, {"OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE": "True"})
def test_extract_dag_code_env_disables_on_true():
    extractor = PythonExtractor(python_task_getcwd)
    metadata = extractor.extract()
    assert metadata is not None
    assert "sourceCode" not in metadata.job_facets


@conf_vars({("openlineage", "disable_source_code"): "True"})
def test_extract_dag_code_conf_disables_on_true():
    extractor = PythonExtractor(python_task_getcwd)
    metadata = extractor.extract()
    assert metadata is not None
    assert "sourceCode" not in metadata.job_facets


@patch.dict(os.environ, {"OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE": "asdftgeragdsfgawef"})
def test_extract_dag_code_env_does_not_disable_on_random_string():
    extractor = PythonExtractor(python_task_getcwd)
    assert extractor.extract().job_facets["sourceCode"] == SourceCodeJobFacet(
        "python", "<built-in function getcwd>"
    )


@conf_vars({("openlineage", "disable_source_code"): "asdftgeragdsfgawef"})
def test_extract_dag_code_conf_does_not_disable_on_random_string():
    extractor = PythonExtractor(python_task_getcwd)
    assert extractor.extract().job_facets["sourceCode"] == SourceCodeJobFacet(
        "python", "<built-in function getcwd>"
    )
