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
from unittest.mock import patch

import pytest
from openlineage.client.facet import SourceCodeJobFacet

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.openlineage.extractors.bash import BashExtractor
from airflow.providers.openlineage.plugins.facets import UnknownOperatorAttributeRunFacet

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
    operator = BashOperator(task_id="taskid", bash_command="exit 0")
    result = BashExtractor(operator).extract()
    assert "sourceCode" not in result.job_facets
    assert "unknownSourceAttribute" in result.run_facets


@patch("airflow.providers.openlineage.conf.is_source_enabled")
def test_extract_operator_bash_command_enabled(mocked_source_enabled):
    mocked_source_enabled.return_value = True
    operator = BashOperator(task_id="taskid", bash_command="exit 0")
    result = BashExtractor(operator).extract()
    assert result.job_facets["sourceCode"] == SourceCodeJobFacet("bash", "exit 0")
    assert "unknownSourceAttribute" in result.run_facets
    unknown_operator_facet = result.run_facets["unknownSourceAttribute"]
    assert isinstance(unknown_operator_facet, UnknownOperatorAttributeRunFacet)
    assert len(unknown_operator_facet.unknownItems) == 1
    assert unknown_operator_facet.unknownItems[0].name == "BashOperator"
