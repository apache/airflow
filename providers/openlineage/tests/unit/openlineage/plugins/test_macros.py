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

from datetime import datetime, timezone
from unittest import mock

import pytest

from airflow import __version__
from airflow.providers.openlineage.conf import namespace
from airflow.providers.openlineage.plugins.macros import (
    lineage_job_name,
    lineage_job_namespace,
    lineage_parent_id,
    lineage_root_run_id,
    lineage_run_id,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

_DAG_NAMESPACE = namespace()

if __version__.startswith("2."):
    LOGICAL_DATE_KEY = "execution_date"
else:
    LOGICAL_DATE_KEY = "logical_date"


def test_lineage_job_namespace():
    assert lineage_job_namespace() == _DAG_NAMESPACE


def test_lineage_job_name():
    task_instance = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        try_number=1,
        **{LOGICAL_DATE_KEY: datetime(2020, 1, 1, 1, 1, 1, 0, tzinfo=timezone.utc)},
    )
    assert lineage_job_name(task_instance) == "dag_id.task_id"


def test_lineage_run_id():
    date = datetime(2020, 1, 1, 1, 1, 1, 0, tzinfo=timezone.utc)
    dag_run = mock.MagicMock(run_id="run_id")
    dag_run.logical_date = date
    task_instance = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        dag_run=dag_run,
        logical_date=date,
        try_number=1,
    )

    call_result1 = lineage_run_id(task_instance)
    call_result2 = lineage_run_id(task_instance)

    # random part value does not matter, it just has to be the same for the same TaskInstance
    assert call_result1 == call_result2
    # execution_date is used as most significant bits of UUID
    assert call_result1.startswith("016f5e9e-c4c8-")


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow 3.0+")
def test_lineage_run_after_airflow_3():
    dag_run = mock.MagicMock(run_id="run_id")
    dag_run.run_after = datetime(2020, 1, 1, 1, 1, 1, 0, tzinfo=timezone.utc)
    dag_run.logical_date = None
    task_instance = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        dag_run=dag_run,
        try_number=1,
    )

    call_result1 = lineage_run_id(task_instance)
    call_result2 = lineage_run_id(task_instance)

    # random part value does not matter, it just has to be the same for the same TaskInstance
    assert call_result1 == call_result2
    # execution_date is used as most significant bits of UUID
    assert call_result1.startswith("016f5e9e-c4c8-")


@mock.patch("airflow.providers.openlineage.plugins.macros.lineage_run_id")
def test_lineage_parent_id(mock_run_id):
    mock_run_id.return_value = "run_id"
    task_instance = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        try_number=1,
        **{LOGICAL_DATE_KEY: datetime(2020, 1, 1, 1, 1, 1, 0, tzinfo=timezone.utc)},
    )
    actual = lineage_parent_id(task_instance)
    expected = f"{_DAG_NAMESPACE}/dag_id.task_id/run_id"
    assert actual == expected


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow 3.0+")
def test_lineage_root_run_id_with_runtime_task_instance(create_runtime_ti):
    """Test lineage_root_run_id with real RuntimeTaskInstance object doesn't throw AttributeError."""
    from airflow.sdk.bases.operator import BaseOperator

    task = BaseOperator(task_id="test_task")

    runtime_ti = create_runtime_ti(
        task=task,
        dag_id="test_dag",
        run_id="test_run_id",
    )

    # Explicitly test that the function doesn't throw an AttributeError
    # This was the original issue: AttributeError: 'RuntimeTaskInstance' object has no attribute 'dag_run'
    try:
        assert lineage_root_run_id(runtime_ti) is not None
    except AttributeError as e:
        pytest.fail(f"lineage_root_run_id should not throw AttributeError with RuntimeTaskInstance: {e}")
