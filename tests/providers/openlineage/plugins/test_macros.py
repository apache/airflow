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

from airflow.providers.openlineage.conf import namespace
from airflow.providers.openlineage.plugins.macros import (
    lineage_job_name,
    lineage_job_namespace,
    lineage_parent_id,
    lineage_run_id,
)

_DAG_NAMESPACE = namespace()


def test_lineage_job_namespace():
    assert lineage_job_namespace() == _DAG_NAMESPACE


def test_lineage_job_name():
    task_instance = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        try_number=1,
        execution_date=datetime(2020, 1, 1, 1, 1, 1, 0, tzinfo=timezone.utc),
    )
    assert lineage_job_name(task_instance) == "dag_id.task_id"


def test_lineage_run_id():
    task_instance = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        dag_run=mock.MagicMock(run_id="run_id"),
        execution_date=datetime(2020, 1, 1, 1, 1, 1, 0, tzinfo=timezone.utc),
        try_number=1,
    )

    call_result1 = lineage_run_id(task_instance)
    call_result2 = lineage_run_id(task_instance)

    # random part value does not matter, it just have to be the same for the same TaskInstance
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
        execution_date=datetime(2020, 1, 1, 1, 1, 1, 0, tzinfo=timezone.utc),
    )
    actual = lineage_parent_id(task_instance)
    expected = f"{_DAG_NAMESPACE}/dag_id.task_id/run_id"
    assert actual == expected
