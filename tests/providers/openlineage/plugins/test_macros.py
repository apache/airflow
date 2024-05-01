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

import uuid
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
        execution_date="execution_date",
        try_number=1,
    )
    assert lineage_job_name(task_instance) == "dag_id.task_id"


def test_lineage_run_id():
    task_instance = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        execution_date="execution_date",
        try_number=1,
    )
    actual = lineage_run_id(task_instance)
    expected = str(
        uuid.uuid3(
            uuid.NAMESPACE_URL,
            f"{_DAG_NAMESPACE}.dag_id.task_id.execution_date.1",
        )
    )
    assert actual == expected


@mock.patch("airflow.providers.openlineage.plugins.macros.lineage_run_id")
def test_lineage_parent_id(mock_run_id):
    mock_run_id.return_value = "run_id"
    task_instance = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        execution_date="execution_date",
        try_number=1,
    )
    actual = lineage_parent_id(task_instance)
    expected = f"{_DAG_NAMESPACE}/dag_id.task_id/run_id"
    assert actual == expected
