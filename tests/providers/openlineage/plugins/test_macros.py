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

from airflow.providers.openlineage.plugins.adapter import _DAG_NAMESPACE
from airflow.providers.openlineage.plugins.macros import lineage_parent_id, lineage_run_id


def test_lineage_run_id():
    task = mock.MagicMock(
        dag_id="dag_id", execution_date="execution_date", try_number=1, task=mock.MagicMock(task_id="task_id")
    )
    actual = lineage_run_id(task)
    expected = str(
        uuid.uuid3(
            uuid.NAMESPACE_URL,
            f"{_DAG_NAMESPACE}.dag_id.task_id.execution_date.1",
        )
    )
    assert actual == expected


def test_lineage_parent_id():
    task = mock.MagicMock(
        dag_id="dag_id", execution_date="execution_date", try_number=1, task=mock.MagicMock(task_id="task_id")
    )
    actual = lineage_parent_id(run_id="run_id", task_instance=task)
    job_name = str(
        uuid.uuid3(
            uuid.NAMESPACE_URL,
            f"{_DAG_NAMESPACE}.dag_id.task_id.execution_date.1",
        )
    )
    expected = f"{_DAG_NAMESPACE}/{job_name}/run_id"
    assert actual == expected
