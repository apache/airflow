#
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
"""Tests for shared TaskInstanceDTO."""

from __future__ import annotations

import uuid

import pytest

from airflow_shared.workloads import TaskInstanceDTO


@pytest.fixture
def minimal_ti_kwargs():
    return {
        "id": uuid.uuid4(),
        "dag_version_id": uuid.uuid4(),
        "task_id": "my_task",
        "dag_id": "my_dag",
        "run_id": "run_1",
        "try_number": 1,
        "pool_slots": 1,
        "queue": "default",
        "priority_weight": 1,
    }


class TestTaskInstanceDTO:
    """Test the shared TaskInstanceDTO model."""

    def test_create_with_required_fields(self, minimal_ti_kwargs):
        ti = TaskInstanceDTO(**minimal_ti_kwargs)
        assert ti is not None
        assert ti.task_id == "my_task"
        assert ti.dag_id == "my_dag"
        assert ti.run_id == "run_1"
        assert ti.try_number == 1
        assert ti.pool_slots == 1
        assert ti.queue == "default"
        assert ti.priority_weight == 1
