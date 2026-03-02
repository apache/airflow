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
from __future__ import annotations

from unittest import mock

import pytest

from airflow.models.taskmap import TaskMap, TaskMapVariant
from airflow.providers.standard.operators.empty import EmptyOperator

from tests_common.test_utils.taskinstance import create_task_instance

pytestmark = pytest.mark.db_test


def test_task_map_from_task_instance_xcom():
    task = EmptyOperator(task_id="test_task")
    ti = create_task_instance(task=task, run_id="test_run", map_index=0, dag_version_id=mock.MagicMock())
    ti.dag_id = "test_dag"
    value = {"key1": "value1", "key2": "value2"}

    # Test case where run_id is not None
    task_map = TaskMap.from_task_instance_xcom(ti, value)
    assert task_map.dag_id == ti.dag_id
    assert task_map.task_id == ti.task_id
    assert task_map.run_id == ti.run_id
    assert task_map.map_index == ti.map_index
    assert task_map.length == len(value)
    assert task_map.keys == list(value)

    # Test case where run_id is None
    ti.run_id = None
    with pytest.raises(ValueError, match="cannot record task map for unrun task instance"):
        TaskMap.from_task_instance_xcom(ti, value)


def test_task_map_with_invalid_task_instance():
    task = EmptyOperator(task_id="test_task")
    ti = create_task_instance(task=task, run_id=None, map_index=0, dag_version_id=mock.MagicMock())
    ti.dag_id = "test_dag"

    # Define some arbitrary XCom-like value data
    value = {"example_key": "example_value"}

    with pytest.raises(ValueError, match="cannot record task map for unrun task instance"):
        TaskMap.from_task_instance_xcom(ti, value)


def test_task_map_variant():
    # Test case where keys is None
    task_map = TaskMap(
        dag_id="test_dag",
        task_id="test_task",
        run_id="test_run",
        map_index=0,
        length=3,
        keys=None,
    )
    assert task_map.variant == TaskMapVariant.LIST

    # Test case where keys is not None
    task_map.keys = ["key1", "key2"]
    assert task_map.variant == TaskMapVariant.DICT
