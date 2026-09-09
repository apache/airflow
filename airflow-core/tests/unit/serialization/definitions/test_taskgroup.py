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

import pytest

from airflow.api_fastapi.core_api.services.ui.task_group import task_group_to_dict_grid
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, TaskGroup, timezone
from airflow.serialization.serialized_objects import DagSerialization


@pytest.mark.parametrize("group_count", [2, 3])
def test_topological_sort_bidirectional_task_level_cross_group_deps(group_count):
    """Sibling projection cycles do not imply a cycle in the task-level Dag."""
    outbound = {}
    inbound = {}
    with DAG(
        "test_bidirectional_cross_group_deps",
        schedule=None,
        start_date=timezone.datetime(2025, 1, 1),
    ) as dag:
        for group_idx in range(group_count):
            with TaskGroup(f"group_{group_idx}"):
                for other_idx in range(group_count):
                    if group_idx == other_idx:
                        continue
                    outbound[group_idx, other_idx] = EmptyOperator(task_id=f"to_{other_idx}")
                    inbound[group_idx, other_idx] = EmptyOperator(task_id=f"from_{other_idx}")

        for source_idx in range(group_count):
            for target_idx in range(group_count):
                if source_idx != target_idx:
                    outbound[source_idx, target_idx] >> inbound[target_idx, source_idx]

    dag.check_cycle()
    serialized = DagSerialization.from_dict(DagSerialization.to_dict(dag))
    expected_order = [f"group_{idx}" for idx in range(group_count)]

    assert [node.node_id for node in serialized.task_group.topological_sort()] == expected_order
    grid = task_group_to_dict_grid(serialized.task_group)
    assert [child["id"] for child in grid["children"]] == expected_order

    inbound[1, 0] >> outbound[0, 1]
    cyclic_serialized = DagSerialization.from_dict(DagSerialization.to_dict(dag))
    with pytest.raises(ValueError, match="A cyclic dependency occurred"):
        cyclic_serialized.task_group.topological_sort()
