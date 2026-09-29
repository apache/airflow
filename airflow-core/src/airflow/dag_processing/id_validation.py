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
"""Validate dag, task, and task-group ids for serialized Dags."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.serialization.enums import Encoding
from airflow.utils.helpers import validate_group_key, validate_key

if TYPE_CHECKING:
    from airflow.serialization.serialized_objects import LazyDeserializedDAG


def _validate_task_group_ids(task_group: dict | None) -> None:
    if not task_group:
        return
    group_id = task_group.get("_group_id")
    if group_id:
        validate_group_key(group_id)
    for child in task_group.get("children", {}).values():
        if isinstance(child, dict) and "children" in child:
            _validate_task_group_ids(child)


def validate_serialized_dag_ids(dag: LazyDeserializedDAG) -> None:
    """Validate dag, task, and task-group ids in a serialized Dag payload."""
    validate_key(dag.dag_id)
    for task in dag.data["dag"]["tasks"]:
        validate_key(task[Encoding.VAR]["task_id"])
    _validate_task_group_ids(dag.data["dag"].get("task_group"))
