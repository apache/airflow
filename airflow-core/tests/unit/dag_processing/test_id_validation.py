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

from airflow.dag_processing.collection import _reject_invalid_serialized_dag_ids
from airflow.dag_processing.id_validation import validate_serialized_dag_ids
from airflow.serialization.enums import DagAttributeTypes as Enum, Encoding
from airflow.serialization.serialized_objects import LazyDeserializedDAG


def _serialized_dag(dag_id: str, task_id: str, group_id: str | None = None) -> LazyDeserializedDAG:
    task_group = None
    if group_id is not None:
        task_group = {"_group_id": group_id, "children": {}}
    return LazyDeserializedDAG(
        data={
            "dag": {
                "dag_id": dag_id,
                "relative_fileloc": "test_dag.py",
                "tasks": [
                    {
                        Encoding.TYPE: Enum.OP,
                        Encoding.VAR: {"task_id": task_id, "task_type": "EmptyOperator"},
                    }
                ],
                "task_group": task_group,
            }
        }
    )


def test_validate_serialized_dag_ids_accepts_valid_ids():
    validate_serialized_dag_ids(_serialized_dag("valid_dag", "valid_task", "valid_group"))


def test_validate_serialized_dag_ids_rejects_invalid_task_id():
    with pytest.raises(ValueError, match="alphanumeric"):
        validate_serialized_dag_ids(_serialized_dag("valid_dag", "bad task"))


def test_reject_invalid_serialized_dag_ids_records_import_error():
    dag = _serialized_dag("valid_dag", "bad task")
    import_errors: dict[tuple[str, str], str] = {}
    accepted = _reject_invalid_serialized_dag_ids("dags-folder", [dag], import_errors)
    assert accepted == []
    assert import_errors[("dags-folder", dag.relative_fileloc)].startswith("ValueError:")
