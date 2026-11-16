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
"""YAML Dag format version 2027-02-28."""

from __future__ import annotations

from typing import Any

from cadwyn_core import RequestInfo, VersionChange, convert_request_to_next_version_for

from airflow.sdk.importers.yaml_importer.models import CONST_KEY, XCOM_KEYS, DagDocument


def _flatten_task_values(task: Any) -> Any:
    """Flatten XCom references in ``with:`` and ``run:``."""
    if not isinstance(task, dict):
        return task
    return {k: _flatten_xcom_refs(v) if k in ("with", "run") else v for k, v in task.items()}


def _flatten_xcom_refs(value: Any) -> Any:
    """
    Recursively rewrite old XCom ref syntax with new in *value*.

    New containers are built instead of mutating *value*, since YAML anchors can
    share one object between several places in a document.
    """
    if isinstance(value, list):
        return [_flatten_xcom_refs(item) for item in value]
    if not isinstance(value, dict):
        return value
    if len(value) != 1:
        return {k: _flatten_xcom_refs(v) for k, v in value.items()}
    ((marker, operand),) = value.items()
    if marker == CONST_KEY:
        return value
    if marker not in XCOM_KEYS or not isinstance(operand, dict):
        return {marker: _flatten_xcom_refs(operand)}
    if "task" not in operand or not operand.keys() <= {"task", "key"}:
        return value  # Invalid; leave for validation to report.
    if operand.get("key") is None:  # A null key meant the return value.
        return {marker: operand["task"]}
    return {marker: operand["task"], "key": operand["key"]}


class FlattenKeyedXComReference(VersionChange):
    """
    Write a keyed XCom reference as ``{$x: task, key: k}``.

    The key was nested inside the operand as ``{$x: {task: task, key: k}}``
    before. Now it is an option beside the ``$x`` key, as for other markers.
    """

    description = __doc__
    instructions_to_migrate_to_previous_version = ()

    @convert_request_to_next_version_for(DagDocument)  # type: ignore[arg-type]
    def flatten_keyed_xcom_reference(request: RequestInfo) -> None:
        body = request.body
        if isinstance(tasks := body.get("tasks"), list):
            body["tasks"] = [_flatten_task_values(task) for task in tasks]
        if isinstance(templates := body.get("templates"), dict):
            body["templates"] = {name: _flatten_task_values(spec) for name, spec in templates.items()}
