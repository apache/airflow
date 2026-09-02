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
"""Business logic backing the task-instance execution routes."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, NoReturn

from fastapi import HTTPException, status

from airflow.models.expandinput import NotFullyPopulated, SchedulerDictOfListsExpandInput
from airflow.models.xcom import XCOM_RETURN_KEY
from airflow.serialization.definitions.mappedoperator import is_mapped
from airflow.serialization.definitions.xcom_arg import SchedulerPlainXComArg, SchedulerXComArg
from airflow.serialization.serialized_objects import _XComRef

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.dagbag import DBDagBag
    from airflow.serialization.definitions.dag import SerializedDAG
    from airflow.serialization.definitions.mappedoperator import SerializedMappedOperator


def client_supports_arg_bindings() -> bool:
    """
    Whether the request's negotiated API version can receive ``arg_bindings``.

    Clients on older versions never see the field (the version migration strips it from
    the response), so the derivation must not run for them.

    Rather than comparing the negotiated version by date, we check the
    ``VersionChangeWithSideEffects`` subclass's ``is_applied`` flag; see
    https://docs.cadwyn.dev/concepts/version_changes/#version-changes-with-side-effects
    """
    # Imported locally: the versions package transitively imports the routes, which import
    # this module, so a top-level import here would be circular.
    from airflow.api_fastapi.execution_api.versions.v2026_10_30 import AddArgBindingsToTIRunContext

    return AddArgBindingsToTIRunContext.is_applied


def get_arg_bindings(dag_bag: DBDagBag, ti: Any, *, session: Session) -> list | None:
    """Extract or derive the stub task's TaskFlow arg spec from its Dag version."""
    if ti.dag_version_id is None:
        return None
    if (dag := dag_bag.get_dag(ti.dag_version_id, session=session)) is None:
        return None
    if (task := dag.task_dict.get(ti.task_id)) is None or not task.is_stub:
        return None
    if is_mapped(task):
        return _resolve_mapped_stub_arg_bindings(task, ti, dag=dag, session=session)
    return task.arg_bindings


def _unsupported_arg_bindings(detail: str) -> NoReturn:
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail={
            "reason": "invalid_arg_bindings",
            "message": f"The stub task's TaskFlow arguments cannot be delivered: {detail}.",
        },
    )


def _resolve_mapped_stub_arg_bindings(
    task: SerializedMappedOperator, ti: Any, *, dag: SerializedDAG, session: Session
) -> list[dict[str, Any]] | None:
    """
    Build the per-map-index arg spec for a mapped (``.expand()``) stub task.

    A mapped stub never instantiates at parse time; the Dag serializer captures its
    per-parameter metadata (declaration order, defaults, value schemas) from the stub
    signature via ``get_mapped_serialized_fields``, and the map-index decomposition is
    delegated to ``SchedulerDictOfListsExpandInput.resolve_expansion_sub_indexes``.
    Dags serialized without the metadata (an older provider) resolve to ``None``: their
    args were never deliverable, so they keep the legacy ignored-args behavior rather
    than receive bindings whose order the server cannot know.
    """
    metadata = getattr(task, "_mapped_arg_binding_params", None)
    if metadata is None:
        return None
    # The isinstance/map_index/unclaimed checks below re-reject what the provider now
    # fails at parse time, for serialized Dags produced by other provider versions.
    expand_input = task._get_specified_expand_input()
    if not isinstance(expand_input, SchedulerDictOfListsExpandInput):
        _unsupported_arg_bindings("expand_kwargs() is not supported on stub tasks")
    if ti.map_index < 0:
        _unsupported_arg_bindings("the task instance has not been expanded to a map index")

    expand_value = expand_input.value
    partial_op_kwargs = task.partial_kwargs.get("op_kwargs") or {}
    if unclaimed := (set(expand_value) | set(partial_op_kwargs)) - {meta["name"] for meta in metadata}:
        _unsupported_arg_bindings(f"kwargs {sorted(unclaimed)} are not in the captured parameter metadata")
    try:
        sub_indexes = expand_input.resolve_expansion_sub_indexes(ti.map_index, ti.run_id, session=session)
    except NotFullyPopulated as e:
        # Neither this nor the ValueError below can happen on the happy path: both take
        # someone clearing upstream TIs or XComs themselves during the DagRun.
        _unsupported_arg_bindings(f"upstream map lengths are not yet known for {sorted(e.missing)}")
    except ValueError as e:
        _unsupported_arg_bindings(str(e))

    spec = []
    for meta in metadata:  # Declaration order, captured at parse time.
        name = meta["name"]
        if name in expand_value:
            entry = _bind_mapped_stub_arg(name, expand_value[name], sub_index=sub_indexes[name])
        elif name in partial_op_kwargs:
            value = partial_op_kwargs[name]
            # XComArgs inside partial() op_kwargs deserialize to _XComRef and are never
            # dereferenced (set_task_dag_references only derefs the expand inputs).
            if isinstance(value, _XComRef):
                value = value.deref(dag)
            entry = _bind_mapped_stub_arg(name, value, sub_index=None)
        elif "default" in meta:
            entry = {"name": name, "kind": "literal", "value": meta["default"], "from_default": True}
        else:
            _unsupported_arg_bindings(f"parameter {name!r} has no expanded, partial, or default value")
        if (value_schema := meta.get("value_schema")) is not None:
            entry["value_schema"] = value_schema
        spec.append(entry)
    return spec


def _bind_mapped_stub_arg(name: str, value: Any, *, sub_index: int | None) -> dict[str, Any]:
    """Build one arg-binding dict; ``sub_index`` is set for expanded kwargs, None for partial ones."""
    if isinstance(value, SchedulerPlainXComArg):
        if value.key != XCOM_RETURN_KEY:
            _unsupported_arg_bindings(f"parameter {name!r} references the XCom key {value.key!r}")
        if sub_index is None and value.operator.is_mapped:
            # A partial() kwarg over a mapped upstream would bind the unmapped XCom row
            # (map_index=-1), which never exists; the aggregated output is inexpressible.
            _unsupported_arg_bindings(
                f"parameter {name!r} references the aggregated output of the mapped task"
                f" {value.operator.task_id!r}"
            )
        entry: dict[str, Any] = {"name": name, "kind": "xcom", "task_id": value.operator.task_id}
        if sub_index is not None:
            if value.operator.is_mapped:
                entry["map_index"] = sub_index
            else:
                entry["element_index"] = sub_index
        return entry
    if isinstance(value, SchedulerXComArg):
        _unsupported_arg_bindings(
            f"parameter {name!r} received a {type(value).__name__}; only direct upstream"
            " task outputs and literals are supported"
        )
    if sub_index is not None:
        # This kwarg was expanded over a literal collection written in the Dag file.
        items = list(value.items()) if isinstance(value, dict) else value
        try:
            value = items[sub_index]
        except (IndexError, KeyError, TypeError):
            _unsupported_arg_bindings(f"parameter {name!r} has no element at expansion index {sub_index}")
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError):
        _unsupported_arg_bindings(
            f"parameter {name!r} carries a {type(value).__name__} value, which cannot cross"
            " the language boundary"
        )
    return {"name": name, "kind": "literal", "value": value}
