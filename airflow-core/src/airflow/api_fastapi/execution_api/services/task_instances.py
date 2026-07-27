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
from airflow.serialization.definitions.mappedoperator import is_mapped
from airflow.serialization.definitions.xcom_arg import SchedulerPlainXComArg, SchedulerXComArg

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.dagbag import DBDagBag
    from airflow.serialization.definitions.mappedoperator import SerializedMappedOperator

# Task type recorded on the TI row (``TaskInstance.operator``) for
# ``airflow.providers.standard.decorators.stub._StubOperator``. Used to gate the
# serialized-Dag lookup for ``arg_bindings`` so regular tasks never pay for it.
# The gate matches the exact class name; a subclass would need its own entry here.
STUB_TASK_TYPE = "_StubOperator"


def get_arg_bindings(dag_bag: DBDagBag, ti: Any, *, session: Session) -> list | None:
    """Extract or derive the stub task's TaskFlow arg spec from its Dag version."""
    if ti.dag_version_id is None:
        return None
    if (dag := dag_bag.get_dag(ti.dag_version_id, session=session)) is None:
        return None
    if (task := dag.task_dict.get(ti.task_id)) is None:
        return None
    if is_mapped(task):
        return _resolve_mapped_stub_arg_bindings(task, ti, session=session)
    return getattr(task, "_arg_bindings", None)


def _unsupported_arg_bindings(detail: str) -> NoReturn:
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail={
            "reason": "invalid_arg_bindings",
            "message": f"The stub task's TaskFlow arguments cannot be delivered: {detail}.",
        },
    )


def _resolve_mapped_stub_arg_bindings(
    task: SerializedMappedOperator, ti: Any, *, session: Session
) -> list[dict[str, Any]]:
    """
    Build the per-map-index arg spec for a mapped (``.expand()``) stub task.

    A mapped stub never instantiates at parse time, so no spec is captured in the
    serialized Dag; it is derived here from the serialized expand input instead, with
    the map-index decomposition delegated to
    ``SchedulerDictOfListsExpandInput.resolve_expansion_sub_indexes``.
    Value schemas come from the stub function's annotations, which are not available
    server-side, so mapped bindings omit them (runtimes fall back to decode-only checks);
    delivering them is tracked at https://github.com/apache/airflow/issues/70523.
    """
    expand_input = task._get_specified_expand_input()
    # TODO: Support the `expand_kwargs` path once https://github.com/apache/airflow/pull/69757
    # is merged and all the Lang-SDKs adapt it.
    if not isinstance(expand_input, SchedulerDictOfListsExpandInput):
        _unsupported_arg_bindings("expand_kwargs() is not supported on stub tasks")
    if ti.map_index < 0:
        _unsupported_arg_bindings("the task instance has not been expanded to a map index")

    expand_value = expand_input.value
    try:
        sub_indexes = expand_input.resolve_expansion_sub_indexes(ti.map_index, ti.run_id, session=session)
    except NotFullyPopulated as e:
        # In the happy path this shouldn't happen at all, unless someone clears upstream
        # TIs or XComs themselves during the DagRun.
        _unsupported_arg_bindings(f"upstream map lengths are not yet known for {sorted(e.missing)}")

    # partial() kwargs
    spec = [
        _bind_mapped_stub_arg(name, value, sub_index=None)
        for name, value in (task.partial_kwargs.get("op_kwargs") or {}).items()
    ]
    # expand() kwargs
    spec += [
        _bind_mapped_stub_arg(name, value, sub_index=sub_indexes[name])
        for name, value in expand_value.items()
    ]
    return spec


def _bind_mapped_stub_arg(name: str, value: Any, *, sub_index: int | None) -> dict[str, Any]:
    """Build one arg-binding dict; ``sub_index`` is set for expanded kwargs, None for partial ones."""
    if isinstance(value, SchedulerPlainXComArg):
        if value.key != "return_value":
            _unsupported_arg_bindings(f"parameter {name!r} references the XCom key {value.key!r}")
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
