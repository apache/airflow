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

"""
Public authoring-time API for per-task / per-DAG OpenLineage emission control.

The central function is :func:`extend_global_openlineage_emission_policy`. It lets Dag authors
override the global ``emission_policy`` Airflow configuration on individual tasks or entire
Dags at authoring time, *extending* the deployment-wide policy with a per-Dag / per-task delta.

Quick-start examples
--------------------

**Disable all OpenLineage events for a sensitive task**::

    from airflow.providers.openlineage.api.emission_policy import (
        extend_global_openlineage_emission_policy,
    )

    with DAG("my_dag", ...) as dag:
        extract = PythonOperator(task_id="extract", ...)
        sensitive = PythonOperator(task_id="sensitive", ...)

    extend_global_openlineage_emission_policy(sensitive, emit=False)

**Disable source-code capture for an entire Dag, then re-enable for one task**::

    extend_global_openlineage_emission_policy(dag, include_source_code=False)  # all tasks in dag
    extend_global_openlineage_emission_policy(extract, include_source_code=True)  # override

**Use as the return value (fluent / inline style)**::

    with DAG("my_dag", ...) as dag:
        task = extend_global_openlineage_emission_policy(
            PythonOperator(task_id="my_task", python_callable=my_fn),
            include_source_code=False,
            hook_lineage=False,
        )

**Suppress DAG-run events while keeping task events**::

    extend_global_openlineage_emission_policy(dag, emit_dag_events=False)

**Works with XComArg** — flags are applied to the underlying operator::

    result = extend_global_openlineage_emission_policy(
        my_python_task_function(),  # returns XComArg
        extract_operator_metadata=False,
    )

Flags and locked conf rules
----------------------------

These flags sit **above** the ``emission_policy`` Airflow configuration in the resolution stack.
However, if an Airflow admin marks a conf rule with ``locked: true``, that field is protected
and cannot be overridden here — the attempt is silently ignored and an INFO log is emitted.

Example conf rule that locks include_source_code for all tasks::

    [openlineage]
    emission_policy = [{"scope": {}, "controls": {"include_source_code": false}, "locked": true}]

Even if a Dag author calls
``extend_global_openlineage_emission_policy(task, include_source_code=True)`` after that,
the lock wins and source code is NOT included.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, TypeVar

from airflow.providers.common.compat.sdk import DAG, XComArg
from airflow.providers.openlineage.utils.emission_policy import (
    _DAG_FLAG_KEYS,
    _TASK_FLAG_KEYS,
    EMIT,
    EMIT_DAG_EVENTS,
    EMIT_TASK_EVENTS,
    EXTRACT_OPERATOR_METADATA,
    HOOK_LINEAGE,
    INCLUDE_FULL_TASK_INFO,
    INCLUDE_SOURCE_CODE,
    OL_EMISSION_POLICY_PARAM,
    _merge_param,
)

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import BaseOperator, MappedOperator

    T = TypeVar("T", bound="DAG | BaseOperator | MappedOperator")

log = logging.getLogger(__name__)

__all__ = [
    "extend_global_openlineage_emission_policy",
]


def extend_global_openlineage_emission_policy(
    obj: T,
    *,
    emit: bool | None = None,
    emit_task_events: bool | None = None,
    emit_dag_events: bool | None = None,
    extract_operator_metadata: bool | None = None,
    include_source_code: bool | None = None,
    hook_lineage: bool | None = None,
    include_full_task_info: bool | None = None,
) -> T:
    """
    Extend the global OpenLineage emission policy with per-task / per-DAG overrides.

    Flags left as ``None`` are not set and will fall through to the ``emission_policy``
    Airflow configuration (or its built-in defaults).  Only explicitly provided flags are
    stored — successive calls **merge** into any previously stored flags.

    When called on a **DAG**, flags are applied as follows:

    - Task-relevant flags (``emit``, ``emit_task_events``, ``extract_operator_metadata``,
      ``include_source_code``, ``hook_lineage``, ``include_full_task_info``) are
      **propagated to all tasks** in the Dag at the time of the call.
    - DAG-run-level flags (``emit``, ``emit_dag_events``) are stored on the Dag itself.
    - ``emit_dag_events`` is meaningless on a task and logs a warning if provided.

    .. warning::

        **DAG-level calls only propagate to tasks that already exist on the DAG when the call
        runs.** Tasks added later (for example, when
        ``extend_global_openlineage_emission_policy`` is called inside ``with DAG(...) as dag:``
        before the operators are defined) will **not** inherit the flags. Call
        ``extend_global_openlineage_emission_policy(dag, ...)`` **after** all task definitions,
        or set flags per-task.

    .. note::

        There is no "unset" API. Passing ``None`` for a flag is treated as "not provided" —
        it does not remove a previously-stored value. To override, pass the explicit boolean
        you want, or call again with the new value (successive calls merge, later wins per key).

    ``emit`` is a shorthand that affects both task and Dag events:

    - ``emit=False`` — disables **all** OpenLineage events (task + Dag).
    - ``emit=True, emit_task_events=False`` — Dag-run events only (task events off).
    - ``emit=True, emit_dag_events=False`` — task events only (Dag-run events off).

    These flags sit **above** the ``emission_policy`` Airflow configuration in priority.
    If an admin marks a conf rule with ``locked: true``, that field is protected and cannot
    be overridden by this function.

    .. note::

        **No global authoring scope.** ``extend_global_openlineage_emission_policy`` only
        accepts a single DAG, operator, or :class:`XComArg` — there is no equivalent of
        "apply globally to every DAG in the deployment." Deployment-wide changes are an
        admin concern and belong in the ``emission_policy`` Airflow configuration with an
        empty ``"scope": {}``. Passing any other object type raises :class:`TypeError`.

    :param obj: An Airflow Dag, operator, or XComArg.
    :param emit: Enable/disable all OpenLineage events (shorthand for both scopes).
    :param emit_task_events: Enable/disable task-level events only; takes precedence over
        ``emit`` for task events.
    :param emit_dag_events: Enable/disable Dag-run-level events only; takes precedence over
        ``emit`` for Dag events.  Ignored (with a warning) when called on a task.
    :param extract_operator_metadata: Whether to run operator-specific extractor-based metadata collection.
    :param include_source_code: Whether to include operator source code in Python/Bash operator events.
    :param hook_lineage: Whether to use ``HookLineageCollector`` as a fallback.
    :param include_full_task_info: Whether to include the full serialized operator state.
    :return: The same *obj* — allows use as a decorator or in chained calls.
    """
    if isinstance(obj, XComArg):
        extend_global_openlineage_emission_policy(
            obj.operator,  # type: ignore[arg-type]
            emit=emit,
            emit_task_events=emit_task_events,
            emit_dag_events=emit_dag_events,
            extract_operator_metadata=extract_operator_metadata,
            include_source_code=include_source_code,
            hook_lineage=hook_lineage,
            include_full_task_info=include_full_task_info,
        )
        return obj

    # Type guard: only DAGs and task/operator objects are supported here.
    # There is no global authoring scope — see the function docstring and
    # the 'emission_policy' Airflow configuration for deployment-wide changes.
    if not isinstance(obj, DAG) and not hasattr(obj, "task_id"):
        raise TypeError(
            "extend_global_openlineage_emission_policy() must be called on a DAG, an Operator, "
            f"or an XComArg; got {type(obj).__name__!s}. There is no global authoring scope — "
            "for deployment-wide changes, use the [openlineage] 'emission_policy' Airflow "
            "configuration with a global rule ('scope': {}) instead."
        )

    provided: dict[str, bool] = {
        k: v
        for k, v in {
            EMIT: emit,
            EMIT_TASK_EVENTS: emit_task_events,
            EMIT_DAG_EVENTS: emit_dag_events,
            EXTRACT_OPERATOR_METADATA: extract_operator_metadata,
            INCLUDE_SOURCE_CODE: include_source_code,
            HOOK_LINEAGE: hook_lineage,
            INCLUDE_FULL_TASK_INFO: include_full_task_info,
        }.items()
        if v is not None
    }

    if not provided:
        log.warning(
            "OpenLineage extend_global_openlineage_emission_policy(): no emission-control flags were "
            "provided for %r — the call has no effect. Pass at least one flag (e.g. emit=False) to "
            "store an override.",
            getattr(obj, "task_id", None) or getattr(obj, "dag_id", None) or repr(obj),
        )
        return obj

    if isinstance(obj, DAG):
        dag_flags = {k: v for k, v in provided.items() if k in _DAG_FLAG_KEYS}
        if dag_flags:
            _merge_param(obj, OL_EMISSION_POLICY_PARAM, dag_flags)

        task_flags = {k: v for k, v in provided.items() if k in _TASK_FLAG_KEYS}
        if task_flags:
            for task in obj.task_dict.values():
                _merge_param(task, OL_EMISSION_POLICY_PARAM, task_flags)
    else:  # Task / operator call
        if EMIT_DAG_EVENTS in provided:
            log.warning(
                "OpenLineage extend_global_openlineage_emission_policy(): 'emit_dag_events' has no effect "
                "on a task (task_id=%r) — set it on the Dag instead.",
                getattr(obj, "task_id", repr(obj)),
            )
        task_flags = {k: v for k, v in provided.items() if k in _TASK_FLAG_KEYS}
        if task_flags:
            _merge_param(obj, OL_EMISSION_POLICY_PARAM, task_flags)

    return obj
