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
Shared lineage validation logic for the Informatica provider.

This module provides functions that resolve inlet/outlet URIs and table
references against Informatica EDC.  The resolution logic is intentionally
separated from the listener so it can also be used as an operator
``pre_execute`` hook — where a raised exception *does* fail the task.

Usage as ``pre_execute``::

    from airflow.providers.informatica.lineage.validation import validate_informatica_lineage

    SQLExecuteQueryOperator(
        task_id="my_task",
        conn_id="my_conn",
        sql="INSERT INTO dst SELECT * FROM src",
        pre_execute=validate_informatica_lineage,
    )

When passed as ``pre_execute``, any
:class:`InformaticaLineageResolutionError` propagates through the task
runner and fails the task *before* ``execute()`` is called.

The listener calls the same functions but wraps them in a ``try/except``
so resolution errors are logged as warnings instead of failing the task
(listener exceptions are swallowed by the Airflow task runner).
"""

from __future__ import annotations

import logging
from typing import Any

from airflow.providers.informatica.conf import auto_lineage_enabled, is_operator_disabled
from airflow.providers.informatica.extractors import InformaticaLineageExtractor
from airflow.providers.informatica.hooks.edc import InformaticaEDCError
from airflow.providers.informatica.lineage.resolver import get_resolver
from airflow.providers.informatica.lineage.selective import is_task_auto_lineage_disabled

log = logging.getLogger(__name__)

# Maximum number of entries kept in the pre-execute result cache.  This is a
# safety bound — under normal operation each entry is consumed (popped) by the
# listener's ``on_task_instance_running`` hook shortly after it is stored.
# Orphaned entries (e.g. task killed between pre_execute and listener hook) are
# evicted in FIFO order once the limit is reached.
_PRE_EXECUTE_CACHE_MAX = 1024

# Cache shared between ``validate_informatica_lineage`` (pre_execute) and the
# listener.  Keyed by (dag_id, run_id, task_id, map_index, try_number).
# Access only via ``pop_pre_execute_result`` / ``_store_pre_execute_result``.
_pre_execute_cache: dict[tuple, tuple[list[tuple[str, str]], list[tuple[str, str]]]] = {}


def pop_pre_execute_result(
    key: tuple,
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]] | None:
    """Remove and return a cached pre-execute result, or ``None`` if absent."""
    return _pre_execute_cache.pop(key, None)


def _store_pre_execute_result(
    key: tuple,
    value: tuple[list[tuple[str, str]], list[tuple[str, str]]],
) -> None:
    """Store a pre-execute result, evicting oldest entries when the cache is full."""
    if len(_pre_execute_cache) >= _PRE_EXECUTE_CACHE_MAX:
        oldest_key = next(iter(_pre_execute_cache))
        _pre_execute_cache.pop(oldest_key, None)
        log.debug("Pre-execute cache full, evicted key %s", oldest_key)
    _pre_execute_cache[key] = value


# Lazy singleton for the default hook used by ``validate_informatica_lineage``.
_default_hook: InformaticaLineageExtractor | None = None


def _get_default_hook() -> InformaticaLineageExtractor:
    """Return (and lazily create) a shared hook instance for pre-execute validation."""
    global _default_hook
    if _default_hook is None:
        from airflow.providers.informatica.hooks.edc import InformaticaEDCHook

        _default_hook = InformaticaLineageExtractor(edc_hook=InformaticaEDCHook())
    return _default_hook


class InformaticaLineageResolutionError(RuntimeError):
    """Raised when an EDC object cannot be resolved for a lineage URI."""


def resolve_uri_to_object_id(hook: InformaticaLineageExtractor, uri: str) -> str:
    """
    Resolve an EDC lineage URI to an Informatica catalog object ID.

    Manual lineage entries are treated as concrete object identifiers/uris.
    They are validated directly via ``get_object`` instead of being reparsed
    and looked up again with ``find_object_id``.

    :raises InformaticaLineageResolutionError: When the URI cannot be resolved.
    """
    try:
        obj = hook.get_object(uri)
    except InformaticaEDCError as exc:
        raise InformaticaLineageResolutionError(
            f"Failed to resolve EDC object for URI {uri!r}: {exc}"
        ) from exc

    object_id = obj.get("id") if isinstance(obj, dict) else None
    if not object_id:
        raise InformaticaLineageResolutionError(
            f"Could not resolve EDC object for URI {uri!r}. "
            "Ensure the object exists in the Informatica catalog."
        )
    log.debug("Resolved URI %r to EDC object_id=%s", uri, object_id)
    return object_id


def resolve_uris(
    hook: InformaticaLineageExtractor,
    items: list,
    role: str,
    task_id: str,
) -> list[tuple[str, str]]:
    """
    Resolve URI items to ``(uri, edc_object_id)`` tuples.

    :raises InformaticaLineageResolutionError: On the first URI that cannot
        be resolved.
    """
    result: list[tuple[str, str]] = []
    for item in items:
        if isinstance(item, dict) and "dataset_uri" in item:
            uri = item["dataset_uri"]
        elif isinstance(item, str):
            uri = item
        elif hasattr(item, "uri") and isinstance(item.uri, str):
            uri = item.uri
        else:
            raise InformaticaLineageResolutionError(
                f"Invalid {role} entry for task {task_id!r}: expected a URI string, "
                f"dict with 'dataset_uri', or an Asset object, got {type(item).__name__!r}."
            )
        object_id = resolve_uri_to_object_id(hook, uri)
        result.append((uri, object_id))
    return result


def resolve_table_refs(
    hook: InformaticaLineageExtractor,
    refs: list,
    task_id: str,
) -> list[tuple[str, str]]:
    """
    Resolve TableRef objects to ``(table_label, edc_object_id)`` tuples.

    Calls ``find_object_id`` which searches EDC by table name and narrows by
    schema/database when multiple results are returned.

    :raises InformaticaLineageResolutionError: On the first unresolvable table.
    """
    result: list[tuple[str, str]] = []
    for ref in refs:
        catalog = ref.database or ""
        schema = ref.schema or ""
        table = ref.table
        try:
            object_id = hook.find_object_id(catalog, schema, table)
        except InformaticaEDCError as e:
            raise InformaticaLineageResolutionError(
                f"EDC error while resolving table {table!r} "
                f"(catalog={catalog!r}, schema={schema!r}) for task {task_id!r}: {e}"
            ) from e
        if not object_id:
            raise InformaticaLineageResolutionError(
                f"Could not resolve EDC object for table {table!r} "
                f"(catalog={catalog!r}, schema={schema!r}) in task {task_id!r}. "
                "Ensure the table is registered in the Informatica catalog."
            )
        result.append((f"{catalog}/{schema}/{table}", object_id))
    return result


def resolve_informatica_lineage(
    task: Any,
    task_id: str,
    hook: InformaticaLineageExtractor | None = None,
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """
    Resolve all inlet/outlet URIs or auto-detected tables for *task*.

    :returns: ``(valid_inlets, valid_outlets)`` — each a list of
        ``(uri_or_label, edc_object_id)`` tuples.
    :raises InformaticaLineageResolutionError: When any URI or table cannot
        be resolved in the Informatica catalog.
    """
    if hook is None:
        hook = _get_default_hook()

    if is_operator_disabled(task):
        log.debug("Lineage disabled for operator %s - skipping", type(task).__name__)
        return [], []

    inlets = getattr(task, "inlets", [])
    outlets = getattr(task, "outlets", [])

    if inlets or outlets:
        valid_inlets = resolve_uris(hook, inlets, "inlet", task_id)
        valid_outlets = resolve_uris(hook, outlets, "outlet", task_id)
    elif auto_lineage_enabled() and not is_task_auto_lineage_disabled(task):
        resolver = get_resolver(task)
        if resolver is not None:
            result = resolver.resolve(task)
            if result is not None:
                source_refs, target_refs = result
                log.info(
                    "Auto-lineage detected %d source(s) and %d target(s) for task %s",
                    len(source_refs),
                    len(target_refs),
                    task_id,
                )
                valid_inlets = resolve_table_refs(hook, source_refs, task_id)
                valid_outlets = resolve_table_refs(hook, target_refs, task_id)
            else:
                valid_inlets, valid_outlets = [], []
        else:
            valid_inlets, valid_outlets = [], []
    else:
        valid_inlets, valid_outlets = [], []

    return valid_inlets, valid_outlets


def validate_informatica_lineage(context: Any) -> None:
    """
    Pre-execute hook that validates Informatica lineage before task execution.

    Pass this function as ``pre_execute`` on any operator to fail the task
    when inlet/outlet URIs cannot be resolved in the Informatica catalog::

        SQLExecuteQueryOperator(
            task_id="my_task",
            conn_id="my_conn",
            sql="INSERT INTO dst SELECT * FROM src",
            pre_execute=validate_informatica_lineage,
        )

    Resolved pairs are cached so the listener's ``on_task_instance_success``
    can create lineage links without making a second round of EDC calls.

    :raises InformaticaLineageResolutionError: When any URI or table cannot
        be resolved.
    """
    ti = context.get("task_instance") or context.get("ti")
    if ti is None:
        return

    task = getattr(ti, "task", None)
    if task is None:
        return

    task_id = getattr(ti, "task_id", "unknown")

    valid_inlets, valid_outlets = resolve_informatica_lineage(task, task_id)

    # Store in cache so the listener can pick it up.
    dag_id = getattr(ti, "dag_id", getattr(task, "dag_id", None))
    cache_key = (
        dag_id,
        getattr(ti, "run_id", None),
        task_id,
        getattr(ti, "map_index", -1),
        getattr(ti, "try_number", None),
    )
    _store_pre_execute_result(cache_key, (valid_inlets, valid_outlets))

    log.info(
        "Pre-execute validation complete for task %s: %d inlet(s), %d outlet(s) resolved",
        task_id,
        len(valid_inlets),
        len(valid_outlets),
    )
