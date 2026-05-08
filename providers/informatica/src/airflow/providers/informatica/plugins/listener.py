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

import logging
from typing import TYPE_CHECKING

from airflow.listeners import hookimpl
from airflow.providers.informatica.conf import auto_lineage_enabled, is_operator_disabled
from airflow.providers.informatica.extractors import InformaticaLineageExtractor
from airflow.providers.informatica.hooks.edc import InformaticaEDCError, InformaticaEDCHook
from airflow.providers.informatica.lineage.resolver import get_resolver
from airflow.providers.informatica.lineage.selective import is_task_auto_lineage_disabled

if TYPE_CHECKING:
    from airflow.models import TaskInstance
    from airflow.utils.state import TaskInstanceState

_informatica_listener: InformaticaListener | None = None


class InformaticaLineageResolutionError(RuntimeError):
    """Raised when an EDC object cannot be resolved for a lineage URI."""


def _resolve_uri_to_object_id(hook: InformaticaLineageExtractor, uri: str) -> str:
    """
    Resolve an EDC lineage URI to an Informatica catalog object ID.

    Manual lineage entries are treated as concrete object identifiers/URIs.
    They are validated directly via ``get_object`` instead of being reparsed
    and looked up again with ``find_object_id``.
    """
    log = logging.getLogger(__name__)
    try:
        obj = hook.get_object(uri)
    except InformaticaEDCError as exc:
        raise InformaticaLineageResolutionError(
            f"Failed to resolve EDC object for URI {uri!r}: {exc}"
        ) from exc

    object_id = obj.get("id") if isinstance(obj, dict) else None
    if not object_id:
        raise InformaticaLineageResolutionError(
            f"Could not resolve EDC object for URI {uri!r}. Ensure the object exists in the Informatica catalog."
        )
    log.debug("Resolved URI %r to EDC object_id=%s", uri, object_id)
    return object_id


class InformaticaListener:
    """Informatica listener sends events on task instance state changes to Informatica EDC for lineage tracking."""

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.hook = InformaticaLineageExtractor(edc_hook=InformaticaEDCHook())
        # Cache: _cache_key(ti) -> (valid_inlets, valid_outlets)
        # Populated by on_task_instance_running (pre-validation), consumed by
        # on_task_instance_success and cleared by on_task_instance_failed.
        self._resolved_cache: dict[tuple, tuple[list[tuple[str, str]], list[tuple[str, str]]]] = {}

    @staticmethod
    def _cache_key(task_instance: TaskInstance) -> tuple:
        dag_id = getattr(task_instance, "dag_id", None)
        if dag_id is None:
            task = getattr(task_instance, "task", None)
            dag_id = getattr(task, "dag_id", None)
        return (
            dag_id,
            getattr(task_instance, "run_id", None),
            task_instance.task_id,
            getattr(task_instance, "map_index", -1),
            getattr(task_instance, "try_number", None),
        )

    @hookimpl
    def on_task_instance_success(
        self, previous_state: TaskInstanceState, task_instance: TaskInstance, *args, **kwargs
    ):
        key = self._cache_key(task_instance)
        cached = self._resolved_cache.pop(key, None)
        if cached is None:
            # Running hook was skipped (e.g. operator disabled) - nothing to do.
            return
        valid_inlets, valid_outlets = cached
        self._create_lineage_links(valid_inlets, valid_outlets, task_instance.task_id)

    @hookimpl
    def on_task_instance_failed(
        self, previous_state: TaskInstanceState, task_instance: TaskInstance, *args, **kwargs
    ):
        # Clean up cache entry so stale entries do not accumulate.
        self._resolved_cache.pop(self._cache_key(task_instance), None)

    @hookimpl
    def on_task_instance_running(
        self, previous_state: TaskInstanceState, task_instance: TaskInstance, *args, **kwargs
    ):
        """
        Validate and pre-resolve all inlet/outlet URIs before the task executes.

        Raises :class:`InformaticaLineageResolutionError` if any URI or table cannot
        be resolved in the Informatica catalog.  This causes Airflow to fail the task
        immediately - before the operator ``execute()`` is called.

        Resolved pairs are cached so ``on_task_instance_success`` can create lineage
        links without making a second round of EDC calls.
        """
        task = getattr(task_instance, "task", None)
        if not task:
            return

        self.log.debug("Pre-validating lineage for task %s", task_instance.task_id)

        if is_operator_disabled(task):
            self.log.debug(
                "Lineage disabled for operator %s - skipping",
                type(task).__name__,
            )
            return

        inlets = getattr(task, "inlets", getattr(task_instance, "inlets", []))
        outlets = getattr(task, "outlets", getattr(task_instance, "outlets", []))

        if inlets or outlets:
            # Manual lineage - strict: any unresolvable URI fails the task immediately.
            valid_inlets = self._resolve_uris_strict(inlets, "inlet", task_instance.task_id)
            valid_outlets = self._resolve_uris_strict(outlets, "outlet", task_instance.task_id)
        elif auto_lineage_enabled() and not is_task_auto_lineage_disabled(task):
            # Auto-lineage - parse SQL, then strictly resolve every detected table.
            resolver = get_resolver(task)
            if resolver is not None:
                result = resolver.resolve(task)
                if result is not None:
                    source_refs, target_refs = result
                    self.log.info(
                        "Auto-lineage detected %d source(s) and %d target(s) for task %s",
                        len(source_refs),
                        len(target_refs),
                        task_instance.task_id,
                    )
                    valid_inlets = self._resolve_table_refs_strict(source_refs, task_instance.task_id)
                    valid_outlets = self._resolve_table_refs_strict(target_refs, task_instance.task_id)
                else:
                    valid_inlets, valid_outlets = [], []
            else:
                valid_inlets, valid_outlets = [], []
        else:
            valid_inlets, valid_outlets = [], []

        # Cache so on_task_instance_success can create links without re-calling EDC.
        self._resolved_cache[self._cache_key(task_instance)] = (valid_inlets, valid_outlets)
        self.log.info(
            "Pre-validation complete for task %s: %d inlet(s), %d outlet(s) resolved",
            task_instance.task_id,
            len(valid_inlets),
            len(valid_outlets),
        )

    def _create_lineage_links(
        self,
        valid_inlets: list[tuple[str, str]],
        valid_outlets: list[tuple[str, str]],
        task_id: str,
    ) -> None:
        """Create EDC lineage links between all resolved inlet and outlet object IDs."""
        for _inlet_uri, inlet_id in valid_inlets:
            for _outlet_uri, outlet_id in valid_outlets:
                try:
                    self.hook.create_lineage_link(inlet_id, outlet_id)
                    self.log.info("Lineage link created: %s -> %s", inlet_id, outlet_id)
                except InformaticaEDCError:
                    self.log.exception("Failed to create lineage link from %s to %s", inlet_id, outlet_id)

    def _resolve_uris_strict(self, items: list, role: str, task_id: str) -> list[tuple[str, str]]:
        """
        Resolve URI items to (uri, edc_object_id) tuples.

        Raises :class:`InformaticaLineageResolutionError` on the first URI that
        cannot be resolved so the task is failed before execution begins.
        """
        result: list[tuple[str, str]] = []
        for item in items:
            if isinstance(item, dict) and "dataset_uri" in item:
                uri = item["dataset_uri"]
            elif isinstance(item, str):
                uri = item
            else:
                raise InformaticaLineageResolutionError(
                    f"Invalid {role} entry for task {task_id!r}: expected a URI string or "
                    f"dict with 'dataset_uri', got {type(item).__name__!r}."
                )
            # Raises InformaticaLineageResolutionError if not found - fails the task.
            object_id = _resolve_uri_to_object_id(self.hook, uri)
            result.append((uri, object_id))
        return result

    def _resolve_table_refs_strict(self, refs: list, task_id: str) -> list[tuple[str, str]]:
        """
        Resolve TableRef objects to (table_label, edc_object_id) tuples.

        Calls ``find_object_id`` which searches EDC by table name and narrows by
        schema/database when multiple results are returned.  Raises
        :class:`InformaticaLineageResolutionError` on the first unresolvable table
        so the task is failed before execution begins.
        """
        result: list[tuple[str, str]] = []
        for ref in refs:
            catalog = ref.database or ""
            schema = ref.schema or ""
            table = ref.table
            try:
                object_id = self.hook.find_object_id(catalog, schema, table)
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


def get_informatica_listener() -> InformaticaListener:
    """Get singleton listener manager."""
    global _informatica_listener
    if not _informatica_listener:
        _informatica_listener = InformaticaListener()
    return _informatica_listener
