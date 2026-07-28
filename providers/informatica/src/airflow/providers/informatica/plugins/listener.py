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
from airflow.providers.informatica.extractors import InformaticaLineageExtractor
from airflow.providers.informatica.hooks.edc import InformaticaEDCError
from airflow.providers.informatica.lineage.validation import (
    InformaticaLineageResolutionError,
    pop_pre_execute_result,
    resolve_informatica_lineage,
    resolve_uri_to_object_id,
)

if TYPE_CHECKING:
    from airflow.models import TaskInstance
    from airflow.utils.state import TaskInstanceState

_informatica_listener: InformaticaListener | None = None

# Re-export for backward compatibility.
__all__ = [
    "InformaticaLineageResolutionError",
    "InformaticaListener",
    "get_informatica_listener",
]

# Backward-compatible alias.
_resolve_uri_to_object_id = resolve_uri_to_object_id


class InformaticaListener:
    """Informatica listener sends events on task instance state changes to Informatica EDC for lineage tracking."""

    def __init__(self):
        self.log = logging.getLogger(__name__)
        from airflow.providers.informatica.hooks.edc import InformaticaEDCHook

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
        Best-effort pre-resolution of inlet/outlet URIs before task execution.

        Resolved pairs are cached so ``on_task_instance_success`` can create
        lineage links without making a second round of EDC calls.

        .. note::

            Exceptions raised inside listener hooks are caught and logged by
            the Airflow task runner — they do **not** fail the task.  To fail
            the task when lineage URIs cannot be resolved, use
            :func:`~airflow.providers.informatica.lineage.validation.validate_informatica_lineage`
            as a ``pre_execute`` hook on the operator instead.
        """
        task = getattr(task_instance, "task", None)
        if not task:
            return

        task_id = task_instance.task_id

        # If validate_informatica_lineage was already called via pre_execute,
        # reuse its cached result instead of calling EDC again.
        key = self._cache_key(task_instance)
        pre_exec_result = pop_pre_execute_result(key)
        if pre_exec_result is not None:
            self._resolved_cache[key] = pre_exec_result
            self.log.debug(
                "Reusing pre_execute cache for task %s: %d inlet(s), %d outlet(s)",
                task_id,
                len(pre_exec_result[0]),
                len(pre_exec_result[1]),
            )
            return

        self.log.debug("Pre-resolving lineage for task %s", task_id)

        try:
            valid_inlets, valid_outlets = resolve_informatica_lineage(task, task_id, hook=self.hook)
        except (InformaticaLineageResolutionError, InformaticaEDCError):
            self.log.warning(
                "Could not pre-resolve lineage for task %s - "
                "lineage links will not be created on success. "
                "To fail the task on resolution errors, use "
                "pre_execute=validate_informatica_lineage on the operator.",
                task_id,
                exc_info=True,
            )
            return

        self._resolved_cache[key] = (valid_inlets, valid_outlets)
        self.log.info(
            "Pre-resolution complete for task %s: %d inlet(s), %d outlet(s) resolved",
            task_id,
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


def get_informatica_listener() -> InformaticaListener:
    """Get singleton listener manager."""
    global _informatica_listener
    if not _informatica_listener:
        _informatica_listener = InformaticaListener()
    return _informatica_listener
