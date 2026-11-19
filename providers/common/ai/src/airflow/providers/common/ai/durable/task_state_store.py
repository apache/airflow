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
Task-state-store-backed storage for the durable execution journal.

Available on Airflow >= 3.3, where the AIP-103 task state store provides a
per-task-instance key/value store that survives retries within a run and is
cleared when the run is removed. Each journal step is written under its own key
(``step_{N}``, prefixed with the reserved ``DURABLE_KEY_PREFIX`` so it cannot
collide with user keys in the shared key namespace); the store handles
persistence and, when ``[workers] state_store_backend`` is configured,
transparently offloads large values to external storage. No
``[common.ai] durable_cache_path`` is needed.

This module is imported only on Airflow >= 3.3 (see
:func:`~airflow.providers.common.ai.durable.journal.build_task_storage`);
``NEVER_EXPIRE`` does not exist on older Airflow versions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.utils.task_logger import get_task_logger
from airflow.sdk.execution_time.context import NEVER_EXPIRE

if TYPE_CHECKING:
    from collections.abc import Iterable

    from airflow.sdk.execution_time.context import TaskStateStoreAccessor

log = get_task_logger()


class TaskStateStoreDurableStorage:
    """
    Stores durable journal steps in the AIP-103 task state store.

    Each step is written under its own key, scoped to the current task instance.
    Entries are written with ``NEVER_EXPIRE`` so a retry can replay them regardless
    of ``retry_delay`` or the global retention config, and the keys this attempt
    touched are deleted by :meth:`delete_steps` once the run succeeds.

    A run that fails permanently leaves its keys behind (``NEVER_EXPIRE`` skips
    garbage collection); they are removed when the Dag run is cleaned up, since
    task state store rows cascade with the run.

    :param accessor: The task state store accessor for the current task
        instance (``context["task_state_store"]``).
    """

    def __init__(self, accessor: TaskStateStoreAccessor) -> None:
        self._store = accessor

    def save_step(self, key: str, entry: dict[str, Any]) -> bool:
        """
        Store one journal step.

        Best-effort: the save runs *after* the step already succeeded, so a failed
        write (e.g. a value over the backend's size limit) must not fail the step.
        It is skipped with a warning, and the step runs again on the next retry.

        :return: ``True`` if the entry was written, ``False`` if it was skipped.
        """
        try:
            self._store.set(key, entry, retention=NEVER_EXPIRE)
        except Exception:
            log.warning("Durable: could not store step", key=key, exc_info=True)
            return False
        return True

    def load_step(self, key: str) -> dict[str, Any] | None:
        """Load one journal step, or ``None`` when there is none or it is not a journal entry."""
        raw = self._store.get(key)
        return raw if isinstance(raw, dict) else None

    def delete_steps(self, keys: Iterable[str]) -> None:
        """Delete these keys; a key left behind by a failed delete is reclaimed by the Dag-run cascade."""
        for key in keys:
            # Runs only after the run has already succeeded, so it must never raise
            # (that would fail a succeeded task) -- hence the deliberately broad catch.
            # Log it so an offloaded value orphaned in external storage is at least visible.
            try:
                self._store.delete(key)
            except Exception:
                log.warning("Durable: failed to delete journal key", key=key, exc_info=True)
