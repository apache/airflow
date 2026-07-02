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
Task-state-store-backed durable storage for pydantic-ai agent step caching.

Available on Airflow >= 3.3, where the AIP-103 task state store provides a
per-task-instance key/value store that survives retries within a run and is
cleared when the run is removed. Each cached step is written under its own key
(``model_step_{N}`` / ``tool_step_{N}``); the store handles persistence and,
when ``[workers] state_store_backend`` is configured, transparently offloads
large values to external storage. No ``[common.ai] durable_cache_path`` is
needed.

This module is imported only on Airflow >= 3.3 (see
``AgentOperator._build_durable_storage``); ``NEVER_EXPIRE`` does not exist on
older cores.
"""

from __future__ import annotations

import contextlib
import json
from typing import TYPE_CHECKING, Any

import structlog
from pydantic_ai.messages import ModelMessagesTypeAdapter

from airflow.providers.common.ai.durable.base import TOOL_RESULT_SENTINEL
from airflow.sdk.execution_time.context import NEVER_EXPIRE

if TYPE_CHECKING:
    from pydantic_ai.messages import ModelResponse

    from airflow.sdk.execution_time.context import TaskStateStoreAccessor

log = structlog.get_logger(logger_name="task")


class TaskStateStoreDurableStorage:
    """
    Stores step-level durable caches in the AIP-103 task state store.

    Each model response and tool result is written under its own key, scoped to
    the current task instance. Entries are written with ``NEVER_EXPIRE`` so a
    retry can replay them regardless of ``retry_delay`` or the global retention
    config, and the keys this run touched are deleted on successful completion.

    A run that fails permanently leaves its keys behind (``NEVER_EXPIRE`` skips
    garbage collection); they are removed when the DAG run is cleaned up, since
    task state store rows cascade with the run.

    :param accessor: The task state store accessor for the current task
        instance (``context["task_state_store"]``).
    """

    def __init__(self, accessor: TaskStateStoreAccessor) -> None:
        self._store = accessor
        # Keys written or replayed this run, deleted on cleanup. A divergent
        # retry that takes fewer steps may orphan keys from a longer earlier
        # attempt; those are reclaimed by the DAG-run cascade, not here.
        self._keys: set[str] = set()

    def save_model_response(self, key: str, response: ModelResponse, *, fingerprint: str | None) -> None:
        """Serialize and store a ModelResponse with the request fingerprint that produced it."""
        self._store.set(
            key,
            {
                "fingerprint": fingerprint,
                "data": ModelMessagesTypeAdapter.dump_python([response], mode="json"),
            },
            retention=NEVER_EXPIRE,
        )
        self._keys.add(key)

    def load_model_response(self, key: str) -> tuple[ModelResponse | None, str | None]:
        """
        Load a cached ModelResponse and its stored request fingerprint.

        Returns ``(None, None)`` on a miss or a torn entry, so the step re-runs
        live rather than crashing the task.
        """
        raw = self._store.get(key)
        if not isinstance(raw, dict):
            return None, None
        try:
            messages = ModelMessagesTypeAdapter.validate_python(raw["data"])
        except (KeyError, IndexError, TypeError, ValueError):
            log.warning("Durable: ignoring malformed cached model response", key=key)
            return None, None
        # A foreign/torn entry can validate as a ModelRequest; only a response is replayable.
        if not messages or messages[0].kind != "response":
            return None, None
        self._keys.add(key)
        fingerprint = raw.get("fingerprint")
        return messages[0], fingerprint if isinstance(fingerprint, str) else None  # type: ignore[return-value]

    def save_tool_result(self, key: str, result: Any, *, fingerprint: str | None) -> None:
        """
        Store a tool call result with the call fingerprint that produced it.

        Non-serializable results (e.g. BinaryContent from MCP tools) are skipped
        with a warning -- the tool call still succeeds, but won't be replayed on
        retry.
        """
        try:
            # Probe serializability before writing: a non-serializable result
            # must skip only this entry, not surface as an opaque comms error.
            json.dumps(result)
        except (TypeError, ValueError):
            log.warning(
                "Durable: skipping cache for non-serializable tool result",
                key=key,
                type=type(result).__name__,
            )
            return
        self._store.set(
            key,
            {TOOL_RESULT_SENTINEL: True, "value": result, "fingerprint": fingerprint},
            retention=NEVER_EXPIRE,
        )
        self._keys.add(key)

    def load_tool_result(self, key: str) -> tuple[bool, Any, str | None]:
        """
        Load a cached tool result and its stored call fingerprint.

        Returns a ``(found, value, fingerprint)`` tuple since the cached value
        itself may be ``None``.
        """
        raw = self._store.get(key)
        if not isinstance(raw, dict) or TOOL_RESULT_SENTINEL not in raw:
            return False, None, None
        self._keys.add(key)
        fingerprint = raw.get("fingerprint")
        return True, raw.get("value"), fingerprint if isinstance(fingerprint, str) else None

    def cleanup(self) -> None:
        """Delete the keys this run wrote or replayed after successful execution."""
        for key in self._keys:
            # Runs only after the task has already succeeded, so it must never raise
            # (that would fail a succeeded task). A key left behind by a failed delete
            # is reclaimed by the DAG-run cascade -- hence the deliberately broad catch.
            with contextlib.suppress(Exception):
                self._store.delete(key)
        self._keys.clear()
        log.debug("Durable cache cleaned up")
