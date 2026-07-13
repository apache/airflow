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
"""ObjectStorage-backed durable storage for pydantic-ai agent step caching."""

from __future__ import annotations

import contextlib
import json
from functools import lru_cache
from typing import Any

import structlog
from pydantic_ai.messages import ModelMessagesTypeAdapter, ModelResponse

# Sentinel to distinguish "cached None" from "no cache entry" for tool results.
# Shared with the task state store backend so the envelope shape cannot drift.
from airflow.providers.common.ai.durable.base import TOOL_RESULT_SENTINEL as _SENTINEL

log = structlog.get_logger(logger_name="task")

SECTION = "common.ai"


@lru_cache(maxsize=1)
def _get_base_path():
    from airflow.providers.common.compat.sdk import conf
    from airflow.sdk import ObjectStoragePath

    path = conf.get(SECTION, "durable_cache_path", fallback="")
    if not path:
        raise ValueError(
            "durable=True requires [common.ai] durable_cache_path to be set. "
            "Example: durable_cache_path = file:///tmp/airflow_durable_cache"
        )
    return ObjectStoragePath(path)


class DurableStorage:
    """
    Stores step-level caches in a single JSON file on ObjectStorage.

    All step caches (model responses and tool results) are stored as entries
    in a single JSON blob, written to a file named after the task execution:
    ``{base_path}/{dag_id}_{task_id}_{run_id}[_{map_index}].json``.

    The file survives Airflow task retries since it lives outside the
    XCom system.  It is deleted on successful task completion.

    :param dag_id: DAG ID of the running task.
    :param task_id: Task ID of the running task.
    :param run_id: DAG run ID.
    :param map_index: Map index for mapped tasks (``-1`` for non-mapped).
    """

    def __init__(
        self,
        *,
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: int = -1,
    ) -> None:
        suffix = f"_{map_index}" if map_index >= 0 else ""
        self._cache_id = f"{dag_id}_{task_id}_{run_id}{suffix}"
        self._cache: dict[str, Any] | None = None

    def _get_path(self):
        return _get_base_path() / f"{self._cache_id}.json"

    def _load_cache(self) -> dict[str, Any]:
        """Load the full cache blob from storage, with in-memory caching."""
        if self._cache is not None:
            return self._cache

        path = self._get_path()
        try:
            self._cache = json.loads(path.read_text())
        except (FileNotFoundError, OSError, json.JSONDecodeError, ValueError):
            self._cache = {}

        return self._cache

    def _save_cache(self) -> None:
        """Persist the in-memory cache blob to storage."""
        path = self._get_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self._cache))

    def save_model_response(self, key: str, response: ModelResponse, *, fingerprint: str | None) -> None:
        """Serialize and store a ModelResponse with the request fingerprint that produced it."""
        cache = self._load_cache()
        # Store the dumped messages as native JSON-compatible objects, not a
        # pre-encoded string: the whole cache is JSON-encoded once in
        # ``_save_cache``, so embedding a string here would double-encode the
        # (large) response payload.
        cache[key] = {
            "fingerprint": fingerprint,
            "data": ModelMessagesTypeAdapter.dump_python([response], mode="json"),
        }
        self._save_cache()

    def load_model_response(self, key: str) -> tuple[ModelResponse | None, str | None]:
        """
        Load a cached ModelResponse and its stored request fingerprint.

        Returns ``(None, None)`` if not cached. Entries written before
        fingerprints existed load with a ``None`` fingerprint.
        """
        cache = self._load_cache()
        raw = cache.get(key)
        if raw is None:
            return None, None
        try:
            if isinstance(raw, dict):
                messages = ModelMessagesTypeAdapter.validate_python(raw["data"])
                fingerprint = raw.get("fingerprint")
            else:
                # Legacy entry: the adapter JSON (a list) was stored directly as a string.
                messages = ModelMessagesTypeAdapter.validate_json(raw)
                fingerprint = None
        except (KeyError, IndexError, ValueError):
            # A torn or malformed entry degrades to a miss (the step re-runs),
            # never a task crash -- the cache is best-effort.
            log.warning("Durable: ignoring malformed cached model response", key=key)
            return None, None
        if not messages:
            return None, None
        return messages[0], fingerprint  # type: ignore[return-value]

    def save_tool_result(self, key: str, result: Any, *, fingerprint: str | None) -> None:
        """
        Store a tool call result with the call fingerprint that produced it.

        Non-serializable results (e.g. BinaryContent from MCP tools) are
        skipped with a warning -- the tool call still succeeds, but won't
        be replayed on retry.
        """
        cache = self._load_cache()
        try:
            # Probe serializability before mutating the shared cache: a
            # non-serializable result must skip only this entry, not break the
            # whole-file ``_save_cache``. TypeError covers unsupported types;
            # ValueError covers circular references.
            json.dumps(result)
        except (TypeError, ValueError):
            log.warning(
                "Durable: skipping cache for non-serializable tool result",
                key=key,
                type=type(result).__name__,
            )
            return
        cache[key] = {_SENTINEL: True, "value": result, "fingerprint": fingerprint}
        self._save_cache()

    def load_tool_result(self, key: str) -> tuple[bool, Any, str | None]:
        """
        Load a cached tool result and its stored call fingerprint.

        Returns a (found, value, fingerprint) tuple since the cached value
        itself could be None. Entries written before fingerprints existed
        load with a ``None`` fingerprint.
        """
        cache = self._load_cache()
        raw = cache.get(key)
        if raw is None:
            return False, None, None
        # Legacy entries were stored as a JSON string; new entries are native dicts.
        if isinstance(raw, str):
            raw = json.loads(raw)
        if not isinstance(raw, dict) or _SENTINEL not in raw:
            return False, None, None
        return True, raw["value"], raw.get("fingerprint")

    def cleanup(self) -> None:
        """Delete the cache file after successful execution."""
        # Best-effort cleanup
        with contextlib.suppress(FileNotFoundError, OSError):
            self._get_path().unlink()
        self._cache = None
        log.debug("Durable cache cleaned up")
