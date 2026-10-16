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
"""ObjectStorage-backed storage for the durable execution journal (Airflow < 3.3)."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from functools import lru_cache
from typing import Any

from airflow.providers.common.ai.utils.task_logger import get_task_logger

log = get_task_logger()

SECTION = "common.ai"

_MISSING = object()


@lru_cache(maxsize=1)
def _get_base_path():
    from airflow.providers.common.compat.sdk import ObjectStoragePath, conf

    path = conf.get(SECTION, "durable_cache_path", fallback="")
    if not path:
        raise ValueError(
            "durable=True requires [common.ai] durable_cache_path to be set. "
            "Example: durable_cache_path = file:///tmp/airflow_durable_cache"
        )
    return ObjectStoragePath(path)


class DurableStorage:
    """
    Stores the durable journal in a single JSON file on ObjectStorage.

    All journal steps are stored as entries
    in a single JSON blob, written to ``{base_path}/{cache_id}.json`` where
    ``cache_id`` is a hash of the task instance's identity (dag, task, run,
    map index) so distinct task instances never share a file.

    The file survives Airflow task retries since it lives outside the
    XCom system. :meth:`delete_steps` removes the steps of a run that succeeded, and
    the file once nothing is left in it.

    Storage failures never fail the task. A write that fails skips that step, which
    runs again on retry. A journal file that exists but cannot be read is left as it
    is for the next attempt, and this attempt runs without journaling; one that is
    not a valid journal is replaced.

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
        # Hash the identity components with a separator that cannot appear in
        # them, so distinct task instances can never alias to the same cache
        # file. A plain ``_``-joined string collides -- e.g. dag ``etl`` + task
        # ``load_data`` and dag ``etl_load`` + task ``data`` both yield
        # ``etl_load_data`` -- letting one task read, overwrite, or delete
        # another's durable cache.
        identity = "\x00".join([dag_id, task_id, run_id, str(map_index)])
        self._cache_id = hashlib.sha256(identity.encode()).hexdigest()
        self._cache: dict[str, Any] | None = None
        # Set when the journal file exists but could not be read: writing would replace
        # the previous attempt's journal with this attempt's steps alone.
        self._unreadable = False

    def _get_path(self):
        return _get_base_path() / f"{self._cache_id}.json"

    def _load_cache(self) -> dict[str, Any]:
        """Load the full cache blob from storage, with in-memory caching."""
        if self._cache is not None:
            return self._cache

        path = self._get_path()
        try:
            text = path.read_text()
        except FileNotFoundError:
            self._cache = {}
            return self._cache
        except Exception:
            log.warning(
                "Durable: could not read the journal file; this attempt runs without it "
                "and leaves it in place for the next one",
                exc_info=True,
            )
            self._unreadable = True
            self._cache = {}
            return self._cache

        try:
            cache = json.loads(text)
        except ValueError:
            cache = None
        if not isinstance(cache, dict):
            # Nothing in it can be replayed (a write cut off part way, say), so start over.
            log.warning("Durable: the journal file is not a valid journal; starting a new one")
            cache = {}
        self._cache = cache
        return self._cache

    def _write(self, text: str) -> None:
        """Persist the encoded journal to storage."""
        path = self._get_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def save_step(self, key: str, entry: dict[str, Any]) -> bool:
        """
        Store one journal step and rewrite the journal file.

        :return: ``True`` if the entry was written, ``False`` if it was skipped: it could
            not be encoded, the write failed, or the journal file could not be read.
        """
        cache = self._load_cache()
        if self._unreadable:
            return False
        previous = cache.get(key, _MISSING)
        cache[key] = entry
        try:
            text = json.dumps(cache)
        except (TypeError, ValueError):
            # Only this entry is skipped; the rest of the journal is still written.
            self._restore(key, previous)
            log.warning("Durable: could not store step", key=key, exc_info=True)
            return False
        try:
            self._write(text)
        except Exception:
            # The step already ran, so a failed write must not fail it; it runs again on retry.
            self._restore(key, previous)
            log.warning("Durable: could not write the journal file", key=key, exc_info=True)
            return False
        return True

    def _restore(self, key: str, previous: Any) -> None:
        """Put back what ``key`` held before a save that was skipped."""
        if self._cache is None:
            return
        if previous is _MISSING:
            self._cache.pop(key, None)
        else:
            self._cache[key] = previous

    def load_step(self, key: str) -> dict[str, Any] | None:
        """Load one journal step, or ``None`` when there is none or it is not a journal entry."""
        raw = self._load_cache().get(key)
        return raw if isinstance(raw, dict) else None

    def delete_steps(self, keys: Iterable[str]) -> None:
        """Remove these steps, and the journal file once nothing is left in it."""
        cache = self._load_cache()
        if self._unreadable:
            # What the file holds is unknown, so it is left alone rather than rewritten or deleted.
            return
        for key in keys:
            cache.pop(key, None)
        # Runs after the run has already succeeded, so it must never raise; a file left
        # behind holds only steps a retry of this run would replay.
        try:
            if cache:
                self._write(json.dumps(cache))
            else:
                self._get_path().unlink()
                self._cache = None
        except FileNotFoundError:
            self._cache = None
        except Exception:
            log.warning("Durable: could not clean up the journal file", exc_info=True)
