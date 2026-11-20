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
A `Strands Agents <https://strandsagents.com/>`__ ``Storage`` backed by the task state store.

Available on Airflow >= 3.3, where the task state store keeps key/value state for a
task instance across its tries within a Dag run; ``NEVER_EXPIRE`` does not exist on older
Airflow versions.
"""

from __future__ import annotations

import asyncio
import base64
import bisect
import builtins
import threading
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

try:
    from strands.storage import Storage
except ImportError as e:
    raise AirflowOptionalProviderFeatureException(e)

from airflow.providers.common.ai.durable.base import DURABLE_KEY_PREFIX
from airflow.sdk.execution_time.context import NEVER_EXPIRE

if TYPE_CHECKING:
    from pydantic import JsonValue

    from airflow.sdk.execution_time.context import TaskStateStoreAccessor

__all__ = ["TaskStateStoreStorage"]

_KEY_PREFIX = f"{DURABLE_KEY_PREFIX}strands/"
# The task state store cannot list keys by prefix, so the keys this storage holds are kept
# in a sorted list under one more key.
_INDEX_KEY = f"{DURABLE_KEY_PREFIX}strands_index"
# Every instance over the same task instance shares that list, so updates to it are
# serialized across the process, not per instance.
_index_lock = threading.Lock()


class TaskStateStoreStorage(Storage):
    """
    A Strands ``Storage`` that keeps its keys in the running task instance's task state store.

    The keys share the task instance's key namespace with anything user code writes through
    ``context["task_state_store"]``, under a reserved prefix that keeps them apart. They are
    written with ``NEVER_EXPIRE``, so a retry finds them whatever ``retry_delay`` or the
    retention config is. Keys left behind by a task that never succeeds are removed with
    the Dag run.

    A value that is valid UTF-8, such as a Strands session snapshot, is stored as text, and
    anything else as base64.

    :param accessor: The task state store of the running task instance,
        ``context["task_state_store"]``.
    """

    def __init__(self, accessor: TaskStateStoreAccessor) -> None:
        self._store = accessor

    async def write(self, key: str, data: bytes) -> None:
        await asyncio.to_thread(self._write, key, data)

    async def read(self, key: str) -> bytes | None:
        return _decode(await asyncio.to_thread(self._store.get, _KEY_PREFIX + key))

    async def delete(self, key: str) -> None:
        await asyncio.to_thread(self._delete, key)

    async def list(self, query: str = "") -> builtins.list[str]:
        keys = await asyncio.to_thread(self._load_index)
        return [key for key in keys if key.startswith(query)]

    def _write(self, key: str, data: bytes) -> None:
        with _index_lock:
            keys = self._load_index()
            if key not in keys:
                # Index first: a listed key with no value is harmless to delete, while a value
                # missing from the index would outlive a delete of everything listed.
                bisect.insort(keys, key)
                self._save_index(keys)
        self._store.set(_KEY_PREFIX + key, _encode(data), retention=NEVER_EXPIRE)

    def _delete(self, key: str) -> None:
        # Value first, so a failure in between leaves the key listed for the next delete.
        self._store.delete(_KEY_PREFIX + key)
        with _index_lock:
            keys = self._load_index()
            if key not in keys:
                return
            keys.remove(key)
            self._save_index(keys)

    def _save_index(self, keys: builtins.list[str]) -> None:
        if not keys:
            self._store.delete(_INDEX_KEY)
            return
        index: builtins.list[JsonValue] = [*keys]
        self._store.set(_INDEX_KEY, index, retention=NEVER_EXPIRE)

    def _load_index(self) -> builtins.list[str]:
        keys = self._store.get(_INDEX_KEY)
        return [key for key in keys if isinstance(key, str)] if isinstance(keys, builtins.list) else []


def _encode(data: bytes) -> JsonValue:
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return {"base64": base64.b64encode(data).decode("ascii")}


def _decode(value: JsonValue) -> bytes | None:
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, dict) and isinstance(encoded := value.get("base64"), str):
        return base64.b64decode(encoded)
    return None
