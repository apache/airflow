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

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime

    from pydantic import JsonValue
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.orm import Session


@dataclass(frozen=True)
class TaskScope:
    """
    Identifies the state namespace for a single task instance (or retry thereof).

    ``map_index`` defaults to ``-1`` for non-mapped tasks. For mapped tasks,
    set it to the actual mapped index. ``get``/``set``/``delete`` always match
    on ``(dag_id, run_id, task_id, map_index)`` exactly.
    """

    dag_id: str
    run_id: str
    task_id: str
    map_index: int = -1


@dataclass(frozen=True)
class AssetScope:
    """
    Identifies the state namespace for an asset.

    Server-side backends receive ``asset_id``. Worker-side backends receive ``name`` or ``uri``
    since workers do not have access to the integer ``asset_id``.

    Note: ``name`` and ``uri`` are not guaranteed to be unique over time — if an asset is
    deactivated and a new one created with the same name, both share the same ``name`` value.
    State for inactive assets is cleaned up by the orphan GC pass; until then, stale rows exist
    in the DB but cannot be written to (the Execution API resolver filters to active assets only).
    """

    asset_id: int | None = None
    name: str | None = None
    uri: str | None = None

    def __post_init__(self) -> None:
        if self.asset_id is None and self.name is None and self.uri is None:
            raise ValueError("AssetScope requires at least one of: asset_id, name, or uri")


StoreScope = TaskScope | AssetScope


class AssetStateStoreWriterKind(str, Enum):
    """
    Identifies what kind of writer last updated an asset state store entry.

    ``TASK`` — written by a task via the execution API.
    ``WATCHER`` — written by a ``BaseEventTrigger`` (no task instance).
    ``API`` — written directly through the Core API (e.g. manual admin write).
    """

    TASK = "task"
    WATCHER = "watcher"
    API = "api"

    def validate_writer_fields(
        self,
        dag_id: str | None,
        run_id: str | None,
        task_id: str | None,
        map_index: int | None,
    ) -> None:
        task_fields = (dag_id, run_id, task_id, map_index)
        match self:
            case AssetStateStoreWriterKind.TASK:
                if any(f is None for f in task_fields):
                    raise ValueError(
                        f"kind='task' requires dag_id, run_id, task_id, and map_index to all be set; "
                        f"got dag_id={dag_id!r}, run_id={run_id!r}, task_id={task_id!r}, map_index={map_index!r}"
                    )
            case AssetStateStoreWriterKind.WATCHER | AssetStateStoreWriterKind.API:
                if any(f is not None for f in task_fields):
                    raise ValueError(
                        f"kind={self.value!r} must not carry task fields; "
                        f"got dag_id={dag_id!r}, run_id={run_id!r}, task_id={task_id!r}, map_index={map_index!r}"
                    )
            case _:
                raise AssertionError(f"Unhandled AssetStateStoreWriterKind: {self!r}")


class BaseStoreBackend(ABC):
    """
    Abstract backend for reading and writing task and asset state.

    Each method receives a ``StoreScope`` which is either a ``TaskScope`` or an ``AssetScope``.
    Implementations must handle both types. The standard dispatch pattern is::

        match scope:
            case TaskScope():
                ...  # task-specific storage
            case AssetScope():
                ...  # asset-specific storage

    Custom backends are configured via ``[state_store] backend`` in ``airflow.cfg``.

    **The ``session`` parameter on ``get``, ``set``, ``delete``, and ``clear``:**

    The default ``MetastoreStateBackend`` passes a SQLAlchemy ``Session`` through
    these methods. Custom backends that do not use SQLAlchemy should accept ``session`` as a
    keyword argument and ignore it.
    """

    @abstractmethod
    def get(self, scope: StoreScope, key: str, *, session: Session | None = None) -> str | None:
        """
        Return the stored JSON encoded value string, or None if the key does not exist.

        Must handle both ``TaskScope`` and ``AssetScope``. The execution API calls
        ``json.loads`` on the returned string from here, so it must be a valid JSON document.
        """

    @abstractmethod
    def set(
        self,
        scope: StoreScope,
        key: str,
        value: str,
        *,
        expires_at: datetime | None = None,
        session: Session | None = None,
    ) -> None:
        """
        Write or overwrite ``value`` for the given key.

        Must handle both ``TaskScope`` and ``AssetScope``. ``value`` is always a
        JSON encoded string (the execution API calls ``json.dumps`` before passing it
        here); store it verbatim so ``get`` can return it unchanged.

        ``expires_at`` is an absolute UTC datetime after which the row may be deleted.
        Pass ``None`` (default) for a key that should never expire — stored as ``NULL``,
        skipped by garbage collection.
        """

    @abstractmethod
    def delete(self, scope: StoreScope, key: str, *, session: Session | None = None) -> None:
        """
        Delete a single key. No-op if the key does not exist.

        Must handle both ``TaskScope`` and ``AssetScope``.
        """

    @abstractmethod
    def clear(
        self, scope: StoreScope, *, all_map_indices: bool = False, session: Session | None = None
    ) -> None:
        """
        Delete all keys under the given scope.

        Must handle both ``TaskScope`` and ``AssetScope``.

        For ``TaskScope``: by default, only keys for the exact ``map_index`` on the
        scope are cleared. When ``all_map_indices=True``, the ``map_index`` filter is
        dropped and state is wiped across every mapped instance — for use by external
        callers (UI, CLI) only, not from within a running task.
        For ``AssetScope`` the flag has no effect.
        """

    @abstractmethod
    async def aget(self, scope: StoreScope, key: str, *, session: AsyncSession | None = None) -> str | None:
        """
        Async variant of ``get`` which returns a JSON encoded value string or None.

        Must handle both ``TaskScope`` and ``AssetScope``. ``session`` is used directly
        when provided; otherwise implementations manage their own session internally.
        """

    @abstractmethod
    async def aset(
        self,
        scope: StoreScope,
        key: str,
        value: str,
        *,
        expires_at: datetime | None = None,
        session: AsyncSession | None = None,
    ) -> None:
        """
        Async variant of ``set``. ``value`` is always a JSON encoded string.

        Must handle both ``TaskScope`` and ``AssetScope``. ``session`` is used directly
        when provided; otherwise implementations manage their own session internally.
        """

    @abstractmethod
    async def adelete(self, scope: StoreScope, key: str, *, session: AsyncSession | None = None) -> None:
        """
        Async variant of delete. Must handle both ``TaskScope`` and ``AssetScope``.

        ``session`` is optional. If provided, implementations should use it directly.
        If ``None``, implementations manage their own async session internally.
        """

    @abstractmethod
    async def aclear(
        self, scope: StoreScope, *, all_map_indices: bool = False, session: AsyncSession | None = None
    ) -> None:
        """
        Async variant of clear. Must handle both ``TaskScope`` and ``AssetScope``.

        For ``TaskScope``: by default, only keys for the exact ``map_index`` on the
        scope are cleared. When ``all_map_indices=True``, the ``map_index`` filter is
        dropped and state is wiped across every mapped instance — for use by external
        callers (UI, CLI) only, not from within a running task.
        For ``AssetScope`` the flag has no effect.

        ``session`` is optional. If provided, implementations should use it directly.
        If ``None``, implementations manage their own async session internally.
        """

    def cleanup(self) -> None:
        """
        Remove expired and orphaned state records.

        This is a no-op by default. Custom backends override this to implement their own
        retention policy. The backend is responsible for reading any relevant config (e.g.
        ``[state_store] default_retention_days``) and deciding what to delete.
        """

    def serialize_task_state_store_to_ref(self, *, value: JsonValue, key: str, scope: TaskScope) -> str:
        """
        Serialize a task state store value before it is sent to the execution API for db persistence.

        Called by ``TaskStateStoreAccessor.set()`` on the worker. The return value is what gets
        stored in the DB — typically a reference path (e.g. an S3 key) rather than the
        actual value. Default: return ``value`` unchanged.

        **Important:** return only the raw reference string. The worker framework automatically
        wraps it in ``{"__airflow_state_ref__": "<ref>"}`` before writing to the DB, and strips
        that wrapper before passing ``stored`` to ``deserialize_task_state_store_from_ref()``. Do not
        wrap the reference yourself.

        The returned reference must be deterministic — given the same ``scope`` and ``key`` it
        must always return the same string. Do not use timestamps or random UUIDs as part of
        the reference, otherwise ``delete()``/``clear()`` cannot reconstruct it and the external
        object will be orphaned. By default, it JSON dumps the value and returns a JSON string.
        """
        return json.dumps(value)

    def deserialize_task_state_store_from_ref(self, stored: str) -> JsonValue:
        """
        Resolve a stored task state store reference back to the actual value.

        Called by ``TaskStateStoreAccessor.get()`` after the stored string is retrieved from
        the execution API. By default, it JSON decodes ``stored`` to reverse the default
        ``serialize_task_state_store_to_ref`` encoding.
        """
        return json.loads(stored)

    def serialize_asset_state_store_to_ref(self, *, value: JsonValue, key: str, scope: AssetScope) -> str:
        """
        Serialize an asset state store value before it is sent to the Execution API for db persistence.

        Called by ``AssetStateStoreAccessor.set()`` on the worker. The return value is what gets
        stored in the DB — typically a reference path rather than the actual value.
        Default: return ``value`` unchanged.

        **Important:** return only the raw reference string. The worker framework automatically
        wraps it in ``{"__airflow_state_ref__": "<ref>"}`` before writing to the DB, and strips
        that wrapper before passing ``stored`` to ``deserialize_asset_state_store_from_ref()``. Do not
        wrap the reference yourself.

        The returned reference must be deterministic — given the same ``scope`` and ``key`` it
        must always return the same string. Do not use timestamps or random UUIDs as part of
        the reference, otherwise ``delete()``/``clear()`` cannot reconstruct it and the external
        object will be orphaned. By default, it JSON dumps the value and returns a JSON string.
        """
        return json.dumps(value)

    def deserialize_asset_state_store_from_ref(self, stored: str) -> JsonValue:
        """
        Resolve a stored asset state store reference back to the actual value.

        Called by ``AssetStateStoreAccessor.get()`` after the stored string is retrieved from
        the Execution API. By default, it JSON decodes ``stored`` to reverse the default
        ``serialize_asset_state_store_to_ref`` encoding.
        """
        return json.loads(stored)
