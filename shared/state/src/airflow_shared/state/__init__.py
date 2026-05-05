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

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class TaskScope:
    """
    Identifies the state namespace for a single task instance (or retry thereof).

    ``map_index`` defaults to ``-1`` for non-mapped tasks. For mapped tasks,
    set it to the actual mapped index. ``get``/``set``/``delete`` always match
    on ``(dag_id, run_id, task_id, map_index)`` exactly. To wipe state across
    every map index of the task, call ``clear``/``aclear`` with
    ``all_map_indices=True``.
    """

    dag_id: str
    run_id: str
    task_id: str
    map_index: int = -1


@dataclass(frozen=True)
class AssetScope:
    """Identifies the state namespace for an asset."""

    asset_id: int


StateScope = TaskScope | AssetScope


class BaseStateBackend(ABC):
    """
    Abstract backend for reading and writing task and asset state.

    Each method receives a ``StateScope`` which is either a ``TaskScope`` or an ``AssetScope``.
    Implementations must handle both types. The standard dispatch pattern is::

        match scope:
            case TaskScope():
                ...  # task-specific storage
            case AssetScope():
                ...  # asset-specific storage

    Custom backends are configured via ``[state_store] backend`` in ``airflow.cfg``.
    """

    @abstractmethod
    def get(self, scope: StateScope, key: str) -> str | None:
        """
        Return the stored value, or None if the key does not exist.

        Must handle both ``TaskScope`` and ``AssetScope``.
        """

    @abstractmethod
    def set(self, scope: StateScope, key: str, value: str) -> None:
        """
        Write or overwrite the value for the given key.

        Must handle both ``TaskScope`` and ``AssetScope``.
        """

    @abstractmethod
    def delete(self, scope: StateScope, key: str) -> None:
        """
        Delete a single key. No-op if the key does not exist.

        Must handle both ``TaskScope`` and ``AssetScope``.
        """

    @abstractmethod
    def clear(self, scope: StateScope, *, all_map_indices: bool = False) -> None:
        """
        Delete all keys under the given scope.

        Must handle both ``TaskScope`` and ``AssetScope``.

        For ``TaskScope``: by default, only keys for the exact ``map_index`` on the
        scope are cleared. Pass ``all_map_indices=True`` to drop the ``map_index``
        filter entirely and wipe state across every mapped instance of the task.
        For ``AssetScope`` the flag has no effect.
        """

    @abstractmethod
    async def aget(self, scope: StateScope, key: str) -> str | None:
        """Async variant of get. Must handle both ``TaskScope`` and ``AssetScope``."""

    @abstractmethod
    async def aset(self, scope: StateScope, key: str, value: str) -> None:
        """Async variant of set. Must handle both ``TaskScope`` and ``AssetScope``."""

    @abstractmethod
    async def adelete(self, scope: StateScope, key: str) -> None:
        """Async variant of delete. Must handle both ``TaskScope`` and ``AssetScope``."""

    @abstractmethod
    async def aclear(self, scope: StateScope, *, all_map_indices: bool = False) -> None:
        """
        Async variant of clear. Must handle both ``TaskScope`` and ``AssetScope``.

        For ``TaskScope``: by default, only keys for the exact ``map_index`` on the
        scope are cleared. Pass ``all_map_indices=True`` to wipe state across every
        mapped instance of the task. For ``AssetScope`` the flag has no effect.
        """
