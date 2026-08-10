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
from functools import cache
from typing import TYPE_CHECKING
from urllib.parse import quote, urlsplit

import fsspec.utils

from airflow.providers.common.compat.sdk import conf

if TYPE_CHECKING:
    from datetime import datetime

    from pydantic import JsonValue
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.orm import Session


from airflow.sdk import ObjectStoragePath
from airflow.sdk._shared.state import AssetScope, BaseStoreBackend, StoreScope, TaskScope

SECTION = "common.io"


@cache
def _get_base_path() -> ObjectStoragePath:
    return ObjectStoragePath(conf.get_mandatory_value(SECTION, "state_store_objectstorage_path"))


@cache
def _get_compression() -> str | None:
    value = conf.get(SECTION, "state_store_objectstorage_compression", fallback=None)
    return value or None


@cache
def _get_threshold() -> int:
    value = conf.getint(SECTION, "state_store_objectstorage_threshold", fallback=0)
    if value < 0:
        raise ValueError(
            f"[{SECTION}] state_store_objectstorage_threshold must be non-negative, got {value}."
        )
    return value


def _get_compression_suffix() -> str:
    compression = _get_compression()
    if not compression:
        return ""
    for suffix, c in fsspec.utils.compressions.items():
        if c == compression:
            return f".{suffix}"
    raise ValueError(f"Compression {compression!r} is not supported.")


def _sanitise_segment(value: str) -> str:
    if not value or value in (".", ".."):
        raise ValueError(f"Invalid path segment: {value!r}")
    return quote(value, safe="")


def _build_task_path(scope: TaskScope, key: str) -> ObjectStoragePath:
    suffix = _get_compression_suffix()
    return (
        _get_base_path()
        / _sanitise_segment(scope.dag_id)
        / _sanitise_segment(scope.run_id)
        / _sanitise_segment(scope.task_id)
        / str(scope.map_index)
        / f"{_sanitise_segment(key)}{suffix}"
    )


def _build_asset_path(scope: AssetScope, key: str) -> ObjectStoragePath:
    suffix = _get_compression_suffix()
    asset_identifier = _sanitise_segment(scope.name or scope.uri or str(scope.asset_id))
    return _get_base_path() / "assets" / asset_identifier / f"{_sanitise_segment(key)}{suffix}"


def _write_to_object_storage(path: ObjectStoragePath, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    compression = _get_compression()
    with path.open(mode="wb", compression=compression) as f:
        f.write(value.encode("utf-8"))


def _read_from_object_storage(path: ObjectStoragePath) -> str | None:
    try:
        with path.open(mode="rb", compression="infer") as f:
            return f.read().decode("utf-8")
    except FileNotFoundError:
        return None


def _is_storage_ref(value: str) -> bool:
    try:
        if not urlsplit(value).scheme:
            return False
        return ObjectStoragePath(value).is_relative_to(_get_base_path())
    except Exception:
        return False


def _scope_path(scope: StoreScope, key: str) -> ObjectStoragePath:
    match scope:
        case TaskScope():
            return _build_task_path(scope, key)
        case AssetScope():
            return _build_asset_path(scope, key)
        case _:
            raise TypeError(f"Unknown scope type: {type(scope)}")


class StateStoreObjectStorageBackend(BaseStoreBackend):
    """
    Object-storage backend for task and asset store.

    Config keys (all under ``[common.io]``):

    - ``state_store_objectstorage_path``: base path, e.g. ``s3://conn_id@bucket/task-state/``
    - ``state_store_objectstorage_compression``: optional compression, e.g. ``gzip``
    """

    def get(self, scope: StoreScope, key: str, *, session: Session | None = None) -> str | None:
        return _read_from_object_storage(_scope_path(scope, key))

    def set(
        self,
        scope: StoreScope,
        key: str,
        value: str,
        *,
        expires_at: datetime | None = None,
        session: Session | None = None,
    ) -> None:
        _write_to_object_storage(_scope_path(scope, key), value)

    def delete(self, scope: StoreScope, key: str, *, session: Session | None = None) -> None:
        _scope_path(scope, key).unlink(missing_ok=True)

    def clear(
        self, scope: StoreScope, *, all_map_indices: bool = False, session: Session | None = None
    ) -> None:
        match scope:
            case TaskScope():
                if all_map_indices:
                    prefix = (
                        _get_base_path()
                        / _sanitise_segment(scope.dag_id)
                        / _sanitise_segment(scope.run_id)
                        / _sanitise_segment(scope.task_id)
                    )
                    for p in prefix.glob("*/*"):
                        p.unlink(missing_ok=True)
                else:
                    prefix = (
                        _get_base_path()
                        / _sanitise_segment(scope.dag_id)
                        / _sanitise_segment(scope.run_id)
                        / _sanitise_segment(scope.task_id)
                        / str(scope.map_index)
                    )
                    for p in prefix.glob("*"):
                        p.unlink(missing_ok=True)
            case AssetScope():
                asset_identifier = _sanitise_segment(scope.name or scope.uri or str(scope.asset_id))
                prefix = _get_base_path() / "assets" / asset_identifier
                for p in prefix.glob("*"):
                    p.unlink(missing_ok=True)
            case _:
                raise TypeError(f"Unknown scope type: {type(scope)}")

    async def aget(self, scope: StoreScope, key: str, *, session: AsyncSession | None = None) -> str | None:
        raise NotImplementedError

    async def aset(
        self,
        scope: StoreScope,
        key: str,
        value: str,
        *,
        expires_at: datetime | None = None,
        session: AsyncSession | None = None,
    ) -> None:
        raise NotImplementedError

    async def adelete(self, scope: StoreScope, key: str, *, session: AsyncSession | None = None) -> None:
        raise NotImplementedError

    async def aclear(
        self, scope: StoreScope, *, all_map_indices: bool = False, session: AsyncSession | None = None
    ) -> None:
        raise NotImplementedError

    def serialize_task_state_store_to_ref(self, *, value: JsonValue, key: str, scope: TaskScope) -> str:
        serialized = json.dumps(value)
        if len(serialized.encode()) < _get_threshold():
            return serialized
        path = _build_task_path(scope, key)
        _write_to_object_storage(path, serialized)
        return str(path)

    def deserialize_task_state_store_from_ref(self, stored: str) -> JsonValue:
        if not stored:
            return None
        if _is_storage_ref(stored):
            data = _read_from_object_storage(ObjectStoragePath(stored))
            if data is not None:
                return json.loads(data)
            return None
        return json.loads(stored)

    def serialize_asset_state_store_to_ref(self, *, value: JsonValue, key: str, scope: AssetScope) -> str:
        serialized = json.dumps(value)
        if len(serialized.encode()) < _get_threshold():
            return serialized
        path = _build_asset_path(scope, key)
        _write_to_object_storage(path, serialized)
        return str(path)

    def deserialize_asset_state_store_from_ref(self, stored: str) -> JsonValue:
        if not stored:
            return None
        if _is_storage_ref(stored):
            data = _read_from_object_storage(ObjectStoragePath(stored))
            if data is not None:
                return json.loads(data)
            return None
        return json.loads(stored)
