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

import contextlib
import json
from functools import cache
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import fsspec.utils

from airflow.providers.common.compat.sdk import conf

if TYPE_CHECKING:
    from datetime import datetime

    from pydantic import JsonValue
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.orm import Session

try:
    from airflow.sdk._shared.state import AssetScope, BaseStoreBackend, StoreScope, TaskScope
except ImportError:
    raise ImportError(
        "StoreObjectStorageBackend requires Airflow >= 3.3. Please upgrade your Airflow installation."
    ) from None

from airflow.sdk import ObjectStoragePath

SECTION = "common.io"


@cache
def _get_base_path() -> ObjectStoragePath:
    return ObjectStoragePath(conf.get_mandatory_value(SECTION, "store_objectstorage_path"))


@cache
def _get_compression() -> str | None:
    value = conf.get(SECTION, "store_objectstorage_compression", fallback=None)
    return value or None


@cache
def _get_threshold() -> int:
    return conf.getint(SECTION, "store_objectstorage_threshold", fallback=0)


def _compression_suffix() -> str:
    compression = _get_compression()
    if not compression:
        return ""
    for suffix, c in fsspec.utils.compressions.items():
        if c == compression:
            return f".{suffix}"
    raise ValueError(f"Compression {compression!r} is not supported.")


def _safe_segment(value: str) -> str:
    """
    Sanitise a string for use as a single path segment.

    This is a simple implementation that replaces slashes with underscores.
    """
    return value.replace("/", "_").replace("\\", "_")


def _task_path(scope: TaskScope, key: str) -> ObjectStoragePath:
    suffix = _compression_suffix()
    return (
        _get_base_path()
        / _safe_segment(scope.dag_id)
        / _safe_segment(scope.run_id)
        / _safe_segment(scope.task_id)
        / str(scope.map_index)
        / f"{_safe_segment(key)}{suffix}"
    )


def _asset_path(scope: AssetScope, key: str) -> ObjectStoragePath:
    suffix = _compression_suffix()
    asset_ref = _safe_segment(scope.name or scope.uri or str(scope.asset_id))
    return _get_base_path() / "assets" / asset_ref / f"{_safe_segment(key)}{suffix}"


def _write(path: ObjectStoragePath, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    compression = _get_compression()
    with path.open(mode="wb", compression=compression) as f:
        f.write(value.encode("utf-8"))


def _read(path: ObjectStoragePath) -> str | None:
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
            return _task_path(scope, key)
        case AssetScope():
            return _asset_path(scope, key)
        case _:
            raise TypeError(f"Unknown scope type: {type(scope)}")


class StoreObjectStorageBackend(BaseStoreBackend):
    """
    Object-storage backend for task and asset store.

    Config keys (all under ``[common.io]``):

    - ``store_objectstorage_path``: base path, e.g. ``s3://conn_id@bucket/task-state/``
    - ``store_objectstorage_compression``: optional compression, e.g. ``gzip``
    """

    def get(self, scope: StoreScope, key: str, *, session: Session | None = None) -> str | None:
        return _read(_scope_path(scope, key))

    def set(
        self,
        scope: StoreScope,
        key: str,
        value: str,
        *,
        expires_at: datetime | None = None,
        session: Session | None = None,
    ) -> None:
        _write(_scope_path(scope, key), value)

    def delete(self, scope: StoreScope, key: str, *, session: Session | None = None) -> None:
        path = _scope_path(scope, key)
        with contextlib.suppress(FileNotFoundError):
            path.unlink(missing_ok=True)

    def clear(
        self, scope: StoreScope, *, all_map_indices: bool = False, session: Session | None = None
    ) -> None:
        match scope:
            case TaskScope():
                if all_map_indices:
                    prefix = (
                        _get_base_path()
                        / _safe_segment(scope.dag_id)
                        / _safe_segment(scope.run_id)
                        / _safe_segment(scope.task_id)
                    )
                    for p in prefix.glob("*/*"):
                        p.unlink(missing_ok=True)
                else:
                    prefix = (
                        _get_base_path()
                        / _safe_segment(scope.dag_id)
                        / _safe_segment(scope.run_id)
                        / _safe_segment(scope.task_id)
                        / str(scope.map_index)
                    )
                    for p in prefix.glob("*"):
                        p.unlink(missing_ok=True)
            case AssetScope():
                asset_ref = _safe_segment(scope.name or scope.uri or str(scope.asset_id))
                prefix = _get_base_path() / "assets" / asset_ref
                for p in prefix.glob("*"):
                    p.unlink(missing_ok=True)

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

    def serialize_task_store_to_ref(self, *, value: JsonValue, key: str, scope: TaskScope) -> str:
        serialized = json.dumps(value)
        if len(serialized.encode()) < _get_threshold():
            return serialized
        path = _task_path(scope, key)
        _write(path, serialized)
        return str(path)

    def deserialize_task_store_from_ref(self, stored: str) -> JsonValue:
        if not stored:
            return None
        if _is_storage_ref(stored):
            data = _read(ObjectStoragePath(stored))
            if data is not None:
                return json.loads(data)
            return None
        return json.loads(stored)

    def serialize_asset_store_to_ref(self, *, value: JsonValue, key: str, scope: AssetScope) -> str:
        serialized = json.dumps(value)
        if len(serialized.encode()) < _get_threshold():
            return serialized
        path = _asset_path(scope, key)
        _write(path, serialized)
        return str(path)

    def deserialize_asset_store_from_ref(self, stored: str) -> JsonValue:
        if not stored:
            return None
        if _is_storage_ref(stored):
            data = _read(ObjectStoragePath(stored))
            if data is not None:
                return json.loads(data)
            return None
        return json.loads(stored)
