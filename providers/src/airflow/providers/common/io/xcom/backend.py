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
import uuid
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar
from urllib.parse import urlsplit

import fsspec.utils

from airflow.configuration import conf
from airflow.io.path import ObjectStoragePath
from airflow.models.xcom import BaseXCom
from airflow.utils.json import XComDecoder, XComEncoder

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models import XCom

T = TypeVar("T")

SECTION = "common.io"


def _get_compression_suffix(compression: str) -> str:
    """
    Return the compression suffix for the given compression.

    :raises ValueError: if the compression is not supported
    """
    for suffix, c in fsspec.utils.compressions.items():
        if c == compression:
            return suffix

    raise ValueError(f"Compression {compression} is not supported. Make sure it is installed.")


@cache
def _get_base_path() -> ObjectStoragePath:
    return ObjectStoragePath(conf.get_mandatory_value(SECTION, "xcom_objectstorage_path"))


@cache
def _get_compression() -> str | None:
    return conf.get(SECTION, "xcom_objectstorage_compression", fallback=None) or None


@cache
def _get_threshold() -> int:
    return conf.getint(SECTION, "xcom_objectstorage_threshold", fallback=-1)


class XComObjectStorageBackend(BaseXCom):
    """
    XCom backend that stores data in an object store or database depending on the size of the data.

    If the value is larger than the configured threshold, it will be stored in an object store.
    Otherwise, it will be stored in the database. If it is stored in an object store, the path
    to the object in the store will be returned and saved in the database (by BaseXCom). Otherwise, the value
    itself will be returned and thus saved in the database.
    """

    @staticmethod
    def _get_full_path(data: str) -> ObjectStoragePath:
        """
        Get the path from stored value.

        :raises ValueError: if the key is not relative to the configured path
        :raises TypeError: if the url is not a valid url or cannot be split
        """
        p = _get_base_path()

        # normalize the path
        try:
            url = urlsplit(data)
        except AttributeError:
            raise TypeError(f"Not a valid url: {data}") from None

        if url.scheme:
            if not Path.is_relative_to(ObjectStoragePath(data), p):
                raise ValueError(f"Invalid key: {data}")
            return p / data.replace(str(p), "", 1).lstrip("/")

        raise ValueError(f"Not a valid url: {data}")

    @staticmethod
    def serialize_value(
        value: T,
        *,
        key: str | None = None,
        task_id: str | None = None,
        dag_id: str | None = None,
        run_id: str | None = None,
        map_index: int | None = None,
    ) -> bytes | str:
        # we will always serialize ourselves and not by BaseXCom as the deserialize method
        # from BaseXCom accepts only XCom objects and not the value directly
        s_val = json.dumps(value, cls=XComEncoder).encode("utf-8")

        if compression := _get_compression():
            suffix = f".{_get_compression_suffix(compression)}"
        else:
            suffix = ""

        threshold = _get_threshold()
        if threshold < 0 or len(s_val) < threshold:  # Either no threshold or value is small enough.
            return s_val

        base_path = _get_base_path()
        while True:  # Safeguard against collisions.
            p = base_path.joinpath(
                dag_id or "NO_DAG_ID",
                run_id or "NO_RUN_ID",
                task_id or "NO_TASK_ID",
                f"{uuid.uuid4()}{suffix}",
            )
            if not p.exists():
                break
        p.parent.mkdir(parents=True, exist_ok=True)

        with p.open(mode="wb", compression=compression) as f:
            f.write(s_val)
        return BaseXCom.serialize_value(str(p))

    @staticmethod
    def deserialize_value(result: XCom) -> Any:
        """
        Deserializes the value from the database or object storage.

        Compression is inferred from the file extension.
        """
        data = BaseXCom.deserialize_value(result)
        try:
            path = XComObjectStorageBackend._get_full_path(data)
        except (TypeError, ValueError):  # Likely value stored directly in the database.
            return data
        try:
            with path.open(mode="rb", compression="infer") as f:
                return json.load(f, cls=XComDecoder)
        except (TypeError, ValueError):
            return data

    @staticmethod
    def purge(xcom: XCom, session: Session) -> None:
        if not isinstance(xcom.value, str):
            return
        with contextlib.suppress(TypeError, ValueError):
            XComObjectStorageBackend._get_full_path(xcom.value).unlink(missing_ok=True)
