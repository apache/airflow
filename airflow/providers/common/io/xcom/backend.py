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
import uuid
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


def _is_relative_to(o: ObjectStoragePath, other: ObjectStoragePath) -> bool:
    """This is a port of the pathlib.Path.is_relative_to method. It is not available in python 3.8."""
    if hasattr(o, "is_relative_to"):
        return o.is_relative_to(other)

    try:
        o.relative_to(other)
        return True
    except ValueError:
        return False


def _get_compression_suffix(compression: str) -> str:
    """This returns the compression suffix for the given compression.

    :raises ValueError: if the compression is not supported
    """
    for suffix, c in fsspec.utils.compressions.items():
        if c == compression:
            return suffix

    raise ValueError(f"Compression {compression} is not supported. Make sure it is installed.")


class XComObjectStoreBackend(BaseXCom):
    """XCom backend that stores data in an object store or database depending on the size of the data.

    If the value is larger than the configured threshold, it will be stored in an object store.
    Otherwise, it will be stored in the database. If it is stored in an object store, the path
    to the object in the store will be returned and saved in the database (by BaseXCom). Otherwise, the value
    itself will be returned and thus saved in the database.
    """

    @staticmethod
    def _get_key(data: str) -> str:
        """This gets the key from the url and normalizes it to be relative to the configured path.

        :raises ValueError: if the key is not relative to the configured path
        :raises TypeError: if the url is not a valid url or cannot be split
        """
        path = conf.get(SECTION, "xcom_objectstore_path", fallback="")
        p = ObjectStoragePath(path)

        try:
            url = urlsplit(data)
        except AttributeError:
            raise TypeError(f"Not a valid url: {data}")

        if url.scheme:
            k = ObjectStoragePath(data)

            if _is_relative_to(k, p) is False:
                raise ValueError(f"Invalid key: {data}")
            else:
                return data.replace(path, "", 1).lstrip("/")

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
        path = conf.get(SECTION, "xcom_objectstore_path", fallback="")
        compression = conf.get(SECTION, "xcom_objectstore_compression", fallback=None)

        if compression:
            suffix = "." + _get_compression_suffix(compression)
        else:
            suffix = ""

        threshold = conf.getint(SECTION, "xcom_objectstore_threshold", fallback=-1)

        if path and -1 < threshold < len(s_val):
            # safeguard against collisions
            while True:
                p = ObjectStoragePath(path) / f"{dag_id}/{run_id}/{task_id}/{str(uuid.uuid4())}{suffix}"
                if not p.exists():
                    break

            if not p.parent.exists():
                p.parent.mkdir(parents=True, exist_ok=True)

            with p.open("wb", compression=compression) as f:
                f.write(s_val)

            return BaseXCom.serialize_value(str(p))
        else:
            return s_val

    @staticmethod
    def deserialize_value(
        result: XCom,
    ) -> Any:
        """Deserializes the value from the database or object storage.

        Compression is inferred from the file extension.
        """
        data = BaseXCom.deserialize_value(result)
        path = conf.get(SECTION, "xcom_objectstore_path", fallback="")

        try:
            p = ObjectStoragePath(path) / XComObjectStoreBackend._get_key(data)
            return json.load(p.open("rb", compression="infer"), cls=XComDecoder)
        except TypeError:
            return data
        except ValueError:
            return data

    @staticmethod
    def purge(xcom: XCom, session: Session) -> None:
        path = conf.get(SECTION, "xcom_objectstore_path", fallback="")
        if isinstance(xcom.value, str):
            try:
                p = ObjectStoragePath(path) / XComObjectStoreBackend._get_key(xcom.value)
                p.unlink(missing_ok=True)
            except TypeError:
                pass
            except ValueError:
                pass
