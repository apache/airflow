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

from airflow.configuration import conf
from airflow.io.path import ObjectStoragePath
from airflow.models.xcom import BaseXCom
from airflow.utils.json import XComDecoder, XComEncoder

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models import XCom

T = TypeVar("T")


class XComObjectStoreBackend(BaseXCom):
    """XCom backend that stores data in an object store."""

    path = conf.get("core", "xcom_objectstore_path", fallback="")

    @staticmethod
    def _get_key(data: str) -> str:
        path = conf.get("core", "xcom_objectstore_path", fallback="")
        p = ObjectStoragePath(path)

        url = urlsplit(data)
        if url.scheme:
            k = ObjectStoragePath(data)

            if k.is_relative_to(p) is False:
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
        s_val = json.dumps(value, cls=XComEncoder).encode("utf-8")
        path = conf.get("core", "xcom_objectstore_path", fallback="")
        compression = conf.get("core", "xcom_objectstore_compression", fallback=None)
        threshold = conf.getint("core", "xcom_objectstore_threshold", fallback=-1)

        if path and -1 < threshold < len(s_val):
            # safeguard against collisions
            while True:
                p = ObjectStoragePath(path) / f"{run_id}/{task_id}/{str(uuid.uuid4())}"
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
        data = BaseXCom.deserialize_value(result)
        path = conf.get("core", "xcom_objectstore_path", fallback="")
        try:
            p = ObjectStoragePath(path) / XComObjectStoreBackend._get_key(data)
            return json.load(p.open("rb"), cls=XComDecoder)
        except TypeError:
            return data
        except ValueError:
            return data

    @staticmethod
    def purge(xcom: XCom, session: Session) -> None:
        path = conf.get("core", "xcom_objectstore_path", fallback="")
        if isinstance(xcom.value, str):
            try:
                p = ObjectStoragePath(path) / XComObjectStoreBackend._get_key(xcom.value)
                p.unlink(missing_ok=True)
            except TypeError:
                pass
            except ValueError:
                pass
