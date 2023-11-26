#
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

from typing import TYPE_CHECKING

from airflow.utils.module_loading import qualname

serializers = ["deltalake.table.DeltaTable"]
deserializers = serializers
stringifiers = serializers

if TYPE_CHECKING:
    from airflow.serialization.serde import U

__version__ = 2


def serialize(o: object) -> tuple[U, str, int, bool]:
    from deltalake.table import DeltaTable

    from airflow.serialization.serde import encrypt

    if not isinstance(o, DeltaTable):
        return "", "", 0, False

    # we encrypt the information here until we have as part of the
    # storage options can have sensitive information
    properties: dict = {}
    for k, v in o._storage_options.items() if o._storage_options else {}:
        properties[k] = encrypt(v)

    data = {
        "table_uri": o.table_uri,
        "version": o.version(),
        "storage_options": properties,
    }

    return data, qualname(o), __version__, True


def deserialize(classname: str, version: int, data: dict):
    from deltalake.table import DeltaTable

    from airflow.models.crypto import get_fernet
    from airflow.serialization.serde import decrypt

    if version > __version__:
        raise TypeError("serialized version is newer than class version")

    if classname == qualname(DeltaTable):
        fernet = get_fernet()
        properties = {}
        for k, v in data["storage_options"].items():
            if version == 1:
                properties[k] = fernet.decrypt(v.encode("utf-8")).decode("utf-8")
            else:
                properties[k] = decrypt(v)

        if len(properties) == 0:
            storage_options = None
        else:
            storage_options = properties

        return DeltaTable(data["table_uri"], version=data["version"], storage_options=storage_options)

    raise TypeError(f"do not know how to deserialize {classname}")
