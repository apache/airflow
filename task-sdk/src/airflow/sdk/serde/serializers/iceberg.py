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

from airflow.sdk.module_loading import qualname

serializers = ["pyiceberg.table.Table"]
deserializers = serializers
stringifiers = serializers

if TYPE_CHECKING:
    from airflow.sdk.serde import U

__version__ = 1


def serialize(o: object) -> tuple[U, str, int, bool]:
    from pyiceberg.table import Table

    if not isinstance(o, Table):
        return "", "", 0, False

    from airflow.models.crypto import get_fernet

    # we encrypt the catalog information here until we have
    # global catalog management in airflow and the properties
    # can have sensitive information
    fernet = get_fernet()
    properties = {}
    for k, v in o.catalog.properties.items():
        properties[k] = fernet.encrypt(v.encode("utf-8")).decode("utf-8")

    data = {
        "identifier": o._identifier,
        "catalog_properties": properties,
    }

    return data, qualname(o), __version__, True


def deserialize(cls: type, version: int, data: dict):
    from pyiceberg.catalog import load_catalog
    from pyiceberg.table import Table

    from airflow.models.crypto import get_fernet

    if version > __version__:
        raise TypeError("serialized version is newer than class version")

    if cls is Table:
        fernet = get_fernet()
        properties = {}
        for k, v in data["catalog_properties"].items():
            properties[k] = fernet.decrypt(v.encode("utf-8")).decode("utf-8")

        catalog = load_catalog(data["identifier"][0], **properties)
        return catalog.load_table((data["identifier"][1], data["identifier"][2]))

    raise TypeError(f"do not know how to deserialize {qualname(cls)}")
