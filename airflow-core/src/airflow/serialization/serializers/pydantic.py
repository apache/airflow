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

from importlib import import_module
from typing import TYPE_CHECKING, cast

from pydantic import BaseModel

from airflow.serialization.serde import _is_pydantic_basemodel
from airflow.utils.module_loading import qualname

if TYPE_CHECKING:
    from airflow.serialization.serde import U

serializers = [
    "pydantic.main.BaseModel",
]
deserializers = serializers
stringifiers = serializers

__version__ = 1


def _resolve_pydantic_class(qn: str):
    module_name, class_name = qn.rsplit(".", 1)
    module = import_module(module_name)
    return getattr(module, class_name)


def serialize(o: object) -> tuple[U, str, int, bool]:
    if not _is_pydantic_basemodel(o):
        return "", "", 0, False

    # to convince mypy
    m = cast("BaseModel", o)
    # Serialize
    data = m.model_dump()
    # Store the actual qualified name for the pydantic class. This classname will be used to import the module and load the data.
    data["__class__"] = qualname(o)

    return data, qualname(BaseModel), __version__, True


def deserialize(classname: str, version: int, data: dict):
    if version > __version__:
        raise TypeError(f"serialized {version} of {classname} > {__version__}")

    # check if it the qualified name is pydantic.main.BaseModel.
    if classname == qualname(BaseModel):
        # the actual qualified name for the pydantic.main.BaseModel subclass is stored in this key.
        if "__class__" not in data:
            raise TypeError("Missing '__class__' in serialized Pydantic.main.BaseModel data")

        qn = data.pop("__class__")
        cls = _resolve_pydantic_class(qn=qn)

        if not _is_pydantic_basemodel(cls):
            raise TypeError(f"{qn} is not a subclass of Pydantic.main.BaseModel")
        return cls.model_validate(data)

    # no deserializer available
    raise TypeError(f"No deserializer found for {classname}")
