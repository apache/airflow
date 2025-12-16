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

from airflow._shared.module_loading import qualname
from airflow.serialization.typing import is_pydantic_model

if TYPE_CHECKING:
    from airflow.serialization.serde import U

serializers = [
    "pydantic.main.BaseModel",
]
deserializers = serializers

__version__ = 1


def serialize(o: object) -> tuple[U, str, int, bool]:
    """
    Serialize a Pydantic BaseModel instance into a dict of built-in types.

    Returns a tuple of:
    - serialized data (as built-in types)
    - fixed class name for registration (BaseModel)
    - version number
    - is_serialized flag (True if handled)
    """
    if not is_pydantic_model(o):
        return "", "", 0, False

    data = o.model_dump(mode="json")  # type: ignore

    return data, qualname(o), __version__, True


def deserialize(cls: type, version: int, data: dict):
    """
    Deserialize a Pydantic class.

    Pydantic models can be serialized into a Python dictionary via `pydantic.main.BaseModel.model_dump`
    and the dictionary can be deserialized through `pydantic.main.BaseModel.model_validate`. This function
    can deserialize arbitrary Pydantic models that are in `allowed_deserialization_classes`.

    :param cls: The actual model class
    :param version: Serialization version (must not exceed __version__)
    :param data: Dictionary with built-in types, typically from model_dump()
    :return: An instance of the actual Pydantic model
    """
    if version > __version__:
        raise TypeError(f"Serialized version {version} is newer than the supported version {__version__}")

    if not is_pydantic_model(cls):
        # no deserializer available
        raise TypeError(f"No deserializer found for {qualname(cls)}")

    # Perform validation-based reconstruction
    return cls.model_validate(data)  # type: ignore
