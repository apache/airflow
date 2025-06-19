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

from typing import TYPE_CHECKING, Any, cast

from airflow.utils.module_loading import qualname

if TYPE_CHECKING:
    from pydantic import BaseModel

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
    if not _is_pydantic_model(o):
        return "", "", 0, False

    model = cast("BaseModel", o)  # for mypy
    data = model.model_dump()

    return data, qualname(o), __version__, True


def deserialize(cls: type, version: int, data: object):
    """
    Deserialize a dictionary into a Pydantic model instance.

    This function is used as a generic deserializer for all subclasses of BaseModel.
    It requires serde.py to fallback from the actual model class name to this handler.

    :param cls: The actual model class
    :param version: Serialization version (must not exceed __version__)
    :param data: Dictionary with built-in types, typically from model_dump()
    :return: An instance of the actual Pydantic model
    """
    if version > __version__:
        raise TypeError(f"Serialized version {version} is newer than the supported version {__version__}")
    print(cls)
    if not _is_pydantic_model(cls):
        # no deserializer available
        raise TypeError(f"No deserializer found for {qualname(cls)}")

    # Perform validation-based reconstruction
    model = cast("BaseModel", cls)  # for mypy
    return model.model_validate(data)


def _is_pydantic_model(cls: Any) -> bool:
    """Return True if the class is a pydantic.main.BaseModel or its subclasses."""
    return hasattr(cls, "__pydantic_fields__") and hasattr(cls, "__pydantic_validator__")
