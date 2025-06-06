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

import sys
from enum import Enum, IntEnum, IntFlag

if sys.version_info >= (3, 11):
    from enum import StrEnum

from typing import TYPE_CHECKING, cast

from airflow.utils.module_loading import qualname

if TYPE_CHECKING:
    from airflow.serialization.serde import U

__version__ = 1

serializers = ["enum.IntEnum", "enum.StrEnum", "enum.IntFlag"]
deserializers = serializers
stringifiers = serializers


def serialize(o: object) -> tuple[U, str, int, bool]:
    return cast("Enum", o).value, qualname(o), __version__, True


def deserialize(classname: str, version: int, data: list) -> Enum:
    if version > __version__:
        raise TypeError("serialized version is newer than class version")

    if classname == qualname(IntEnum):
        return IntEnum(data)

    if classname == qualname(IntFlag):
        return IntFlag(data)

    if sys.version_info >= (3, 11) and classname == qualname(StrEnum):
        return StrEnum(data)

    raise TypeError(f"do not know how to deserialize {classname}")


def stringify(classname: str, version: int, data: Enum) -> str:
    if classname not in stringifiers:
        raise TypeError(f"do not know how to stringify {classname}")

    return f"{data.value}"
