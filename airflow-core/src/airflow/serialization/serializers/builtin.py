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

from typing import TYPE_CHECKING, cast

from airflow._shared.module_loading import qualname

if TYPE_CHECKING:
    from airflow.serialization.serde import U

__version__ = 1

serializers = ["builtins.frozenset", "builtins.set", "builtins.tuple"]
deserializers = serializers
stringifiers = serializers


def serialize(o: object) -> tuple[U, str, int, bool]:
    return list(cast("list", o)), qualname(o), __version__, True


def deserialize(cls: type, version: int, data: list) -> tuple | set | frozenset:
    if version > __version__:
        raise TypeError(f"serialized version {version} is newer than class version {__version__}")

    if cls is tuple:
        return tuple(data)

    if cls is set:
        return set(data)

    if cls is frozenset:
        return frozenset(data)

    raise TypeError(f"do not know how to deserialize {qualname(cls)}")


def stringify(classname: str, version: int, data: list) -> str:
    if classname not in stringifiers:
        raise TypeError(f"do not know how to stringify {classname}")

    s = ",".join(str(d) for d in data)
    return f"({s})"
