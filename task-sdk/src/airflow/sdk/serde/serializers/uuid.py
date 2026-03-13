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

if TYPE_CHECKING:
    import uuid

    from airflow.sdk.serde import U

__version__ = 1

serializers = ["uuid.UUID"]
deserializers = serializers


def serialize(o: object) -> tuple[U, str, int, bool]:
    """Serialize a UUID object to a string representation."""
    import uuid

    if isinstance(o, uuid.UUID):
        return str(o), qualname(o), __version__, True
    return "", "", 0, False


def deserialize(cls: type, version: int, data: str) -> uuid.UUID:
    """Deserialize a string back to a UUID object."""
    import uuid

    if cls is uuid.UUID and isinstance(data, str):
        return uuid.UUID(data)
    raise TypeError(f"cannot deserialize {qualname(cls)} from {type(data)}")
