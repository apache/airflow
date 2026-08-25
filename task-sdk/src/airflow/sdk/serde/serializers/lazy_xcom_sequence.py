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
    from airflow.sdk.serde import U

__version__ = 1

serializers = ["airflow.sdk.execution_time.lazy_sequence.LazyXComSequence"]
deserializers = serializers


def serialize(o: object) -> tuple[U, str, int, bool]:
    """Materialize a lazily-resolved mapped XCom to a list (single slice fetch)."""
    from airflow.sdk.execution_time.lazy_sequence import LazyXComSequence

    if isinstance(o, LazyXComSequence):
        return list(o[:]), qualname(o), __version__, True
    return "", "", 0, False


def deserialize(cls: type, version: int, data: list) -> list:
    """Deserialize to a plain list; the lazy sequence cannot be rebuilt off-worker."""
    if version > __version__:
        raise TypeError(f"serialized version {version} is newer than class version {__version__}")
    return list(data)
