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

from typing import Any

from airflow.partition_mappers.base import PartitionMapper


class AllowedKeyMapper(PartitionMapper):
    """Partition mapper that validates keys against a set of allowed keys."""

    def __init__(self, allowed_keys: list[str]) -> None:
        self.allowed_keys = allowed_keys

    def to_downstream(self, key: str) -> str:
        if key not in self.allowed_keys:
            raise ValueError(f"Key {key!r} not in allowed keys {self.allowed_keys}")
        return key

    def serialize(self) -> dict[str, Any]:
        return {"allowed_keys": self.allowed_keys}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> PartitionMapper:
        return cls(allowed_keys=data["allowed_keys"])
