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


class SequenceMapper(PartitionMapper):
    """Partition mapper that validates keys against a defined sequence."""

    def __init__(self, sequence: list[str]) -> None:
        self.sequence = sequence
        self._valid_keys = frozenset(sequence)

    def to_downstream(self, key: str) -> str:
        if key not in self._valid_keys:
            raise ValueError(f"Key {key!r} not in sequence {self.sequence}")
        return key

    def serialize(self) -> dict[str, Any]:
        return {"sequence": self.sequence}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> PartitionMapper:
        return cls(sequence=data["sequence"])
