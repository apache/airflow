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


class ProductMapper(PartitionMapper):
    """Partition mapper that combines multiple mappers into a multi-dimensional key."""

    DELIMITER = "|"

    def __init__(self, mappers: list[PartitionMapper]) -> None:
        if len(mappers) < 2:
            raise ValueError("ProductMapper requires at least 2 child mappers")
        self.mappers = mappers

    def to_downstream(self, key: str) -> str:
        segments = key.split(self.DELIMITER)
        if len(segments) != len(self.mappers):
            raise ValueError(f"Expected {len(self.mappers)} segments in key, got {len(segments)}")
        return self.DELIMITER.join(
            mapper.to_downstream(segment) for mapper, segment in zip(self.mappers, segments)
        )

    def serialize(self) -> dict[str, Any]:
        from airflow.serialization.encoders import encode_partition_mapper

        return {"mappers": [encode_partition_mapper(m) for m in self.mappers]}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> PartitionMapper:
        from airflow.serialization.decoders import decode_partition_mapper

        return cls(mappers=[decode_partition_mapper(m) for m in data["mappers"]])
