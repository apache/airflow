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

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from airflow.partition_mappers.base import PartitionMapper

if TYPE_CHECKING:
    from datetime import datetime


class ChainMapper(PartitionMapper):
    """Partition mapper that applies multiple mappers sequentially."""

    def __init__(
        self,
        mapper0: PartitionMapper,
        mapper1: PartitionMapper,
        /,
        *mappers: PartitionMapper,
        max_downstream_keys: int | None = None,
    ) -> None:
        super().__init__(max_downstream_keys=max_downstream_keys)
        self.mappers = [mapper0, mapper1, *mappers]

    def to_downstream(self, key: str) -> str | Iterable[str]:
        keys: list[str] = [key]
        for mapper in self.mappers:
            next_keys: list[str] = []
            for current_key in keys:
                mapped = mapper.to_downstream(current_key)
                if not isinstance(mapped, (str, Iterable)):
                    raise TypeError(
                        f"ChainMapper child mappers must return a string or iterable of strings, "
                        f"but {type(mapper).__name__} returned {type(mapped).__name__}"
                    )

                if isinstance(mapped, str):
                    next_keys.append(mapped)
                elif isinstance(mapped, Iterable):
                    for mapped_key in mapped:
                        if not isinstance(mapped_key, str):
                            raise TypeError(
                                f"ChainMapper child mappers must return an iterable of strings, "
                                f"but {type(mapper).__name__} yielded {type(mapped_key).__name__}"
                            )
                        next_keys.append(mapped_key)
            keys = next_keys
        return keys[0] if len(keys) == 1 else keys

    def to_partition_date(self, downstream_key: str) -> datetime | None:
        # The last mapper in the chain formats the final downstream key, so it owns the anchor.
        return self.mappers[-1].to_partition_date(downstream_key)

    def serialize(self) -> dict[str, Any]:
        from airflow.serialization.encoders import encode_partition_mapper

        result: dict[str, Any] = {"mappers": [encode_partition_mapper(m) for m in self.mappers]}
        if self.max_downstream_keys is not None:
            result["max_downstream_keys"] = self.max_downstream_keys
        return result

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> PartitionMapper:
        from airflow.serialization.decoders import decode_partition_mapper

        mappers = [decode_partition_mapper(m) for m in data["mappers"]]
        return cls(*mappers, max_downstream_keys=data.get("max_downstream_keys"))
