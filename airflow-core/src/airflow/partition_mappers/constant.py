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

from typing import Any, ClassVar

from airflow.partition_mappers.base import PartitionMapper


class ConstantMapper(PartitionMapper):
    """
    Collapse every upstream partition key onto a single downstream partition key.

    Pair with an anchor-independent window (``SegmentWindow`` or
    ``DynamicSegmentWindow``) inside a ``RollupMapper`` so that all upstream
    segment events accumulate into one downstream partition. Segment windows
    expect the same set of upstream keys for every downstream key, so those keys
    only fill the set if they all land in the same partition — which is exactly
    what this mapper guarantees.

    ``encode_upstream`` / ``decode_downstream`` use the base identity behaviour:
    the segment window iterates plain ``str`` keys and the constant downstream
    key is only handed to the window as an anchor it ignores.

    :param downstream_key: The single non-empty downstream partition key that
        every upstream key maps to.
    :raises ValueError: if *downstream_key* is not a non-empty ``str``.
    """

    collapses_to_constant: ClassVar[bool] = True

    def __init__(self, downstream_key: str) -> None:
        if not isinstance(downstream_key, str) or downstream_key == "":
            raise ValueError(
                f"ConstantMapper downstream_key must be a non-empty str; got {downstream_key!r}."
            )
        self.downstream_key = downstream_key

    def to_downstream(self, key: str) -> str:
        return self.downstream_key

    def serialize(self) -> dict[str, Any]:
        return {"downstream_key": self.downstream_key}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> ConstantMapper:
        return cls(data["downstream_key"])
