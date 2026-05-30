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

from typing import ClassVar

from airflow.sdk.definitions.partition_mappers.base import PartitionMapper


class ConstantMapper(PartitionMapper):
    """
    Collapse every upstream partition key onto a single downstream partition key.

    Pair with an anchor-independent window (``SegmentWindow`` or
    ``DynamicSegmentWindow``) inside a ``RollupMapper`` so that all upstream
    segment events accumulate into one downstream partition. Because segment
    windows expect the same set of upstream keys for every downstream key, the
    upstream events only fill that set if they all land in the same partition —
    which is exactly what this mapper guarantees::

        from airflow.sdk import RollupMapper, ConstantMapper, SegmentWindow

        mapper = RollupMapper(
            upstream_mapper=ConstantMapper("all_regions"),
            window=SegmentWindow(["us", "eu", "apac"]),
        )

    The fired downstream Dag run's ``partition_key`` is *downstream_key*.

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
