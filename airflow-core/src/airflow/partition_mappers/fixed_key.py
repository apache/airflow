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

import attrs

from airflow.partition_mappers.base import PartitionMapper


@attrs.define
class FixedKeyMapper(PartitionMapper):
    """
    Collapse every upstream partition key onto one fixed downstream key.

    Returns the same *downstream_key* for any upstream key passed to
    ``to_downstream``. Does not override ``decode_downstream`` or
    ``encode_upstream``, so it works with the string-based identity path and
    satisfies :class:`~airflow.partition_mappers.base.RollupMapper`'s guard
    when paired with :class:`~airflow.partition_mappers.window.SegmentWindow`.

    Typical use is as the ``upstream_mapper`` inside a categorical rollup::

        RollupMapper(
            upstream_mapper=FixedKeyMapper("all_regions"),
            window=SegmentWindow(["us", "eu", "apac"]),
        )

    :param downstream_key: The fixed downstream partition key every upstream key
        maps to. Must be a non-empty string.
    :raises ValueError: if *downstream_key* is not a non-empty ``str``.
    """

    downstream_key: str = attrs.field()

    @downstream_key.validator
    def _validate_downstream_key(self, attribute: attrs.Attribute, value: str) -> None:
        if not isinstance(value, str) or value == "":
            raise ValueError(f"FixedKeyMapper downstream_key must be a non-empty str; got {value!r}.")

    def to_downstream(self, key: str) -> str:
        """Return the fixed downstream key regardless of *key*."""
        return self.downstream_key

    def serialize(self) -> dict[str, Any]:
        return {"downstream_key": self.downstream_key}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> FixedKeyMapper:
        return cls(data["downstream_key"])
