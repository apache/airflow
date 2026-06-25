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

import attrs

from airflow.sdk.definitions.partition_mappers.base import PartitionMapper


@attrs.define
class FixedKeyMapper(PartitionMapper):
    """
    Collapse every upstream partition key onto one fixed downstream key.

    Authoring marker for the scheduler-side
    :class:`airflow.partition_mappers.fixed_key.FixedKeyMapper`. Paired with
    :class:`~airflow.sdk.definitions.partition_mappers.window.SegmentWindow` inside a
    :class:`~airflow.sdk.definitions.partition_mappers.base.RollupMapper` to express a
    categorical rollup.

    Construction validates *downstream_key* so Dag parse errors surface
    immediately rather than deferring to scheduler deserialization.

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
