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

from airflow.partition_mappers.base import PartitionMapper
from airflow.plugins_manager import AirflowPlugin

if TYPE_CHECKING:
    from typing import Any


# [START custom_partition_mapper]
class PrefixStripMapper(PartitionMapper):
    """
    A partition mapper that strips a fixed namespace prefix from upstream keys.

    Upstream systems often qualify partition keys with a region or environment
    prefix — for example ``"eu::daily-sales"`` or ``"us::daily-sales"``.  A
    downstream asset that aggregates across regions only cares about the base key
    (``"daily-sales"``).  ``PrefixStripMapper`` strips the given prefix (including
    a configurable separator) so that all upstream namespaces collapse to the
    same downstream partition key.

    If the upstream key does not start with the configured prefix the key is
    returned unchanged, which is deliberate: keys that already live in the target
    namespace pass through without modification.

    This class demonstrates registering a custom :class:`PartitionMapper
    <airflow.partition_mappers.base.PartitionMapper>` subclass via the
    ``AirflowPlugin.partition_mappers`` registry. Any plugin that lists it in
    ``partition_mappers = [...]`` makes it available to
    :class:`~airflow.sdk.PartitionedAssetTimetable` and
    :class:`~airflow.partition_mappers.base.RollupMapper` without modifying core
    Airflow.

    :param prefix: The namespace prefix to strip, e.g. ``"eu"``.
    :param separator: The string that separates the prefix from the base key.
        Defaults to ``"::"`` to match a common ``"region::key"`` convention.
    """

    def __init__(
        self,
        prefix: str,
        *,
        separator: str = "::",
        max_downstream_keys: int | None = None,
    ) -> None:
        super().__init__(max_downstream_keys=max_downstream_keys)
        if not prefix:
            raise ValueError("prefix must be a non-empty string.")
        self.prefix = prefix
        self.separator = separator

    def to_downstream(self, key: str) -> str:
        full_prefix = self.prefix + self.separator
        if key.startswith(full_prefix):
            return key[len(full_prefix) :]
        return key

    def serialize(self) -> dict[str, Any]:
        data: dict[str, Any] = {"prefix": self.prefix, "separator": self.separator}
        if self.max_downstream_keys is not None:
            data["max_downstream_keys"] = self.max_downstream_keys
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> PartitionMapper:
        return cls(
            prefix=data["prefix"],
            separator=data.get("separator", "::"),
            max_downstream_keys=data.get("max_downstream_keys"),
        )


class PrefixStripMapperPlugin(AirflowPlugin):
    name = "prefix_strip_mapper_plugin"
    partition_mappers = [PrefixStripMapper]


# [END custom_partition_mapper]
