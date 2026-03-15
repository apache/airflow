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

import attrs

from airflow.sdk.bases.timetable import BaseTimetable
from airflow.sdk.definitions.asset import AssetAll, BaseAsset
from airflow.sdk.definitions.partition_mappers.identity import IdentityMapper

if TYPE_CHECKING:
    from collections.abc import Collection

    from airflow.sdk import Asset
    from airflow.sdk.definitions.partition_mappers.base import PartitionMapper


@attrs.define
class AssetTriggeredTimetable(BaseTimetable):
    """
    Timetable that never schedules anything.

    This should not be directly used anywhere, but only set if a DAG is triggered by assets.

    :meta private:
    """

    asset_condition: BaseAsset = attrs.field(alias="assets")


@attrs.define
class PartitionedAssetTimetable(AssetTriggeredTimetable):
    """Asset-driven timetable that listens for partitioned assets."""

    asset_condition: BaseAsset = attrs.field(alias="assets")
    partition_mapper_config: dict[BaseAsset, PartitionMapper] = attrs.field(factory=dict)
    default_partition_mapper: PartitionMapper = IdentityMapper()


def _coerce_assets(o: Collection[Asset] | BaseAsset) -> BaseAsset:
    if isinstance(o, BaseAsset):
        return o
    return AssetAll(*o)


@attrs.define(kw_only=True)
class AssetOrTimeSchedule(AssetTriggeredTimetable):
    """
    Combine time-based scheduling with event-based scheduling.

    :param assets: An asset of list of assets, in the same format as
        ``DAG(schedule=...)`` when using event-driven scheduling. This is used
        to evaluate event-based scheduling.
    :param timetable: A timetable instance to evaluate time-based scheduling.
    """

    asset_condition: BaseAsset = attrs.field(alias="assets", converter=_coerce_assets)
    timetable: BaseTimetable

    def __attrs_post_init__(self) -> None:
        self.active_runs_limit = self.timetable.active_runs_limit
        self.can_be_scheduled = self.timetable.can_be_scheduled
