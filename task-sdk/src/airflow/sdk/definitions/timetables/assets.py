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
from airflow.sdk.exceptions import AirflowTimetableInvalid

if TYPE_CHECKING:
    from collections.abc import Collection

    from airflow.sdk import Asset
    from airflow.sdk.definitions.partition_mappers.base import PartitionMapper


def _get_default_batch_asset_events(timetable: AssetTriggeredTimetable) -> bool:
    from airflow.sdk.configuration import conf

    return (
        conf.getboolean("scheduler", "batch_asset_events", fallback=False)
        or timetable.get_batching_requirement() is not None
    )


@attrs.define
class AssetTriggeredTimetable(BaseTimetable):
    """
    Schedule a Dag when its asset condition is satisfied.

    :param assets: The asset expression that triggers the Dag.
    :param batch_asset_events: Consume queued events together in one Dag run. Defaults
        to ``[scheduler] batch_asset_events``, which is false, except that conditions
        combining multiple assets with ``&`` and ``RollupMapper`` partition mappers always
        default to batching. Passing ``False`` explicitly for those is rejected.
    """

    asset_triggered = True
    asset_condition: BaseAsset = attrs.field(alias="assets")
    batch_asset_events: bool = attrs.field(
        default=attrs.Factory(_get_default_batch_asset_events, takes_self=True), kw_only=True
    )

    def get_batching_requirement(self) -> str | None:
        """Return why this timetable cannot run without event batching, or ``None``."""
        if self.asset_condition.requires_batching:
            return "Asset AND conditions require batch_asset_events=True"
        return None

    def validate(self) -> None:
        if not self.batch_asset_events and (reason := self.get_batching_requirement()):
            raise AirflowTimetableInvalid(reason)


@attrs.define
class PartitionedAssetTimetable(AssetTriggeredTimetable):
    """Asset-driven timetable that listens for partitioned assets."""

    partition_mapper_config: dict[BaseAsset, PartitionMapper] = attrs.field(factory=dict)
    default_partition_mapper: PartitionMapper = IdentityMapper()

    # The factory needs the partition mappers to be initialized first.
    batch_asset_events: bool = attrs.field(
        default=attrs.Factory(_get_default_batch_asset_events, takes_self=True), kw_only=True
    )

    def get_batching_requirement(self) -> str | None:
        if any(
            mapper.is_rollup
            for mapper in (self.default_partition_mapper, *self.partition_mapper_config.values())
        ):
            return "Partition rollups require batch_asset_events=True"
        return super().get_batching_requirement()


class PartitionedAtRuntime(BaseTimetable):
    """Marker timetable indicating that partition key(s) are determined at runtime."""

    can_be_scheduled = False
    partitioned_at_runtime = True


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

    batch_asset_events: bool = attrs.field(
        default=attrs.Factory(_get_default_batch_asset_events, takes_self=True), kw_only=True
    )

    def __attrs_post_init__(self) -> None:
        self.active_runs_limit = self.timetable.active_runs_limit
        self.can_be_scheduled = self.timetable.can_be_scheduled


@attrs.define(kw_only=True)
class AssetAndTimeSchedule(BaseTimetable):
    """
    Combine time-based scheduling with asset conditions.

    :param assets: An asset or list of assets, in the same format as
        ``DAG(schedule=...)`` when using event-driven scheduling. This is used
        to evaluate whether a scheduled run can be created.
    :param timetable: A timetable instance to evaluate time-based scheduling.
    """

    asset_gated = True
    asset_condition: BaseAsset = attrs.field(alias="assets", converter=_coerce_assets)
    timetable: BaseTimetable

    @property
    def active_runs_limit(self) -> int | None:  # type: ignore[override]
        return self.timetable.active_runs_limit

    @property
    def can_be_scheduled(self) -> bool:  # type: ignore[override]
        return self.timetable.can_be_scheduled
