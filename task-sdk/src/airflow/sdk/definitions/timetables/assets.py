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

import typing

import attrs

from airflow.sdk.definitions.asset import AssetAll, BaseAsset
from airflow.sdk.definitions.timetables.simple import AssetTriggeredTimetable

if typing.TYPE_CHECKING:
    from collections.abc import Collection

    from airflow.sdk import Asset
    from airflow.sdk.definitions.timetables.base import BaseTimetable


@attrs.define(kw_only=True)
class AssetOrTimeSchedule(AssetTriggeredTimetable):
    """Combine time-based scheduling with event-based scheduling."""

    timetable: BaseTimetable

    def __init__(
        self,
        *,
        timetable: BaseTimetable,
        assets: Collection[Asset] | BaseAsset,
    ) -> None:
        if isinstance(assets, BaseAsset):
            asset_condition = assets
        else:
            asset_condition = AssetAll(*assets)
        self.__attrs_init__(timetable=timetable, assets=asset_condition)
        self.active_runs_limit = timetable.active_runs_limit
        self.can_be_scheduled = timetable.can_be_scheduled
