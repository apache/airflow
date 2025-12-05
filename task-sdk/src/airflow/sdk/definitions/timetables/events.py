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

if TYPE_CHECKING:
    from collections.abc import Iterable

    from pendulum import DateTime


@attrs.define(init=False)
class EventsTimetable(BaseTimetable):
    """
    Timetable that schedules DAG runs at specific listed datetimes.

    Suitable for predictable but truly irregular scheduling, such as sporting
    events, or to schedule against National Holidays.

    :param event_dates: List of datetimes for the DAG to run at. Duplicates
        will be ignored. This must be finite and of reasonable size, as it will
        be loaded in its entirety.
    :param restrict_to_events: Whether manual runs should use the most recent
        event or the current time
    :param presorted: if True, event_dates will be assumed to be in ascending
        order. Provides modest performance improvement for larger lists of
        *event_dates*.
    :param description: A name for the timetable to display in the UI. If not
        provided explicitly (or *None*) the UI will show "X Events" where X is
        the length of *event_dates*.
    """

    event_dates: list[DateTime]
    restrict_to_events: bool
    description: str | None

    def __init__(
        self,
        event_dates: Iterable[DateTime],
        *,
        restrict_to_events: bool = False,
        presorted: bool = False,
        description: str | None = None,
    ) -> None:
        self.__attrs_init__(  # type: ignore[attr-defined]
            sorted(event_dates) if presorted else list(event_dates),
            restrict_to_events=restrict_to_events,
            description=description,
        )
