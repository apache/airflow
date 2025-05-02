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

import itertools
from collections.abc import Iterable
from typing import TYPE_CHECKING

import pendulum

from airflow.timetables.base import DagRunInfo, DataInterval, Timetable
from airflow.utils import timezone

if TYPE_CHECKING:
    from pendulum import DateTime

    from airflow.timetables.base import TimeRestriction


class EventsTimetable(Timetable):
    """
    Timetable that schedules DAG runs at specific listed datetimes.

    Suitable for predictable but truly irregular scheduling such as sporting events.

    :param event_dates: List of datetimes for the DAG to run at. Duplicates will be ignored. Must be finite
                        and of reasonable size as it will be loaded in its entirety.
    :param restrict_to_events: Whether manual runs should use the most recent event or
        the current time
    :param presorted: if True, event_dates will be assumed to be in ascending order. Provides modest
        performance improvement for larger lists of event_dates.
    :param description: A name for the timetable to display in the UI. Default None will be shown as
                        "X Events" where X is the len of event_dates
    """

    def __init__(
        self,
        event_dates: Iterable[DateTime],
        restrict_to_events: bool = False,
        presorted: bool = False,
        description: str | None = None,
    ):
        self.event_dates = list(event_dates)  # Must be reversible and indexable
        if not presorted:
            # For long lists this could take a while, so only want to do it once
            self.event_dates.sort()
        self.restrict_to_events = restrict_to_events
        if description is None:
            if self.event_dates:
                self.description = (
                    f"{len(self.event_dates)} events between {self.event_dates[0]} and {self.event_dates[-1]}"
                )
            else:
                self.description = "No events"
            self._summary = f"{len(self.event_dates)} events"
        else:
            self._summary = description
            self.description = description

    @property
    def summary(self) -> str:
        return self._summary

    def __repr__(self):
        return self.summary

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        earliest = restriction.earliest
        if not restriction.catchup:
            current_time = timezone.utcnow()
            if earliest is None or current_time > earliest:
                earliest = pendulum.instance(current_time)

        for next_event in self.event_dates:
            if earliest and next_event < earliest:
                continue
            if last_automated_data_interval and next_event <= last_automated_data_interval.end:
                continue
            break
        else:
            # We need to return None if self.event_dates is empty or,
            # if not empty, when no suitable event can be found.
            return None

        if restriction.latest is not None and next_event > restriction.latest:
            return None

        return DagRunInfo.exact(next_event)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        # If Timetable not restricted to events, run for the time specified
        if not self.restrict_to_events or not self.event_dates:
            return DataInterval.exact(run_after)

        # If restricted to events, run for the most recent past event
        # or for the first event if all events are in the future
        if run_after < self.event_dates[0]:
            return DataInterval.exact(self.event_dates[0])
        past_events = itertools.dropwhile(lambda when: when > run_after, self.event_dates[::-1])
        most_recent_event = next(past_events)
        return DataInterval.exact(most_recent_event)

    def serialize(self):
        return {
            "event_dates": [x.isoformat(sep="T") for x in self.event_dates],
            "restrict_to_events": self.restrict_to_events,
        }

    @classmethod
    def deserialize(cls, data) -> Timetable:
        return cls(
            [pendulum.DateTime.fromisoformat(x) for x in data["event_dates"]],
            data["restrict_to_events"],
            presorted=True,
        )
