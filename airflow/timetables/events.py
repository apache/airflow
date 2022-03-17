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

from typing import Iterable, Optional

import numpy as np
import pendulum
from pendulum import DateTime

from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.timetables.simple import NullTimetable


class EventsTimetable(NullTimetable):
    """
    Timetable that schedules DAG runs at specific listed datetimes. Suitable for truly
    irregular scheduling such as sporting events.

    :param event_dates: List of datetimes for the DAG to run at
    :type event_dates: Iterable of pendulum DateTimes
    :param restrict_to_events: Whether manual runs should use the most recent event or
    the current time
    :type restrict_to_events: bool
    """

    def __init__(self, event_dates: Iterable[DateTime], restrict_to_events: bool = False):

        self.event_dates: np.array[pendulum.DateTime] = np.array(event_dates)
        self.restrict_to_events = restrict_to_events

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if last_automated_data_interval is None:
            next_event = self.event_dates.min()
        else:
            future_dates = self.event_dates[self.event_dates > last_automated_data_interval.end]
            if len(future_dates) > 0:
                next_event = future_dates.min()
            else:
                next_event = None

        return DagRunInfo.exact(next_event)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        # If Timetable not restricted to events, run for the time specified
        if not self.restrict_to_events:
            return DataInterval.exact(run_after)

        # If restricted to events, run for the most recent past event
        # or for the first event if all events are in the future
        if run_after < self.event_dates.min():
            return DataInterval.exact(self.event_dates.min())
        else:
            most_recent_event = self.event_dates[self.event_dates <= run_after].max()
            return DataInterval.exact(most_recent_event)

    def serialize(self):
        return {
            "event_dates": [str(x) for x in self.event_dates],
            "restrict_to_events": self.restrict_to_events,
        }

    @classmethod
    def deserialize(cls, data) -> Timetable:
        return cls(np.array([pendulum.parse(x) for x in data["event_dates"]]), data["restrict_to_events"])
