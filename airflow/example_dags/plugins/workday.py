#
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

"""Plugin to demonstrate timetable registration and accommodate example DAGs."""

# [START howto_timetable]
from datetime import timedelta
from typing import Optional

from pendulum import UTC, Date, DateTime, Time

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable


class AfterWorkdayTimetable(Timetable):

    # [START howto_timetable_infer_manual_data_interval]
    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        weekday = run_after.weekday()
        if weekday in (0, 6):  # Monday and Sunday -- interval is last Friday.
            days_since_friday = (run_after.weekday() - 4) % 7
            delta = timedelta(days=days_since_friday)
        else:  # Otherwise the interval is yesterday.
            delta = timedelta(days=1)
        start = DateTime.combine((run_after - delta).date(), Time.min).replace(tzinfo=UTC)
        return DataInterval(start=start, end=(start + timedelta(days=1)))

    # [END howto_timetable_infer_manual_data_interval]

    # [START howto_timetable_next_dagrun_info]
    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if last_automated_data_interval is not None:  # There was a previous run on the regular schedule.
            last_start = last_automated_data_interval.start
            last_start_weekday = last_start.weekday()
            if 0 <= last_start_weekday < 4:  # Last run on Monday through Thursday -- next is tomorrow.
                delta = timedelta(days=1)
            else:  # Last run on Friday -- skip to next Monday.
                delta = timedelta(days=(7 - last_start_weekday))
            next_start = DateTime.combine((last_start + delta).date(), Time.min).replace(tzinfo=UTC)
        else:  # This is the first ever run on the regular schedule.
            next_start = restriction.earliest
            if next_start is None:  # No start_date. Don't schedule.
                return None
            if not restriction.catchup:
                # If the DAG has catchup=False, today is the earliest to consider.
                next_start = max(next_start, DateTime.combine(Date.today(), Time.min).replace(tzinfo=UTC))
            elif next_start.time() != Time.min:
                # If earliest does not fall on midnight, skip to the next day.
                next_day = next_start.date() + timedelta(days=1)
                next_start = DateTime.combine(next_day, Time.min).replace(tzinfo=UTC)
            next_start_weekday = next_start.weekday()
            if next_start_weekday in (5, 6):  # If next start is in the weekend, go to next Monday.
                delta = timedelta(days=(7 - next_start_weekday))
                next_start = next_start + delta
        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.
        return DagRunInfo.interval(start=next_start, end=(next_start + timedelta(days=1)))

    # [END howto_timetable_next_dagrun_info]


class WorkdayTimetablePlugin(AirflowPlugin):
    name = "workday_timetable_plugin"
    timetables = [AfterWorkdayTimetable]


# [END howto_timetable]
