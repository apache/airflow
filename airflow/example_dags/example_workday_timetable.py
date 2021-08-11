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

"""Example DAG demostrating how to implement a custom timetable for a DAG."""

# [START howto_timetable]
from datetime import timedelta
from typing import Optional

from pendulum import Date, DateTime, Time, timezone

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable

UTC = timezone("UTC")


class AfterWorkdayTimetable(Timetable):

    # [START howto_timetable_infer_data_interval]
    def infer_data_interval(self, run_after: DateTime) -> DataInterval:
        weekday = run_after.weekday()
        if weekday in (0, 6):  # Monday and Sunday -- interval is last Friday.
            days_since_friday = (run_after.weekday() - 4) % 7
            delta = timedelta(days=days_since_friday)
        else:  # Otherwise the interval is yesterday.
            delta = timedelta(days=1)
        start = DateTime.combine((run_after - delta).date(), Time.min).replace(tzinfo=UTC)
        return DataInterval(start=start, end=(start + timedelta(days=1)))

    # [END howto_timetable_infer_data_interval]

    # [START howto_timetable_next_dagrun_info]
    def next_dagrun_info(
        self,
        last_automated_dagrun: Optional[DateTime],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if last_automated_dagrun is not None:
            # There was a previous run on the regular schedule.
            # last_automated_dagrun os the last run's logical date.
            weekday = last_automated_dagrun.weekday()
            if 0 <= weekday < 4:  # Monday through Thursday -- next is tomorrow.
                delta = timedelta(days=1)
            else:  # Week is ending -- skip to next Monday.
                delta = timedelta(days=(7 - weekday))
            start = DateTime.combine((last_automated_dagrun + delta).date(), Time.min)
        else:  # This is the first ever run on the regular schedule.
            if restriction.earliest is None:  # No start_date. Don't schedule.
                return None
            start = restriction.earliest
            if start.time() != Time.min:
                # If earliest does not fall on midnight, skip to the next day.
                start = DateTime.combine(start.date(), Time.min).replace(tzinfo=UTC) + timedelta(days=1)
            if not restriction.catchup:
                # If the DAG has catchup=False, today is the earliest to consider.
                start = max(start, DateTime.combine(Date.today(), Time.min).replace(tzinfo=UTC))
            weekday = start.weekday()
            if weekday in (5, 6):  # If 'start' is in the weekend, go to next Monday.
                delta = timedelta(days=(7 - weekday))
                start = start + delta
        if start > restriction.latest:  # Over the DAG's scheduled end; don't schedule.
            return None
        return DagRunInfo.interval(start=start, end=(start + timedelta(days=1)))

    # [END howto_timetable_next_dagrun_info]


with DAG(timetable=AfterWorkdayTimetable(), tags=["example", "timetable"]) as dag:
    DummyOperator(task_id="run_this")


if __name__ == "__main__":
    dag.cli()

# [END howto_timetable]
