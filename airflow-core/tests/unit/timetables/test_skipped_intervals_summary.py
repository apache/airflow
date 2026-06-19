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

from datetime import timedelta

import pendulum
import pytest

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.timetables.base import DataInterval
from airflow.timetables.interval import CronDataIntervalTimetable, DeltaDataIntervalTimetable

from tests_common.test_utils.dag import create_scheduler_dag

DEFAULT_DATE = pendulum.datetime(2016, 1, 1, tz="UTC")


def _reference_count(timetable, prev_end, new_start):
    from airflow.timetables.base import TimeRestriction

    count = 0
    info = None
    restriction = TimeRestriction(prev_end, new_start, catchup=True)
    while True:
        info = timetable.next_dagrun_info_v2(last_dagrun_info=info, restriction=restriction)
        if not info or not info.data_interval:
            break
        if info.data_interval.start >= new_start:
            break
        count += 1
    return count


@pytest.mark.parametrize(
    ("schedule", "prev_end", "new_start", "expected_count"),
    [
        pytest.param(
            timedelta(days=1),
            DEFAULT_DATE + timedelta(days=1),
            DEFAULT_DATE + timedelta(days=4),
            3,
            id="delta-daily",
        ),
        pytest.param(
            timedelta(hours=1),
            DEFAULT_DATE + timedelta(hours=1),
            DEFAULT_DATE + timedelta(hours=4),
            3,
            id="delta-hourly",
        ),
        pytest.param(
            "@daily",
            DEFAULT_DATE + timedelta(days=1),
            DEFAULT_DATE + timedelta(days=4),
            3,
            id="cron-daily",
        ),
        pytest.param(
            "@hourly",
            DEFAULT_DATE + timedelta(hours=1),
            DEFAULT_DATE + timedelta(hours=4),
            3,
            id="cron-hourly",
        ),
        pytest.param(
            "0 0 1 * *",
            pendulum.datetime(2016, 2, 1, tz="UTC"),
            pendulum.datetime(2016, 4, 1, tz="UTC"),
            2,
            id="cron-monthly",
        ),
    ],
)
def test_count_skipped_intervals_between_matches_reference(schedule, prev_end, new_start, expected_count):
    if isinstance(schedule, str):
        timetable = CronDataIntervalTimetable(schedule, "UTC")
    else:
        timetable = DeltaDataIntervalTimetable(schedule)

    assert timetable.count_skipped_intervals_between(prev_end, new_start) == expected_count
    assert _reference_count(timetable, prev_end, new_start) == expected_count


def test_summarize_skipped_intervals_between_returns_gap_summary():
    dag = DAG(
        dag_id="test_summarize_skipped_intervals",
        schedule=timedelta(days=1),
        start_date=DEFAULT_DATE,
        catchup=False,
    )
    EmptyOperator(task_id="dummy", dag=dag)
    scheduler_dag = create_scheduler_dag(dag)

    prev_end = DEFAULT_DATE + timedelta(days=1)
    new_start = DEFAULT_DATE + timedelta(days=4)

    summary = scheduler_dag.summarize_skipped_intervals_between(prev_end, new_start)

    assert summary is not None
    assert summary.skipped_interval_count == 3
    assert summary.skipped_range == DataInterval(start=prev_end, end=new_start)


def test_summarize_skipped_intervals_between_returns_none_without_gap():
    dag = DAG(
        dag_id="test_summarize_skipped_intervals_no_gap",
        schedule=timedelta(days=1),
        start_date=DEFAULT_DATE,
        catchup=False,
    )
    EmptyOperator(task_id="dummy", dag=dag)
    scheduler_dag = create_scheduler_dag(dag)

    boundary = DEFAULT_DATE + timedelta(days=1)
    assert scheduler_dag.summarize_skipped_intervals_between(boundary, boundary) is None
