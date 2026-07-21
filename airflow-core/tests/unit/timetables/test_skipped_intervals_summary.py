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

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, CronTriggerTimetable
from airflow.timetables.base import DagRunInfo, DataInterval

from tests_common.test_utils.dag import create_scheduler_dag

DEFAULT_DATE = pendulum.datetime(2016, 1, 1, tz="UTC")


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
    prev_info = DagRunInfo.interval(DEFAULT_DATE, prev_end)
    new_info = DagRunInfo.interval(new_start, new_start + timedelta(days=1))

    summary = scheduler_dag.summarize_skipped_intervals_between(prev_info, new_info)

    assert summary is not None
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
    prev_info = DagRunInfo.interval(DEFAULT_DATE, boundary)
    new_info = DagRunInfo.interval(boundary, boundary + timedelta(days=1))
    assert scheduler_dag.summarize_skipped_intervals_between(prev_info, new_info) is None


def test_summarize_skipped_intervals_between_returns_none_for_consecutive_trigger_runs():
    """Zero-width consecutive CronTriggerTimetable runs must not look like a skip."""
    dag = DAG(
        dag_id="test_summarize_consecutive_trigger",
        schedule=CronTriggerTimetable("@daily", timezone="UTC"),
        start_date=DEFAULT_DATE,
        catchup=False,
    )
    EmptyOperator(task_id="dummy", dag=dag)
    scheduler_dag = create_scheduler_dag(dag)

    prev_info = DagRunInfo.exact(DEFAULT_DATE)
    new_info = DagRunInfo.exact(DEFAULT_DATE + timedelta(days=1))
    assert scheduler_dag.summarize_skipped_intervals_between(prev_info, new_info) is None


def test_summarize_skipped_intervals_between_returns_gap_for_skipped_trigger_runs():
    """A real multi-period jump on CronTriggerTimetable must still report a skip."""
    dag = DAG(
        dag_id="test_summarize_skipped_trigger",
        schedule=CronTriggerTimetable("@daily", timezone="UTC"),
        start_date=DEFAULT_DATE,
        catchup=False,
    )
    EmptyOperator(task_id="dummy", dag=dag)
    scheduler_dag = create_scheduler_dag(dag)

    prev_info = DagRunInfo.exact(DEFAULT_DATE)
    new_start = DEFAULT_DATE + timedelta(days=3)
    new_info = DagRunInfo.exact(new_start)

    summary = scheduler_dag.summarize_skipped_intervals_between(prev_info, new_info)

    assert summary is not None
    assert summary.skipped_range == DataInterval(start=DEFAULT_DATE, end=new_start)
