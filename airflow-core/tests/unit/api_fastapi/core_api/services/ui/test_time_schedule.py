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

from airflow.api_fastapi.core_api.datamodels.ui.time_schedule import TimeScheduleItem
from airflow.api_fastapi.core_api.services.ui.time_schedule import aggregate_time_schedule_items
from airflow.utils.state import DagRunState


def _make_item(*, start: pendulum.DateTime, duration_minutes: int, run_id: str) -> TimeScheduleItem:
    return TimeScheduleItem(
        dag_id="example_dag",
        dag_run_id=run_id,
        duration_ms=duration_minutes * 60_000,
        end_date=start + timedelta(minutes=duration_minutes),
        is_time_scheduled=True,
        dag_display_name="example_dag",
        run_count=1,
        run_after_min=start,
        run_after_max=start,
        start_date=start,
        state=DagRunState.SUCCESS,
    )


@pytest.mark.parametrize(
    ("aggregation_mode", "expected_start", "expected_end"),
    [
        ("mean", "2026-08-03T09:10:00Z", "2026-08-03T09:30:00Z"),
        ("max", "2026-08-03T09:00:00Z", "2026-08-03T09:50:00Z"),
        ("min", "2026-08-03T09:00:00Z", "2026-08-03T09:10:00Z"),
    ],
)
def test_aggregate_time_schedule_items_uses_selected_duration_rule(
    aggregation_mode, expected_start, expected_end
):
    items = [
        _make_item(start=pendulum.parse("2026-08-03T09:00:00Z"), duration_minutes=10, run_id="run-1"),
        _make_item(start=pendulum.parse("2026-08-03T09:20:00Z"), duration_minutes=30, run_id="run-2"),
    ]

    [result] = aggregate_time_schedule_items(
        aggregation_mode=aggregation_mode,
        items=items,
        time_scale=60,
        timezone="UTC",
        view_mode="day",
    )

    assert result.start_date == pendulum.parse(expected_start)
    assert result.end_date == pendulum.parse(expected_end)
    assert result.run_count == 2
    assert result.start_time_gte == "09:00"
    assert result.start_time_lt == "10:00"


def test_aggregate_time_schedule_items_only_groups_week_items_from_the_same_weekday():
    items = [
        _make_item(start=pendulum.parse("2026-08-03T09:00:00Z"), duration_minutes=10, run_id="monday"),
        _make_item(start=pendulum.parse("2026-08-04T09:00:00Z"), duration_minutes=10, run_id="tuesday"),
    ]

    day_items = aggregate_time_schedule_items(
        aggregation_mode="mean", items=items, time_scale=60, timezone="UTC", view_mode="day"
    )
    week_items = aggregate_time_schedule_items(
        aggregation_mode="mean", items=items, time_scale=60, timezone="UTC", view_mode="week"
    )

    assert len(day_items) == 1
    assert day_items[0].run_count == 2
    assert day_items[0].run_after_min == pendulum.parse("2026-08-03T09:00:00Z")
    assert day_items[0].run_after_max == pendulum.parse("2026-08-04T09:00:00Z")
    assert day_items[0].start_time_gte == "09:00"
    assert day_items[0].start_time_lt == "10:00"
    assert day_items[0].start_weekday is None
    assert {item.start_weekday for item in week_items} == {1, 2}
    assert len(week_items) == 2
    assert {item.run_count for item in week_items} == {1}


def test_aggregate_time_schedule_items_preserves_a_duration_across_midnight():
    [result] = aggregate_time_schedule_items(
        aggregation_mode="mean",
        items=[
            _make_item(start=pendulum.parse("2026-08-03T23:50:00Z"), duration_minutes=20, run_id="overnight")
        ],
        time_scale=60,
        timezone="UTC",
        view_mode="day",
    )

    assert result.duration_ms == 20 * 60_000
    assert result.end_date == pendulum.parse("2026-08-04T00:10:00Z")
    assert result.start_time_lt == "24:00"


def test_aggregate_time_schedule_items_caps_the_last_local_bucket_at_midnight():
    [result] = aggregate_time_schedule_items(
        aggregation_mode="mean",
        items=[
            _make_item(start=pendulum.parse("2026-08-03T14:27:00Z"), duration_minutes=6, run_id="last-bucket")
        ],
        time_scale=50,
        timezone="Asia/Seoul",
        view_mode="week",
    )
    assert result.start_time_gte == "23:20"
    assert result.start_time_lt == "24:00"
    assert result.start_weekday == 1
