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

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Literal

import pendulum

from airflow.api_fastapi.core_api.datamodels.ui.time_schedule import TimeScheduleItem

AggregationMode = Literal["max", "mean", "min"]
ViewMode = Literal["day", "week"]


def _get_local_day_start(value: datetime, timezone: str) -> pendulum.DateTime:
    local_value = pendulum.instance(value).in_timezone(timezone)
    return local_value.start_of("day")


def _get_local_offset(value: datetime, *, day_start: pendulum.DateTime, timezone: str) -> float:
    local_value = pendulum.instance(value).in_timezone(timezone)
    return (local_value - day_start).total_seconds() * 1000


def aggregate_time_schedule_items(
    *,
    aggregation_mode: AggregationMode,
    items: list[TimeScheduleItem],
    time_scale: int,
    timezone: str,
    view_mode: ViewMode,
) -> list[TimeScheduleItem]:
    """Aggregate items into the visible time buckets for one selected view."""
    groups: dict[tuple[str, int | None, int, object], list[TimeScheduleItem]] = defaultdict(list)

    for item in items:
        if item.is_placeholder or item.start_date is None:
            continue
        local_start = pendulum.instance(item.start_date).in_timezone(timezone)
        minute = local_start.hour * 60 + local_start.minute
        bucket_minute = minute // time_scale * time_scale
        weekday = (local_start.day_of_week + 1) % 7 if view_mode == "week" else None
        groups[(item.dag_id, weekday, bucket_minute, item.state)].append(item)

    aggregated_items: list[TimeScheduleItem] = []
    for group in groups.values():
        representative = group[0]
        if representative.start_date is None:
            continue

        day_start = _get_local_day_start(representative.start_date, timezone)
        timed_items: list[tuple[TimeScheduleItem, float, float]] = []
        for item in group:
            if item.start_date is None:
                continue
            item_day_start = _get_local_day_start(item.start_date, timezone)
            start_offset = _get_local_offset(item.start_date, day_start=item_day_start, timezone=timezone)
            end_offset = _get_local_offset(
                item.end_date or item.start_date,
                day_start=item_day_start,
                timezone=timezone,
            )
            timed_items.append((item, start_offset, end_offset))

        if aggregation_mode == "max":
            start_offset = min(value[1] for value in timed_items)
            end_offset = max(value[2] for value in timed_items)
        elif aggregation_mode == "min":
            shortest = min(timed_items, key=lambda value: value[0].duration_ms)
            start_offset, end_offset = shortest[1], shortest[2]
        else:
            start_offset = sum(value[1] for value in timed_items) / len(timed_items)
            end_offset = sum(value[2] for value in timed_items) / len(timed_items)

        aggregated_start = day_start + timedelta(milliseconds=start_offset)
        aggregated_end = day_start + timedelta(milliseconds=end_offset)
        aggregated_items.append(
            representative.model_copy(
                update={
                    "duration_ms": (aggregated_end - aggregated_start).total_seconds() * 1000,
                    "end_date": aggregated_end,
                    "run_count": sum(not item.is_planned for item in group),
                    "start_date": aggregated_start,
                }
            )
        )

    return aggregated_items
