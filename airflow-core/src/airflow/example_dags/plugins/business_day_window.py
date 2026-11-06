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

import calendar
from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import ClassVar

from airflow.partition_mappers.window import Window
from airflow.plugins_manager import AirflowPlugin


# [START custom_window]
class BusinessDayWindow(Window):
    """
    A calendar-month rollup window that yields only weekday (Mon–Fri) period-starts.

    The built-in :class:`~airflow.partition_mappers.window.MonthWindow` yields every
    calendar day in the month. ``BusinessDayWindow`` skips Saturdays and Sundays, so
    a monthly downstream asset only waits for the business-day upstream partitions —
    useful for financial or operational pipelines whose upstream data isn't produced
    on weekends.

    This class demonstrates registering a custom ``Window`` subclass via the
    ``AirflowPlugin.windows`` registry: any plugin that lists it in ``windows = [...]``
    makes it available to ``RollupMapper`` without modifying core Airflow.

    *Assumes FORWARD direction and a day-1 month anchor* — the standard contract for
    month-aligned temporal upstream mappers.
    """

    expected_decoded_type: ClassVar[type] = datetime

    def to_upstream(self, period_start: datetime) -> Iterable[datetime]:
        if period_start.day != 1:
            raise ValueError(
                f"BusinessDayWindow expects a period start on day 1 of the month, "
                f"got {period_start.isoformat()}."
            )
        days_in_month = calendar.monthrange(period_start.year, period_start.month)[1]
        return (day for i in range(days_in_month) if (day := period_start + timedelta(days=i)).weekday() < 5)


class BusinessDayWindowPlugin(AirflowPlugin):
    name = "business_day_window_plugin"
    windows = [BusinessDayWindow]


# [END custom_window]
