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

from airflow.partition_mappers.allowed_key import AllowedKeyMapper
from airflow.partition_mappers.base import PartitionMapper, RollupMapper
from airflow.partition_mappers.chain import ChainMapper
from airflow.partition_mappers.fixed_key import FixedKeyMapper
from airflow.partition_mappers.identity import IdentityMapper
from airflow.partition_mappers.product import ProductMapper
from airflow.partition_mappers.temporal import (
    FanOutMapper,
    StartOfDayMapper,
    StartOfHourMapper,
    StartOfMonthMapper,
    StartOfQuarterMapper,
    StartOfWeekMapper,
    StartOfYearMapper,
)
from airflow.partition_mappers.wait_policy import MinimumCount, WaitForAll
from airflow.partition_mappers.window import (
    DayWindow,
    HourWindow,
    MonthWindow,
    QuarterWindow,
    SegmentWindow,
    WeekWindow,
    Window,
    YearWindow,
)

__all__ = [
    "AllowedKeyMapper",
    "ChainMapper",
    "DayWindow",
    "FanOutMapper",
    "FixedKeyMapper",
    "HourWindow",
    "IdentityMapper",
    "MinimumCount",
    "MonthWindow",
    "PartitionMapper",
    "ProductMapper",
    "QuarterWindow",
    "RollupMapper",
    "SegmentWindow",
    "StartOfDayMapper",
    "StartOfHourMapper",
    "StartOfMonthMapper",
    "StartOfQuarterMapper",
    "StartOfWeekMapper",
    "StartOfYearMapper",
    "WaitForAll",
    "WeekWindow",
    "Window",
    "YearWindow",
]
