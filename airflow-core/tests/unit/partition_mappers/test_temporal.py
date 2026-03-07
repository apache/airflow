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

import pytest

from airflow.partition_mappers.temporal import (
    DailyMapper,
    HourlyMapper,
    MonthlyMapper,
    QuarterlyMapper,
    WeeklyMapper,
    YearlyMapper,
    _BaseTemporalMapper,
)


class TestTemporalMappers:
    @pytest.mark.parametrize(
        ("mapper_cls", "expected_downstream_key"),
        [
            (HourlyMapper, "2026-02-10T14+0000"),
            (DailyMapper, "2026-02-10+0000"),
            (WeeklyMapper, "2026-02-09 (W07)+0000"),
            (MonthlyMapper, "2026-02+0000"),
            (QuarterlyMapper, "2026-Q1+0000"),
            (YearlyMapper, "2026+0000"),
        ],
    )
    def test_to_downstream(
        self,
        mapper_cls: type[_BaseTemporalMapper],
        expected_downstream_key: str,
    ):
        pm = mapper_cls(timezone="UTC")
        assert pm.to_downstream("2026-02-10T14:30:45+0000") == expected_downstream_key

    @pytest.mark.parametrize(
        ("mapper_cls", "expected_outut_format"),
        [
            (HourlyMapper, "%Y-%m-%dT%H%z"),
            (DailyMapper, "%Y-%m-%d%z"),
            (WeeklyMapper, "%Y-%m-%d (W%V)%z"),
            (MonthlyMapper, "%Y-%m%z"),
            (QuarterlyMapper, "%Y-Q{quarter}%z"),
            (YearlyMapper, "%Y%z"),
        ],
    )
    def test_serialize(self, mapper_cls: type[_BaseTemporalMapper], expected_outut_format: str):
        pm = mapper_cls(timezone="UTC")
        assert pm.serialize() == {
            "input_format": "%Y-%m-%dT%H:%M:%S%z",
            "output_format": expected_outut_format,
            "timezone": "UTC",
        }

    @pytest.mark.parametrize(
        "mapper_cls",
        [HourlyMapper, DailyMapper, WeeklyMapper, MonthlyMapper, QuarterlyMapper, YearlyMapper],
    )
    def test_deserialize(self, mapper_cls):
        pm = mapper_cls.deserialize(
            {"input_format": "%Y-%m-%dT%H:%M:%S%z", "output_format": "customized-format", "timezone": "UTC"}
        )
        assert isinstance(pm, mapper_cls)
        assert pm.input_format == "%Y-%m-%dT%H:%M:%S%z"
        assert pm.output_format == "customized-format"
