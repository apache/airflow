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
    ToDailyMapper,
    ToHourlyMapper,
    ToMonthlyMapper,
    ToQuarterlyMapper,
    ToWeeklyMapper,
    ToYearlyMapper,
    _BaseTemporalMapper,
)


class TestTemporalMappers:
    @pytest.mark.parametrize(
        ("mapper_cls", "expected_downstream_key"),
        [
            (ToHourlyMapper, "2026-02-10T14"),
            (ToDailyMapper, "2026-02-10"),
            (ToWeeklyMapper, "2026-02-09 (W07)"),
            (ToMonthlyMapper, "2026-02"),
            (ToQuarterlyMapper, "2026-Q1"),
            (ToYearlyMapper, "2026"),
        ],
    )
    def test_to_downstream(
        self,
        mapper_cls: type[_BaseTemporalMapper],
        expected_downstream_key: str,
    ):
        pm = mapper_cls()
        assert pm.to_downstream("2026-02-10T14:30:45") == expected_downstream_key

    @pytest.mark.parametrize(
        ("mapper_cls", "expected_outut_format"),
        [
            (ToHourlyMapper, "%Y-%m-%dT%H"),
            (ToDailyMapper, "%Y-%m-%d"),
            (ToWeeklyMapper, "%Y-%m-%d (W%V)"),
            (ToMonthlyMapper, "%Y-%m"),
            (ToQuarterlyMapper, "%Y-Q{quarter}"),
            (ToYearlyMapper, "%Y"),
        ],
    )
    def test_serialize(self, mapper_cls: type[_BaseTemporalMapper], expected_outut_format: str):
        pm = mapper_cls()
        assert pm.serialize() == {
            "input_format": "%Y-%m-%dT%H:%M:%S",
            "output_format": expected_outut_format,
        }

    @pytest.mark.parametrize(
        "mapper_cls",
        [ToHourlyMapper, ToDailyMapper, ToWeeklyMapper, ToMonthlyMapper, ToQuarterlyMapper, ToYearlyMapper],
    )
    def test_deserialize(self, mapper_cls):
        pm = mapper_cls.deserialize(
            {
                "input_format": "%Y-%m-%dT%H:%M:%S",
                "output_format": "customized-format",
            }
        )
        assert isinstance(pm, mapper_cls)
        assert pm.input_format == "%Y-%m-%dT%H:%M:%S"
        assert pm.output_format == "customized-format"
