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

import pendulum
import pytest

from airflow import sdk
from airflow.partition_mappers.temporal import (
    StartOfDayMapper,
    StartOfHourMapper,
    StartOfMonthMapper,
    StartOfQuarterMapper,
    StartOfWeekMapper,
    StartOfYearMapper,
    _BaseTemporalMapper,
)
from airflow.serialization.decoders import decode_partition_mapper
from airflow.serialization.encoders import encode_partition_mapper


class TestTemporalMappers:
    @pytest.mark.parametrize(
        ("mapper_cls", "expected_downstream_key"),
        [
            (StartOfHourMapper, "2026-02-10T14"),
            (StartOfDayMapper, "2026-02-10"),
            (StartOfWeekMapper, "2026-02-09 (W07)"),
            (StartOfMonthMapper, "2026-02"),
            (StartOfQuarterMapper, "2026-Q1"),
            (StartOfYearMapper, "2026"),
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
        ("timezone", "expected_timezone"),
        [
            (None, "UTC"),
            ("America/New_York", "America/New_York"),
        ],
    )
    @pytest.mark.parametrize(
        ("mapper_cls", "expected_output_format"),
        [
            (StartOfHourMapper, "%Y-%m-%dT%H"),
            (StartOfDayMapper, "%Y-%m-%d"),
            (StartOfWeekMapper, "%Y-%m-%d (W%V)"),
            (StartOfMonthMapper, "%Y-%m"),
            (StartOfQuarterMapper, "%Y-Q{quarter}"),
            (StartOfYearMapper, "%Y"),
        ],
    )
    def test_serialize(
        self,
        mapper_cls: type[_BaseTemporalMapper],
        expected_output_format: str,
        timezone: str | None,
        expected_timezone: str,
    ):
        pm = mapper_cls() if timezone is None else mapper_cls(timezone=timezone)
        assert pm.serialize() == {
            "timezone": expected_timezone,
            "input_format": "%Y-%m-%dT%H:%M:%S",
            "output_format": expected_output_format,
        }

    @pytest.mark.parametrize(
        ("timezone_payload", "expected_tz_name"),
        [
            ("UTC", "UTC"),
            ("Asia/Taipei", "Asia/Taipei"),
        ],
    )
    @pytest.mark.parametrize(
        "mapper_cls",
        [
            StartOfHourMapper,
            StartOfDayMapper,
            StartOfWeekMapper,
            StartOfMonthMapper,
            StartOfQuarterMapper,
            StartOfYearMapper,
        ],
    )
    def test_deserialize(self, mapper_cls, timezone_payload, expected_tz_name):
        pm = mapper_cls.deserialize(
            {
                "timezone": timezone_payload,
                "input_format": "%Y-%m-%dT%H:%M:%S",
                "output_format": "customized-format",
            }
        )
        assert isinstance(pm, mapper_cls)
        assert pm.input_format == "%Y-%m-%dT%H:%M:%S"
        assert pm.output_format == "customized-format"
        assert pm._timezone.name == expected_tz_name

    @pytest.mark.parametrize(
        "mapper_cls",
        [
            StartOfHourMapper,
            StartOfDayMapper,
            StartOfWeekMapper,
            StartOfMonthMapper,
            StartOfQuarterMapper,
            StartOfYearMapper,
        ],
    )
    def test_deserialize_legacy_payload_without_timezone(self, mapper_cls):
        """Payloads written by ``task-sdk`` 1.2.1 omit ``timezone`` from the
        SDK-mapper wire format; the core decoder must default it to UTC so
        those serialized Dags can still be loaded."""
        pm = mapper_cls.deserialize(
            {
                "input_format": "%Y-%m-%dT%H:%M:%S",
                "output_format": "customized-format",
            }
        )
        assert isinstance(pm, mapper_cls)
        assert pm._timezone.name == "UTC"

    def test_to_downstream_timezone_aware(self):
        """Input is interpreted as local time in the given timezone."""
        pm = StartOfDayMapper(timezone="America/New_York")
        # 2026-02-10T23:00:00 in New York local time → start-of-day is 2026-02-10
        assert pm.to_downstream("2026-02-10T23:00:00") == "2026-02-10"
        # 2026-02-11T01:00:00 in New York local time → start-of-day is 2026-02-11
        assert pm.to_downstream("2026-02-11T01:00:00") == "2026-02-11"

    def test_to_downstream_input_timezone_differs_from_mapper_timezone(self):
        """When input_format includes %z and the parsed tz differs from the mapper tz,
        the key is converted to the mapper timezone before normalization."""
        pm = StartOfDayMapper(
            timezone="America/New_York",
            input_format="%Y-%m-%dT%H:%M:%S%z",
        )
        # 2026-02-11T04:00:00+00:00 UTC == 2026-02-10T23:00:00-05:00 New York
        # → start-of-day in New York is 2026-02-10, not 2026-02-11
        assert pm.to_downstream("2026-02-11T04:00:00+0000") == "2026-02-10"
        # 2026-02-11T06:00:00+00:00 UTC == 2026-02-11T01:00:00-05:00 New York
        # → start-of-day in New York is 2026-02-11
        assert pm.to_downstream("2026-02-11T06:00:00+0000") == "2026-02-11"

    def test_validate_source_key(self):
        StartOfHourMapper().validate_source_key("2026-02-10T14:30:45")

    def test_validate_source_key_rejects_non_canonical_input(self):
        with pytest.raises(ValueError, match="does not round-trip"):
            StartOfHourMapper().validate_source_key("2026-2-10T4:30:45")


class TestSdkTemporalMappersTimezoneSerialization:
    """
    SDK-side temporal mappers (``airflow.sdk.StartOf*Mapper``) must accept a
    ``timezone`` kwarg in their constructor and round-trip it through the
    encoder/decoder path. Previously the SDK class signature omitted timezone
    entirely and the dispatch handler in ``encoders._Serializer`` dropped it,
    so a Dag using ``StartOfDayMapper(timezone="Asia/Taipei")`` silently fell
    back to UTC after serialization.
    """

    @pytest.mark.parametrize("timezone", ["UTC", "America/New_York", "Asia/Taipei"])
    @pytest.mark.parametrize(
        "sdk_mapper_name",
        [
            "StartOfHourMapper",
            "StartOfDayMapper",
            "StartOfWeekMapper",
            "StartOfMonthMapper",
            "StartOfQuarterMapper",
            "StartOfYearMapper",
        ],
    )
    def test_sdk_constructor_accepts_timezone(self, sdk_mapper_name, timezone):
        sdk_cls = getattr(sdk, sdk_mapper_name)
        mapper = sdk_cls(timezone=timezone)
        assert mapper._timezone.name == timezone

    @pytest.mark.parametrize("timezone", ["UTC", "America/New_York", "Asia/Taipei"])
    @pytest.mark.parametrize(
        ("sdk_mapper_name", "core_cls"),
        [
            ("StartOfHourMapper", StartOfHourMapper),
            ("StartOfDayMapper", StartOfDayMapper),
            ("StartOfWeekMapper", StartOfWeekMapper),
            ("StartOfMonthMapper", StartOfMonthMapper),
            ("StartOfQuarterMapper", StartOfQuarterMapper),
            ("StartOfYearMapper", StartOfYearMapper),
        ],
    )
    def test_encode_decode_round_trip_preserves_timezone(self, sdk_mapper_name, core_cls, timezone):
        sdk_cls = getattr(sdk, sdk_mapper_name)
        original = sdk_cls(timezone=timezone)
        restored = decode_partition_mapper(encode_partition_mapper(original))

        # decode resolves to the Core class via BUILTIN_PARTITION_MAPPERS.
        assert isinstance(restored, core_cls)
        assert restored._timezone.name == timezone

    @pytest.mark.parametrize(
        "sdk_mapper_name",
        [
            "StartOfHourMapper",
            "StartOfDayMapper",
            "StartOfWeekMapper",
            "StartOfMonthMapper",
            "StartOfQuarterMapper",
            "StartOfYearMapper",
        ],
    )
    def test_encode_decode_round_trip_accepts_pendulum_tzinfo(self, sdk_mapper_name):
        """The SDK ``timezone`` kwarg is advertised as ``str | Timezone | FixedTimezone``;
        a pendulum tz object must survive the encoder pipeline (encode_timezone handles
        the object branch) and land on the core class with the matching IANA name."""
        sdk_cls = getattr(sdk, sdk_mapper_name)
        original = sdk_cls(timezone=pendulum.timezone("Asia/Taipei"))
        restored = decode_partition_mapper(encode_partition_mapper(original))

        assert restored._timezone.name == "Asia/Taipei"

    @pytest.mark.parametrize(
        "sdk_mapper_name",
        [
            "StartOfHourMapper",
            "StartOfDayMapper",
            "StartOfWeekMapper",
            "StartOfMonthMapper",
            "StartOfQuarterMapper",
            "StartOfYearMapper",
        ],
    )
    def test_sdk_constructor_invalid_timezone_raises_eagerly(self, sdk_mapper_name):
        """Passing an unknown timezone string must raise at construction time
        (via ``parse_timezone``), not silently fall back to UTC or fail later."""
        from pendulum.tz.exceptions import InvalidTimezone

        sdk_cls = getattr(sdk, sdk_mapper_name)
        with pytest.raises(InvalidTimezone):
            sdk_cls(timezone="Not/A/Real/Zone")
