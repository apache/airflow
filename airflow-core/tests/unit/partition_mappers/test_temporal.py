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

from datetime import datetime, timezone as dt_timezone

import pendulum
import pytest
from pendulum.tz.exceptions import InvalidTimezone

from airflow import sdk
from airflow.partition_mappers.base import RollupMapper
from airflow.partition_mappers.identity import IdentityMapper
from airflow.partition_mappers.temporal import (
    FanOutMapper,
    StartOfDayMapper,
    StartOfHourMapper,
    StartOfMonthMapper,
    StartOfQuarterMapper,
    StartOfWeekMapper,
    StartOfYearMapper,
    _BaseTemporalMapper,
    _compile_output_format_regex,
)
from airflow.partition_mappers.window import HourWindow, WeekWindow
from airflow.serialization.decoders import decode_partition_mapper
from airflow.serialization.encoders import encode_partition_mapper
from airflow.serialization.enums import Encoding


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

    def test_encode_upstream_accepts_aware_utc_decode(self):
        """`encode_upstream` accepts aware datetimes produced by `decode_downstream` with %z."""
        mapper = StartOfDayMapper(
            input_format="%Y-%m-%dT%H:%M:%S",
            output_format="%Y-%m-%dT%H%z",
        )
        decoded = mapper.decode_downstream("2024-03-13T00+0000")
        assert decoded.tzinfo is not None  # confirm strptime returned aware
        encoded = mapper.encode_upstream(decoded)
        assert encoded == "2024-03-13T00:00:00"

    def test_encode_upstream_normalises_non_utc_aware_decode(self):
        """%z with non-UTC offset must reflect the same instant in mapper timezone, not the offset's wall-clock."""
        mapper = StartOfDayMapper(
            timezone="UTC",
            input_format="%Y-%m-%dT%H:%M:%S",
            output_format="%Y-%m-%dT%H%z",
        )
        # "2024-03-13T00+0530" 代表 UTC 2024-03-12T18:30:00
        decoded = mapper.decode_downstream("2024-03-13T00+0530")
        assert decoded.tzinfo is not None
        encoded = mapper.encode_upstream(decoded)
        assert encoded == "2024-03-12T18:30:00"

    def test_start_of_week_decode_rejects_trailing_garbage(self):
        """malformed downstream_key with trailing garbage must raise, not silently match a substring."""
        mapper = StartOfWeekMapper()
        # output_format default is "%Y-%m-%d (W%V)"; trailing garbage must not silently parse.
        with pytest.raises(ValueError, match="could not parse"):
            mapper.decode_downstream("2024-03-11 (W11) trailing-garbage")

    def test_start_of_week_decode_rejects_leading_garbage(self):
        """malformed downstream_key with leading garbage must raise."""
        mapper = StartOfWeekMapper()
        with pytest.raises(ValueError, match="could not parse"):
            mapper.decode_downstream("garbage-prefix 2024-03-11 (W11)")

    def test_start_of_quarter_decode_rejects_trailing_garbage(self):
        """StartOfQuarterMapper.decode_downstream raises on trailing garbage."""
        mapper = StartOfQuarterMapper()
        # output_format default is "%Y-Q{quarter}"
        with pytest.raises(ValueError, match="could not parse"):
            mapper.decode_downstream("2024-Q1 trailing-garbage")


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
        sdk_cls = getattr(sdk, sdk_mapper_name)
        with pytest.raises(InvalidTimezone):
            sdk_cls(timezone="Not/A/Real/Zone")


class TestOutputFormatValidation:
    """Malformed output_format must fail fast at mapper construction, not as an opaque re.error inside the scheduler."""

    @pytest.mark.parametrize(
        ("output_format", "match"),
        [
            ("a{}b", "invalid placeholder"),
            ("%Y-{1bad}", "invalid placeholder"),
            ("%Y-%Y-%m-%d", "reuses directive"),
            ("%Y-Q{quarter}-{quarter}", "reuses placeholder"),
        ],
    )
    def test_quarter_mapper_rejects_malformed_format(self, output_format, match):
        with pytest.raises(ValueError, match=match):
            StartOfQuarterMapper(output_format=output_format)

    @pytest.mark.parametrize(
        ("output_format", "match"),
        [
            ("week-{}", "invalid placeholder"),
            ("%Y-%m-%d-%Y", "reuses directive"),
        ],
    )
    def test_week_mapper_rejects_malformed_format(self, output_format, match):
        with pytest.raises(ValueError, match=match):
            StartOfWeekMapper(output_format=output_format)

    def test_rejects_adjacent_default_pattern_placeholders(self):
        """``{a}{b}`` with default ``\\w+`` produces an unintuitive greedy split
        (e.g. on "foo123" yields a="foo12", b="3"), so the helper rejects it at
        compile time. Callers must insert a separator or narrow at least one
        pattern via ``placeholder_patterns``."""
        with pytest.raises(ValueError, match="adjacent default-pattern placeholders"):
            _compile_output_format_regex("{first}{last}")

    def test_adjacent_placeholders_allowed_when_one_is_narrowed(self):
        """Adjacent placeholders are fine if at least one has a narrowed pattern
        (the narrow side anchors the boundary, removing the greedy ambiguity)."""
        pattern = _compile_output_format_regex(
            "{year}{quarter}", placeholder_patterns={"year": r"\d{4}", "quarter": r"[1-4]"}
        )
        match = pattern.search("20242")
        assert match is not None
        assert match.group("year") == "2024"
        assert match.group("quarter") == "2"

    def test_separator_between_default_placeholders_is_allowed(self):
        """A literal separator between two default-pattern placeholders is enough
        to disambiguate (the literal terminates the greedy first group)."""
        pattern = _compile_output_format_regex("{first}-{last}")
        match = pattern.search("foo-bar")
        assert match is not None
        assert match.group("first") == "foo"
        assert match.group("last") == "bar"


class TestTemporalMapperDecodeNormalizeRoundTrip:
    """
    ``normalize(decode_downstream(to_downstream(dt)))`` must equal the anchor
    produced by ``normalize(dt)`` for every ``_BaseTemporalMapper`` subclass.

    This is the "Step 2" semantic guarantee: ``decode_downstream`` reconstructs
    the period-start, and ``normalize`` is idempotent, so the composed call
    used in ``_resolve_partition_date`` must not drift from the direct anchor.
    """

    SAMPLE_DT = datetime(2024, 3, 15, 10, 42, 35)

    @pytest.mark.parametrize(
        "mapper",
        [
            StartOfHourMapper(),
            StartOfDayMapper(),
            StartOfWeekMapper(),
            StartOfMonthMapper(),
            StartOfQuarterMapper(),
            StartOfYearMapper(),
        ],
    )
    def test_round_trip_anchor_is_stable(self, mapper: _BaseTemporalMapper):
        """``normalize(decode_downstream(to_downstream(dt)))`` == ``normalize(dt)``."""
        downstream_key = mapper.to_downstream(self.SAMPLE_DT.strftime(mapper.input_format))
        decoded = mapper.decode_downstream(downstream_key)
        round_tripped = mapper.normalize(decoded)
        direct_anchor = mapper.normalize(self.SAMPLE_DT)
        assert round_tripped == direct_anchor, (
            f"{type(mapper).__name__}: round-trip anchor {round_tripped!r} "
            f"differs from direct anchor {direct_anchor!r}"
        )

    @pytest.mark.parametrize(
        ("mapper", "expected_aware"),
        [
            # UTC mapper: UTC midnight stays at 00:00 UTC.
            (
                StartOfDayMapper(timezone="UTC"),
                datetime(2024, 3, 15, 0, 0, 0, tzinfo=dt_timezone.utc),
            ),
            # Non-UTC mapper: NY midnight (EDT = UTC-4) → 04:00 UTC.
            (
                StartOfDayMapper(timezone="America/New_York"),
                datetime(2024, 3, 15, 4, 0, 0, tzinfo=dt_timezone.utc),
            ),
        ],
    )
    def test_to_partition_date_uses_mapper_timezone(
        self, mapper: _BaseTemporalMapper, expected_aware: datetime
    ):
        """``to_partition_date`` localises the anchor with ``mapper._timezone``, not the global default."""
        downstream_key = mapper.to_downstream(self.SAMPLE_DT.strftime(mapper.input_format))
        aware = mapper.to_partition_date(downstream_key)
        # Convert to UTC for a timezone-neutral comparison.
        aware_utc = aware.astimezone(dt_timezone.utc)
        assert aware_utc == expected_aware, (
            f"{type(mapper).__name__} (tz={mapper._timezone}): "
            f"to_partition_date produced {aware_utc!r}, expected {expected_aware!r}"
        )


class TestToPartitionDateDelegation:
    """Composite mappers delegate ``to_partition_date`` to the child that owns the downstream key."""

    @pytest.mark.parametrize(
        ("mapper", "downstream_key", "expected"),
        [
            # RollupMapper (fan-in): downstream key is the upstream_mapper's format → it owns it.
            (
                RollupMapper(upstream_mapper=StartOfHourMapper(), window=HourWindow()),
                "2024-01-01T00",
                datetime(2024, 1, 1, 0, 0, 0, tzinfo=dt_timezone.utc),
            ),
            # FanOutMapper (fan-out): downstream keys are the downstream_mapper's format → it owns them.
            (
                FanOutMapper(upstream_mapper=StartOfWeekMapper(), window=WeekWindow()),
                "2024-01-16",
                datetime(2024, 1, 16, 0, 0, 0, tzinfo=dt_timezone.utc),
            ),
            # Non-temporal mapper → no anchor.
            (IdentityMapper(), "anything", None),
        ],
    )
    def test_to_partition_date(self, mapper, downstream_key, expected):
        assert mapper.to_partition_date(downstream_key) == expected


class TestTemporalMapperMaxDownstreamKeys:
    """Round-trip and zero-bloat tests for max_downstream_keys on temporal mappers."""

    def test_max_downstream_keys_encode_decode_roundtrip(self):
        """max_downstream_keys=5 survives encode_partition_mapper → decode_partition_mapper."""
        mapper = StartOfWeekMapper(max_downstream_keys=5)
        restored = decode_partition_mapper(encode_partition_mapper(mapper))
        assert restored.max_downstream_keys == 5

    def test_max_downstream_keys_absent_from_default_encoded_payload(self):
        """max_downstream_keys must NOT appear in the encoded payload when not set (zero-bloat contract)."""
        mapper = StartOfWeekMapper()
        encoded_var = encode_partition_mapper(mapper)[Encoding.VAR]
        assert "max_downstream_keys" not in encoded_var
