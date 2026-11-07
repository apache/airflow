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

from airflow.exceptions import ParamValidationError
from airflow.serialization.definitions.param import SerializedParam, SerializedParamsDict


class TestSerializedParam:
    def test_resolve_no_schema(self):
        """Test resolve when no schema is provided."""
        assert SerializedParam(default=42).resolve() == 42

    @pytest.mark.parametrize(
        "duration",
        [
            pytest.param("PT15M", id="minutes-only"),
            pytest.param("P1Y", id="years-only"),
            pytest.param("P1W", id="weeks-only"),
            pytest.param("P1D", id="days-only"),
            pytest.param("PT1H", id="hours-only"),
            pytest.param("PT30S", id="seconds-only"),
            pytest.param("P1DT2H", id="days-and-hours"),
            pytest.param("P1Y2M3DT4H5M6S", id="full-duration"),
            pytest.param("PT1.5H", id="fractional-hours-dot"),
        ],
    )
    def test_string_duration_format(self, duration):
        """Test valid ISO 8601 duration strings."""
        assert SerializedParam(duration, type="string", format="duration").resolve(raises=True) == duration

    @pytest.mark.parametrize(
        "duration",
        [
            pytest.param("P", id="bare-P"),
            pytest.param("PT", id="bare-PT"),
            pytest.param("invalid", id="plain-text"),
            pytest.param("15M", id="missing-P-prefix"),
            pytest.param("1Y2M", id="no-P-prefix"),
        ],
    )
    def test_string_duration_format_error(self, duration):
        """Test invalid ISO 8601 duration strings."""
        with pytest.raises(Exception, match="is not a 'duration'"):
            SerializedParam(duration, type="string", format="duration").resolve(raises=True)


AFTER_START = {"formatExclusiveMinimum": {"$data": "1/start"}}
ON_OR_AFTER_START = {"formatMinimum": {"$data": "1/start"}}


def _date_range(start, end, bound, fmt):
    nullable = {"type": ["null", "string"], "format": fmt}
    return SerializedParamsDict(
        {"start": SerializedParam(start, **nullable), "end": SerializedParam(end, **nullable, **bound)}
    )


class TestSerializedParamsDictDateBounds:
    @pytest.mark.parametrize(
        ("start", "end", "bound", "fmt"),
        [
            pytest.param("2026-10-01", "2026-10-01", ON_OR_AFTER_START, "date", id="on-or-after-equal"),
            pytest.param(
                "2026-10-01T10:00:00+02:00",
                "2026-10-01T09:00:00+00:00",
                AFTER_START,
                "date-time",
                id="later-in-utc",
            ),
            pytest.param("2026-10-02", None, AFTER_START, "date", id="end-empty"),
            pytest.param(None, "2026-10-01", AFTER_START, "date", id="start-empty"),
        ],
    )
    def test_validate_passes(self, start, end, bound, fmt):
        assert _date_range(start, end, bound, fmt).validate() == {"start": start, "end": end}

    @pytest.mark.parametrize(
        ("start", "end", "bound", "fmt", "match"),
        [
            pytest.param(
                "2026-10-01T00:00:00+00:00",
                "2026-10-01T00:00:00+00:00",
                AFTER_START,
                "date-time",
                "must be after start",
                id="after-equal",
            ),
            pytest.param(
                "2026-10-02",
                "2026-10-01",
                ON_OR_AFTER_START,
                "date",
                "must be on or after start",
                id="on-or-after-before",
            ),
            pytest.param(
                "2026-10-01",
                "2026-10-02",
                {"formatMinimum": "2026-10-01"},
                "date",
                "formatMinimum must be",
                id="literal-bound",
            ),
            pytest.param(
                "2026-10-01",
                "2026-10-02",
                {"formatMinimum": {"$data": "1/missing"}},
                "date",
                "formatMinimum must be",
                id="unknown-param",
            ),
        ],
    )
    def test_validate_fails(self, start, end, bound, fmt, match):
        with pytest.raises(ParamValidationError, match=f"Invalid input for param end: .*{match}"):
            _date_range(start, end, bound, fmt).validate()

    def test_validate_fails_on_unparseable_bound(self):
        params = SerializedParamsDict(
            {
                "start": SerializedParam("not-a-date"),
                "end": SerializedParam("2026-10-02", type="string", format="date", **ON_OR_AFTER_START),
            }
        )
        with pytest.raises(ParamValidationError, match="Invalid input for param end: Invalid isoformat"):
            params.validate()
