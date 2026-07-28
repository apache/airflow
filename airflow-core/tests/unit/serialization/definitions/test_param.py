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

from airflow.serialization.definitions.param import SerializedParam


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
