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

from airflow_breeze.utils.click_validators import validate_release_date


class TestPlannedReleaseDateValidation:
    """Test validation of planned release date format YYYY_MM_DD[_NN]."""

    @pytest.mark.parametrize(
        "date_value",
        [
            "2025-11-16",
            "2025-11-16_01",
            "2025-11-16_99",
            "2025-01-01",
            "2024-02-29",  # Leap year
            "2025-12-31",
        ],
    )
    def test_valid_date_formats(self, date_value):
        """Test that valid date formats are accepted."""
        # The function is a click callback, so we pass None for ctx and param
        result = validate_release_date(None, None, date_value)
        assert result == date_value

    def test_empty_value(self):
        """Test that empty value is accepted."""
        result = validate_release_date(None, None, "")
        assert result == ""

    @pytest.mark.parametrize(
        ("date_value", "error_pattern"),
        [
            ("2025_11_16", "YYYY-MM-DD"),  # Wrong separator (underscores)
            ("2025-11-16_1", "YYYY-MM-DD"),  # Wrong NN format (needs 2 digits)
            ("25-11-16", "YYYY-MM-DD"),  # Wrong year format (needs 4 digits)
            ("2025-13-16", "Invalid date"),  # Invalid month (13)
            ("2025-11-32", "Invalid date"),  # Invalid day (32)
            ("2025-02-30", "Invalid date"),  # Invalid date (Feb 30)
            ("2025-00-01", "Invalid date"),  # Invalid month (0)
            ("2025-11-00", "Invalid date"),  # Invalid day (0)
        ],
    )
    def test_invalid_date_formats(self, date_value, error_pattern):
        """Test that invalid date formats are rejected."""
        from click import BadParameter

        with pytest.raises(BadParameter, match=error_pattern):
            validate_release_date(None, None, date_value)
