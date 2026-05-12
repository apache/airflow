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

import json

from airflow.providers.common.ai.toolsets.dataquality.sql import SQLDQToolset
from airflow.providers.common.ai.utils.dataquality.models import DQCheckInput
from airflow.providers.common.ai.utils.dataquality.validation import ValidatorRegistry


def _email_row_validator(*, max_invalid_pct: float = 0.0):
    def _check(value: object) -> bool:
        if value in (None, ""):
            return False
        text = str(value)
        return "@" in text and "." in text

    _check._row_level = True  # type: ignore[attr-defined]
    _check._max_invalid_pct = max_invalid_pct  # type: ignore[attr-defined]
    return _check


class TestSQLDQToolsetRowLevel:
    def test_fixed_row_level_returns_structured_summary(self):
        ts = SQLDQToolset()
        ts.set_checks(
            [
                DQCheckInput(
                    name="email_format",
                    description="Validate emails",
                    validator=_email_row_validator(max_invalid_pct=0.34),
                )
            ]
        )

        payload = json.loads(
            ts._apply_validator(
                check_name="email_format",
                value=["ok@example.com", "bad-email", ""],
                validator_name="fixed",
                validator_args={},
            )
        )

        assert payload["passed"] is False
        assert "invalid_pct" in payload["reason"]
        assert payload["value"]["total"] == 3
        assert payload["value"]["invalid"] == 2
        assert payload["value"]["sample_size"] == 2

    def test_registry_row_level_uses_max_invalid_pct_threshold(self):
        registry = ValidatorRegistry()

        @registry.register("positive_only", row_level=True)
        def positive_only(*, max_invalid_pct: float = 0.0):
            def _check(value: object) -> bool:
                return isinstance(value, (int, float)) and value > 0

            return _check

        ts = SQLDQToolset(validator_registry=registry)
        ts.set_checks([DQCheckInput(name="metric_rows", description="Row-level numeric checks")])

        payload = json.loads(
            ts._apply_validator(
                check_name="metric_rows",
                value=[1, -2, 3],
                validator_name="positive_only",
                validator_args={"max_invalid_pct": 0.34},
            )
        )

        assert payload["passed"] is True
        assert payload["value"]["invalid"] == 1
        assert payload["value"]["invalid_pct"] == 1 / 3

    def test_registry_row_level_honors_factory_default_threshold(self):
        registry = ValidatorRegistry()

        @registry.register("non_negative", row_level=True)
        def non_negative(*, max_invalid_pct: float = 0.34):
            def _check(value: object) -> bool:
                return isinstance(value, (int, float)) and value >= 0

            return _check

        ts = SQLDQToolset(validator_registry=registry)
        ts.set_checks([DQCheckInput(name="metric_rows", description="Row-level numeric checks")])

        payload = json.loads(
            ts._apply_validator(
                check_name="metric_rows",
                value=[1, -2, 3],
                validator_name="non_negative",
                validator_args={},
            )
        )

        assert payload["passed"] is True
        assert payload["value"]["invalid"] == 1
        assert payload["value"]["invalid_pct"] == 1 / 3

    def test_row_level_wraps_scalar_values(self):
        ts = SQLDQToolset()
        ts.set_checks(
            [
                DQCheckInput(
                    name="email_format",
                    description="Validate emails",
                    validator=_email_row_validator(max_invalid_pct=0.0),
                )
            ]
        )

        payload = json.loads(
            ts._apply_validator(
                check_name="email_format",
                value="ok@example.com",
                validator_name="fixed",
                validator_args={},
            )
        )

        assert payload["passed"] is True
        assert payload["value"]["total"] == 1
        assert payload["value"]["invalid"] == 0
