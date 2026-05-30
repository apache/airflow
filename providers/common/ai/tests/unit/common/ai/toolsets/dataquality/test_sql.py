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

import asyncio
import json
from unittest.mock import MagicMock

from airflow.providers.common.ai.toolsets.dataquality.sql import SQLDQToolset
from airflow.providers.common.ai.utils.dataquality.models import DQCheckInput
from airflow.providers.common.ai.utils.dataquality.validation import ValidatorRegistry, null_pct_check


class TestSQLDQToolsetInit:
    def test_id(self):
        ts = SQLDQToolset()
        assert ts.id == "dq-sql"

    def test_output_mode_is_execute(self):
        ts = SQLDQToolset()
        assert ts.output_mode == "execute"

    def test_custom_registry_is_stored(self):
        registry = ValidatorRegistry()
        ts = SQLDQToolset(validator_registry=registry)
        assert ts._registry is registry


class TestSQLDQToolsetGetTools:
    def test_get_tools_includes_list_validators_and_apply_validator(self):
        ts = SQLDQToolset()
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        assert "list_checks" in tools
        assert "list_validators" in tools
        assert "apply_validator" in tools

    def test_planning_mode_hides_apply_validator(self):
        ts = SQLDQToolset()
        ts._planning_mode = True
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        assert "apply_validator" not in tools
        assert "list_validators" in tools


class TestSQLDQToolsetListValidators:
    def test_lists_registered_validators(self):
        ts = SQLDQToolset()
        result = json.loads(ts._list_validators())
        names = [v["name"] for v in result]
        assert "null_pct_check" in names
        assert "row_count_check" in names

    def test_validator_entry_has_required_fields(self):
        ts = SQLDQToolset()
        result = json.loads(ts._list_validators())
        entry = next(v for v in result if v["name"] == "null_pct_check")
        assert "category" in entry
        assert "parameters" in entry
        assert "description" in entry


class TestSQLDQToolsetApplyValidator:
    def test_scalar_validator_passes(self):
        ts = SQLDQToolset()
        ts.set_checks([DQCheckInput(name="nulls", description="null check")])
        result = json.loads(ts._apply_validator("nulls", 0.01, "null_pct_check", {"max_pct": 0.05}))
        assert result["passed"] is True
        assert result["check_name"] == "nulls"

    def test_scalar_validator_fails(self):
        ts = SQLDQToolset()
        ts.set_checks([DQCheckInput(name="nulls", description="null check")])
        result = json.loads(ts._apply_validator("nulls", 0.10, "null_pct_check", {"max_pct": 0.05}))
        assert result["passed"] is False

    def test_none_validator_passes(self):
        ts = SQLDQToolset()
        ts.set_checks([DQCheckInput(name="c", description="d")])
        result = json.loads(ts._apply_validator("c", 42, "none", {}))
        assert result["passed"] is True
        assert result["reason"] == "no validator"

    def test_unknown_validator_returns_failure(self):
        ts = SQLDQToolset()
        ts.set_checks([DQCheckInput(name="c", description="d")])
        result = json.loads(ts._apply_validator("c", 0.0, "nonexistent_validator", {}))
        assert result["passed"] is False
        assert "not registered" in result["reason"]

    def test_fixed_validator_is_used(self):
        ts = SQLDQToolset()
        ts.set_checks([DQCheckInput(name="c", description="d", validator=null_pct_check(max_pct=0.05))])
        result = json.loads(ts._apply_validator("c", 0.02, "fixed", {}))
        assert result["passed"] is True

    def test_row_level_validator_returns_summary(self):
        def _row_validator(*, max_invalid_pct: float = 0.0):
            def _check(v: object) -> bool:
                return v is not None and str(v) != ""

            _check._row_level = True  # type: ignore[attr-defined]
            _check._max_invalid_pct = max_invalid_pct  # type: ignore[attr-defined]
            return _check

        ts = SQLDQToolset()
        ts.set_checks(
            [DQCheckInput(name="not_empty", description="d", validator=_row_validator(max_invalid_pct=0.5))]
        )
        result = json.loads(ts._apply_validator("not_empty", ["a", "", "b", None], "fixed", {}))
        assert "passed" in result
        assert "value" in result
        assert result["value"]["total"] == 4
        assert result["value"]["invalid"] == 2


class TestSQLDQToolsetCallTool:
    def test_call_tool_list_validators(self):
        ts = SQLDQToolset()
        result = asyncio.run(ts.call_tool("list_validators", {}, ctx=MagicMock(), tool=MagicMock()))
        data = json.loads(result)
        assert isinstance(data, list)

    def test_call_tool_apply_validator(self):
        ts = SQLDQToolset()
        ts.set_checks([DQCheckInput(name="n", description="d")])
        result = asyncio.run(
            ts.call_tool(
                "apply_validator",
                {
                    "check_name": "n",
                    "value": 0.0,
                    "validator_name": "null_pct_check",
                    "validator_args": {"max_pct": 0.05},
                },
                ctx=MagicMock(),
                tool=MagicMock(),
            )
        )
        data = json.loads(result)
        assert data["passed"] is True
