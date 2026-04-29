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

import logging
from unittest.mock import MagicMock

import pytest

from airflow.providers.common.ai.toolsets.logging import LoggingToolset
from airflow.providers.common.ai.utils.policy_exposure import (
    LLMExposure,
    ResourceExposure,
    ToolsetExposure,
    _dedupe_reasons,
    classify_policy_risk,
    describe_toolset_exposure,
    unwrap_toolset,
)


class _UnknownToolset:
    id = "custom"


class _BaseToolset:
    def describe_policy_exposure(self) -> ToolsetExposure:
        return ToolsetExposure(
            toolset_type="BaseToolset",
            toolset_id="base",
            summary="base toolset",
        )


class _BrokenToolset:
    id = "broken"

    def describe_policy_exposure(self) -> ToolsetExposure:
        raise RuntimeError("boom")


def test_unwrap_toolset_returns_base_toolset():
    wrapped = _BaseToolset()
    toolset = LoggingToolset(wrapped=wrapped, logger=logging.getLogger(__name__))

    base_toolset = unwrap_toolset(toolset)

    assert base_toolset is wrapped


def test_describe_toolset_exposure_uses_base_toolset_for_wrappers():
    wrapped = MagicMock()
    wrapped.describe_policy_exposure.return_value = ToolsetExposure(
        toolset_type="WrappedToolset",
        toolset_id="wrapped",
        summary="wrapped toolset",
    )
    toolset = LoggingToolset(wrapped=wrapped, logger=logging.getLogger(__name__))

    exposure = describe_toolset_exposure(toolset)

    assert exposure.toolset_type == "WrappedToolset"
    assert exposure.toolset_id == "wrapped"


def test_describe_toolset_exposure_falls_back_for_unknown_toolset():
    exposure = describe_toolset_exposure(_UnknownToolset())

    assert exposure.toolset_type == "_UnknownToolset"
    assert "unknown toolset exposure" in exposure.risk_flags


def test_describe_toolset_exposure_falls_back_when_report_generation_fails():
    exposure = describe_toolset_exposure(_BrokenToolset())

    assert exposure.toolset_type == "_BrokenToolset"
    assert exposure.summary == "Toolset exposure details are unavailable because report generation failed."
    assert exposure.risk_flags == ["toolset exposure report failed"]


@pytest.mark.parametrize(
    ("llm", "toolsets", "runtime_notes", "expected_level"),
    [
        (
            LLMExposure(llm_conn_id="llm"),
            [
                ToolsetExposure(
                    toolset_type="Unknown", summary="unknown", risk_flags=["unknown toolset exposure"]
                )
            ],
            [],
            "high",
        ),
        (
            LLMExposure(llm_conn_id="llm"),
            [
                ToolsetExposure(
                    toolset_type="HookToolset",
                    summary="hook",
                    risk_flags=["potentially mutating hook methods exposed"],
                )
            ],
            [],
            "high",
        ),
        (
            LLMExposure(llm_conn_id="llm"),
            [
                ToolsetExposure(
                    toolset_type="WriteToolset",
                    summary="write",
                    resources=[ResourceExposure(category="database", name="db", access_mode="read_write")],
                )
            ],
            [],
            "high",
        ),
        (
            LLMExposure(llm_conn_id="llm"),
            [],
            ["tool logging enabled"],
            "low",
        ),
        (
            LLMExposure(llm_conn_id="llm"),
            [],
            [],
            "low",
        ),
    ],
)
def test_classify_policy_risk_returns_expected_level(llm, toolsets, runtime_notes, expected_level):
    risk = classify_policy_risk(llm=llm, toolsets=toolsets, runtime_notes=runtime_notes)

    assert risk.level == expected_level


def test_classify_policy_risk_does_not_duplicate_write_reasons():
    risk = classify_policy_risk(
        llm=LLMExposure(llm_conn_id="llm"),
        toolsets=[
            ToolsetExposure(
                toolset_type="SQLToolset",
                summary="write",
                resources=[ResourceExposure(category="database", name="db", access_mode="read_write")],
                risk_flags=["write-capable SQL access configured"],
            )
        ],
        runtime_notes=[],
    )

    assert risk.level == "high"
    assert risk.reasons == ["write-capable SQL access configured"]


def test_dedupe_reasons_preserves_order_and_drops_empty_strings():
    reasons = _dedupe_reasons(["first", "", "second", "first", "third", "second"])

    assert reasons == ["first", "second", "third"]
