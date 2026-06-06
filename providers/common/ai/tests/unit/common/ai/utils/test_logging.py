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

from pydantic import BaseModel

from airflow.providers.common.ai.hooks.base import AgentRunResult, AgentUsage
from airflow.providers.common.ai.utils.logging import (
    _log_output_debug,
    log_run_summary,
)


def _make_result(model_name="gpt-5", tool_names=None, usage_kwargs=None):
    usage_kwargs = usage_kwargs or {
        "requests": 4,
        "tool_calls": 3,
        "input_tokens": 2847,
        "output_tokens": 512,
        "total_tokens": 3359,
    }
    return AgentRunResult(
        output="test output",
        model_name=model_name,
        usage=AgentUsage(**usage_kwargs),
        tool_names=tool_names,
    )


class TestLogRunSummary:
    def test_logs_usage(self, caplog):
        logger = logging.getLogger("test.log_run_summary")
        result = _make_result()

        with caplog.at_level(logging.INFO, logger="test.log_run_summary"):
            log_run_summary(logger, result)

        records = [r for r in caplog.records if r.name == "test.log_run_summary"]
        summary_line = records[0].message
        assert summary_line.startswith("::group::")
        assert "model=gpt-5" in summary_line
        assert "requests=4" in summary_line
        assert "tool_calls=3" in summary_line
        assert "input_tokens=2847" in summary_line
        assert "output_tokens=512" in summary_line
        assert "total_tokens=3359" in summary_line
        assert records[-1].message == "::endgroup::"

    def test_logs_tool_sequence(self, caplog):
        logger = logging.getLogger("test.log_run_summary")
        result = _make_result(tool_names=["list_tables", "get_schema", "query"])

        with caplog.at_level(logging.INFO, logger="test.log_run_summary"):
            log_run_summary(logger, result)

        records = [r for r in caplog.records if r.name == "test.log_run_summary"]
        tool_line = records[1].message
        assert tool_line == "Tool call sequence: list_tables -> get_schema -> query"
        assert records[-1].message == "::endgroup::"

    def test_no_tools_skips_sequence_line(self, caplog):
        logger = logging.getLogger("test.log_run_summary")
        result = _make_result(tool_names=None)

        with caplog.at_level(logging.INFO, logger="test.log_run_summary"):
            log_run_summary(logger, result)

        records = [r for r in caplog.records if r.name == "test.log_run_summary"]
        assert len(records) == 2  # summary line + endgroup (no tool sequence)
        assert records[-1].message == "::endgroup::"

    def test_logs_without_usage(self, caplog):
        logger = logging.getLogger("test.log_run_summary")
        result = AgentRunResult(output="something", model_name="my-model", usage=None)

        with caplog.at_level(logging.INFO, logger="test.log_run_summary"):
            log_run_summary(logger, result)

        records = [r for r in caplog.records if r.name == "test.log_run_summary"]
        assert "model=my-model" in records[0].message
        assert records[-1].message == "::endgroup::"


class TestLogOutputDebug:
    def test_logs_string_output(self, caplog):
        logger = logging.getLogger("test.output_debug")
        with caplog.at_level(logging.DEBUG, logger="test.output_debug"):
            _log_output_debug(logger, "Hello world")

        debug_records = [r for r in caplog.records if r.levelno == logging.DEBUG]
        assert any("Output: 'Hello world'" in r.message for r in debug_records)

    def test_logs_pydantic_model_dump(self, caplog):
        class Info(BaseModel):
            name: str

        logger = logging.getLogger("test.output_debug")
        with caplog.at_level(logging.DEBUG, logger="test.output_debug"):
            _log_output_debug(logger, Info(name="Alice"))

        debug_records = [r for r in caplog.records if r.levelno == logging.DEBUG]
        assert any("'name': 'Alice'" in r.message for r in debug_records)

    def test_truncates_long_output(self, caplog):
        logger = logging.getLogger("test.output_debug")
        long_text = "x" * 1000
        with caplog.at_level(logging.DEBUG, logger="test.output_debug"):
            _log_output_debug(logger, long_text)

        debug_records = [r for r in caplog.records if r.levelno == logging.DEBUG]
        assert any(r.message.endswith("...") for r in debug_records)

    def test_skipped_when_debug_disabled(self, caplog):
        logger = logging.getLogger("test.output_debug")
        with caplog.at_level(logging.INFO, logger="test.output_debug"):
            _log_output_debug(logger, "should not appear")

        debug_records = [r for r in caplog.records if r.levelno == logging.DEBUG]
        assert len(debug_records) == 0
