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

import pytest

from airflow_breeze.utils.llm_utils import (
    MAX_DIFF_CHARS,
    MAX_PR_BODY_CHARS,
    _build_review_user_message,
    _build_user_message,
    _extract_json,
    _fix_json_with_llm,
    _parse_response,
    _parse_review_response,
    _read_file_section,
    _resolve_cli_provider,
)


# ===========================================================================
# _read_file_section
# ===========================================================================
class TestReadFileSection:
    def test_reads_section_between_markers(self, tmp_path):
        f = tmp_path / "doc.rst"
        f.write_text("Some header\nSTART MARKER\nline one\nline two\nEND MARKER\ntrailing text\n")
        result = _read_file_section(tmp_path, "doc.rst", "START MARKER", "END MARKER")
        assert result == "line one\nline two"

    def test_returns_empty_for_missing_file(self, tmp_path):
        assert _read_file_section(tmp_path, "nonexistent.rst", "START", "END") == ""

    def test_returns_empty_when_start_marker_not_found(self, tmp_path):
        f = tmp_path / "doc.rst"
        f.write_text("no markers here\n")
        assert _read_file_section(tmp_path, "doc.rst", "START", "END") == ""

    def test_captures_to_end_when_end_marker_missing(self, tmp_path):
        f = tmp_path / "doc.rst"
        f.write_text("START\ncaptured line\nanother line\n")
        result = _read_file_section(tmp_path, "doc.rst", "START", "NONEXISTENT_END")
        assert "captured line" in result
        assert "another line" in result

    def test_start_marker_line_is_excluded(self, tmp_path):
        f = tmp_path / "doc.rst"
        f.write_text("START\ncontent\nEND\n")
        result = _read_file_section(tmp_path, "doc.rst", "START", "END")
        assert "START" not in result
        assert result == "content"


# ===========================================================================
# _build_user_message
# ===========================================================================
class TestBuildUserMessage:
    def test_basic_message(self):
        msg = _build_user_message(42, "Fix bug", "This fixes a bug", "All passing")
        assert "PR #42" in msg
        assert "Fix bug" in msg
        assert "This fixes a bug" in msg
        assert "All passing" in msg

    def test_empty_body(self):
        msg = _build_user_message(1, "Title", "", "checks")
        assert "(empty)" in msg

    def test_truncates_long_body(self):
        long_body = "x" * (MAX_PR_BODY_CHARS + 100)
        msg = _build_user_message(1, "Title", long_body, "checks")
        assert "... (truncated)" in msg
        # The body portion should be capped
        assert len(long_body) > MAX_PR_BODY_CHARS

    def test_short_body_not_truncated(self):
        msg = _build_user_message(1, "Title", "short body", "checks")
        assert "truncated" not in msg


# ===========================================================================
# _extract_json — the most complex pure function
# ===========================================================================
class TestExtractJson:
    def test_plain_json(self):
        raw = '{"should_flag": true, "violations": [], "summary": "bad"}'
        assert json.loads(_extract_json(raw)) == json.loads(raw)

    def test_json_in_markdown_fences(self):
        raw = 'Some text\n```json\n{"key": "value"}\n```\nmore text'
        result = _extract_json(raw)
        assert json.loads(result) == {"key": "value"}

    def test_json_with_prose_before(self):
        raw = 'Here is the assessment:\n\n{"should_flag": false, "summary": "ok"}'
        result = _extract_json(raw)
        parsed = json.loads(result)
        assert parsed["should_flag"] is False

    def test_json_with_nested_braces_in_strings(self):
        raw = '{"summary": "contains {braces} inside", "count": 1}'
        result = _extract_json(raw)
        parsed = json.loads(result)
        assert parsed["summary"] == "contains {braces} inside"

    def test_json_with_escaped_quotes(self):
        raw = '{"summary": "he said \\"hello\\"", "count": 1}'
        result = _extract_json(raw)
        parsed = json.loads(result)
        assert "hello" in parsed["summary"]

    def test_no_json_returns_stripped(self):
        raw = "no json here at all"
        assert _extract_json(raw) == "no json here at all"

    def test_json_in_fences_with_backticks_inside(self):
        # JSON that contains ``` inside a string value (e.g., code suggestion)
        inner_json = '{"summary": "use ```code``` block", "count": 1}'
        raw = f"```json\n{inner_json}\n```"
        result = _extract_json(raw)
        parsed = json.loads(result)
        assert "code" in parsed["summary"]

    def test_json_with_trailing_text_after_brace(self):
        raw = 'prefix {"a": 1} suffix'
        result = _extract_json(raw)
        assert json.loads(result) == {"a": 1}

    def test_deeply_nested_json(self):
        raw = '{"outer": {"inner": {"deep": true}}}'
        result = _extract_json(raw)
        parsed = json.loads(result)
        assert parsed["outer"]["inner"]["deep"] is True


# ===========================================================================
# _parse_response
# ===========================================================================
class TestParseResponse:
    def test_valid_response(self):
        raw = json.dumps(
            {
                "should_flag": True,
                "should_report": False,
                "violations": [{"category": "quality", "explanation": "Low effort", "severity": "error"}],
                "summary": "Low quality PR",
            }
        )
        result = _parse_response(raw)
        assert result.should_flag is True
        assert result.should_report is False
        assert len(result.violations) == 1
        assert result.violations[0].category == "quality"
        assert result.violations[0].severity == "error"
        assert result.summary == "Low quality PR"

    def test_should_report_forces_flag(self):
        raw = json.dumps(
            {
                "should_flag": False,
                "should_report": True,
                "violations": [],
                "summary": "Spam PR",
            }
        )
        result = _parse_response(raw)
        assert result.should_flag is True
        assert result.should_report is True

    def test_missing_fields_use_defaults(self):
        raw = json.dumps({"violations": []})
        result = _parse_response(raw)
        assert result.should_flag is False
        assert result.should_report is False
        assert result.summary == ""
        assert result.violations == []

    def test_empty_violations(self):
        raw = json.dumps({"should_flag": False, "violations": [], "summary": "Looks good"})
        result = _parse_response(raw)
        assert result.violations == []

    def test_multiple_violations(self):
        raw = json.dumps(
            {
                "should_flag": True,
                "violations": [
                    {"category": "a", "explanation": "e1", "severity": "error"},
                    {"category": "b", "explanation": "e2", "severity": "warning"},
                ],
                "summary": "Issues found",
            }
        )
        result = _parse_response(raw)
        assert len(result.violations) == 2
        assert result.violations[0].severity == "error"
        assert result.violations[1].severity == "warning"

    def test_violation_missing_fields(self):
        raw = json.dumps(
            {
                "violations": [{"category": "test"}],
            }
        )
        result = _parse_response(raw)
        assert result.violations[0].explanation == ""
        assert result.violations[0].severity == "warning"

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            _parse_response("not json at all {{{")

    def test_response_in_markdown_fences(self):
        raw = '```json\n{"should_flag": false, "violations": [], "summary": "ok"}\n```'
        result = _parse_response(raw)
        assert result.should_flag is False


# ===========================================================================
# _resolve_cli_provider
# ===========================================================================
class TestResolveCliProvider:
    def test_claude_model(self):
        provider, model = _resolve_cli_provider("claude/sonnet")
        assert provider == "claude"
        assert model == "sonnet"

    def test_codex_model(self):
        provider, model = _resolve_cli_provider("codex/gpt-5.3-codex")
        assert provider == "codex"
        assert model == "gpt-5.3-codex"

    def test_model_with_multiple_slashes(self):
        provider, model = _resolve_cli_provider("claude/claude-3/opus")
        assert provider == "claude"
        assert model == "claude-3/opus"

    def test_missing_slash_exits(self):
        with pytest.raises(SystemExit) as exc_info:
            _resolve_cli_provider("no-slash-here")
        assert exc_info.value.code == 1


# ===========================================================================
# _build_review_user_message
# ===========================================================================
class TestBuildReviewUserMessage:
    def test_basic_message(self):
        msg = _build_review_user_message(10, "Refactor X", "Description", "diff content here")
        assert "PR #10" in msg
        assert "Refactor X" in msg
        assert "Description" in msg
        assert "diff content here" in msg

    def test_empty_body_and_diff(self):
        msg = _build_review_user_message(1, "Title", "", "")
        assert "(empty)" in msg

    def test_truncates_long_diff(self):
        long_diff = "+" * (MAX_DIFF_CHARS + 100)
        msg = _build_review_user_message(1, "Title", "body", long_diff)
        assert "... (truncated)" in msg

    def test_truncates_long_body(self):
        long_body = "x" * (MAX_PR_BODY_CHARS + 100)
        msg = _build_review_user_message(1, "Title", long_body, "diff")
        assert "... (truncated)" in msg


# ===========================================================================
# _parse_review_response
# ===========================================================================
class TestParseReviewResponse:
    def test_valid_review(self):
        raw = json.dumps(
            {
                "summary": "Good PR",
                "overall_assessment": "APPROVE",
                "overall_comment": "LGTM",
                "comments": [{"path": "foo.py", "line": 10, "body": "Nit", "category": "style"}],
            }
        )
        result = _parse_review_response(raw)
        assert result["overall_assessment"] == "APPROVE"
        assert len(result["comments"]) == 1

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            _parse_review_response("broken json }{")


# ===========================================================================
# _fix_json_with_llm
# ===========================================================================
class TestFixJsonWithLlm:
    def test_successful_fix(self):
        # Mock caller that returns valid JSON
        def mock_caller(model, system_prompt, user_message):
            return '{"should_flag": false, "violations": [], "summary": "fixed"}'

        result = _fix_json_with_llm('{"broken json', mock_caller, "test-model", "parse error")
        assert result["should_flag"] is False
        assert result["summary"] == "fixed"

    def test_fix_still_broken_raises(self):
        def mock_caller(model, system_prompt, user_message):
            return "still broken {{{{"

        with pytest.raises(json.JSONDecodeError):
            _fix_json_with_llm('{"broken', mock_caller, "test-model", "error")

    def test_caller_receives_context(self):
        received = {}

        def mock_caller(model, system_prompt, user_message):
            received["model"] = model
            received["system_prompt"] = system_prompt
            received["user_message"] = user_message
            return '{"key": "value"}'

        _fix_json_with_llm('{"broken', mock_caller, "my-model", "some parse error")
        assert received["model"] == "my-model"
        assert "JSON repair" in received["system_prompt"]
        assert "some parse error" in received["user_message"]
        assert '{"broken' in received["user_message"]
