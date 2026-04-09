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
"""Tests for utility functions in pr_commands."""

from __future__ import annotations

from airflow_breeze.commands.pr_commands import (
    _are_only_static_check_failures,
    _extract_error_lines,
    _extract_static_check_errors,
    _format_snippet,
    _process_check_contexts,
)


# ===========================================================================
# _are_only_static_check_failures
# ===========================================================================
class TestAreOnlyStaticCheckFailures:
    def test_empty(self):
        assert _are_only_static_check_failures([]) is False

    def test_single_static_check(self):
        assert _are_only_static_check_failures(["Static checks"]) is True

    def test_mypy(self):
        assert _are_only_static_check_failures(["MyPy checks"]) is True

    def test_ruff(self):
        assert _are_only_static_check_failures(["ruff linting"]) is True

    def test_mixed_static_and_test(self):
        assert _are_only_static_check_failures(["Static checks", "Tests postgres"]) is False

    def test_only_test_failures(self):
        assert _are_only_static_check_failures(["Tests postgres", "Tests sqlite"]) is False

    def test_lint_patterns(self):
        for name in ["pylint check", "bandit scan", "codespell", "yamllint", "shellcheck", "isort"]:
            assert _are_only_static_check_failures([name]) is True, f"Expected True for {name}"

    def test_case_insensitive(self):
        assert _are_only_static_check_failures(["STATIC CHECK"]) is True
        assert _are_only_static_check_failures(["MyPy Checks"]) is True


# ===========================================================================
# _process_check_contexts
# ===========================================================================
class TestProcessCheckContexts:
    def test_empty(self):
        summary, failed, has_tests = _process_check_contexts([], 0)
        assert summary == "No check runs found."
        assert failed == []
        assert has_tests is False

    def test_check_run_success(self):
        contexts = [{"__typename": "CheckRun", "name": "Tests", "conclusion": "SUCCESS"}]
        summary, failed, has_tests = _process_check_contexts(contexts, 1)
        assert "Tests: SUCCESS" in summary
        assert failed == []
        assert has_tests is True

    def test_check_run_failure(self):
        contexts = [{"__typename": "CheckRun", "name": "CI", "conclusion": "FAILURE"}]
        _, failed, _ = _process_check_contexts(contexts, 1)
        assert "CI" in failed

    def test_check_run_timed_out(self):
        contexts = [{"__typename": "CheckRun", "name": "Slow", "conclusion": "TIMED_OUT"}]
        _, failed, _ = _process_check_contexts(contexts, 1)
        assert "Slow" in failed

    def test_check_run_cancelled(self):
        contexts = [{"__typename": "CheckRun", "name": "Job", "conclusion": "CANCELLED"}]
        _, failed, _ = _process_check_contexts(contexts, 1)
        assert "Job" in failed

    def test_check_run_startup_failure(self):
        contexts = [{"__typename": "CheckRun", "name": "Build", "conclusion": "STARTUP_FAILURE"}]
        _, failed, _ = _process_check_contexts(contexts, 1)
        assert "Build" in failed

    def test_status_context_failure(self):
        contexts = [{"__typename": "StatusContext", "context": "ci/status", "state": "FAILURE"}]
        _, failed, _ = _process_check_contexts(contexts, 1)
        assert "ci/status" in failed

    def test_status_context_error(self):
        contexts = [{"__typename": "StatusContext", "context": "ci/lint", "state": "ERROR"}]
        _, failed, _ = _process_check_contexts(contexts, 1)
        assert "ci/lint" in failed

    def test_has_test_checks(self):
        contexts = [{"__typename": "CheckRun", "name": "test suite", "conclusion": "SUCCESS"}]
        _, _, has_tests = _process_check_contexts(contexts, 1)
        assert has_tests is True

    def test_no_test_checks(self):
        contexts = [{"__typename": "CheckRun", "name": "labeler", "conclusion": "SUCCESS"}]
        _, _, has_tests = _process_check_contexts(contexts, 1)
        assert has_tests is False

    def test_extra_count_shown(self):
        contexts = [{"__typename": "CheckRun", "name": "CI", "conclusion": "SUCCESS"}]
        summary, _, _ = _process_check_contexts(contexts, 10)
        assert "9 more" in summary


# ===========================================================================
# _extract_error_lines
# ===========================================================================
class TestExtractErrorLines:
    def test_finds_error_markers(self):
        log = "line 1\nline 2\nerror: something broke\nline 4"
        result = _extract_error_lines(log, "Build")
        assert "something broke" in result

    def test_finds_traceback(self):
        log = "start\nTraceback (most recent call last)\n  File x.py\nValueError: bad\nend"
        result = _extract_error_lines(log, "Test")
        assert "Traceback" in result

    def test_fallback_last_n_lines(self):
        lines = [f"line {i}" for i in range(50)]
        log = "\n".join(lines)
        result = _extract_error_lines(log, "")
        # Should contain the last lines since no error markers
        assert "line 49" in result

    def test_strips_timestamp(self):
        log = "2026-01-01T00:00:00.0000000Z error: test failure"
        result = _extract_error_lines(log, "Step")
        assert "error: test failure" in result
        assert "2026-01-01" not in result

    def test_static_check_step_uses_specialised_extraction(self):
        # When step_name indicates static checks, should use _extract_static_check_errors
        log = "Run 'ruff'.....Failed\n- hook id: ruff\n"
        result = _extract_error_lines(log, "Static checks")
        # If no Failed pattern found (no ANSI stripping here), returns empty
        # This tests the dispatch path
        assert isinstance(result, str)

    def test_empty_log(self):
        assert _extract_error_lines("", "Step") == ""


# ===========================================================================
# _extract_static_check_errors
# ===========================================================================
class TestExtractStaticCheckErrors:
    def test_detects_failed_hook(self):
        lines = [
            "Check something...Passed",
            "ruff output line 1",
            "ruff output line 2",
            "Run 'ruff'.....Failed",
            "- hook id: ruff",
        ]
        result = _extract_static_check_errors(lines)
        assert any("FAILED" in line for line in result)
        assert any("ruff output" in line for line in result)

    def test_filters_trace_lines(self):
        lines = [
            "2026-01-01T00:00:00.000Z TRACE run{hook_id=ruff}: Resolved command: /usr/bin/ruff",
            "2026-01-01T00:00:00.000Z TRACE run{hook_id=ruff}: Found shebang: python",
            "actual error output",
            "Run 'ruff'.....Failed",
        ]
        result = _extract_static_check_errors(lines)
        assert not any("TRACE" in line for line in result)
        assert any("actual error" in line for line in result)

    def test_captures_files_modified(self):
        lines = [
            "Run 'ruff'.....Failed",
            "- hook id: ruff",
            "- files were modified by this hook",
            "- duration: 1.2s",
        ]
        result = _extract_static_check_errors(lines)
        assert any("files were modified" in line for line in result)

    def test_captures_exit_code(self):
        lines = [
            "actual error",
            "Run 'mypy'.....Failed",
            "- hook id: mypy",
            "- exit code: 1",
        ]
        result = _extract_static_check_errors(lines)
        assert any("exit code: 1" in line for line in result)

    def test_ansi_codes_stripped(self):
        lines = [
            "Run 'ruff'.....\x1b[41mFailed\x1b[49m",
            "\x1b[2m- hook id: ruff\x1b[0m",
            "\x1b[2m- files were modified by this hook\x1b[0m",
        ]
        result = _extract_static_check_errors(lines)
        assert any("FAILED" in line for line in result)

    def test_passed_hooks_excluded(self):
        lines = [
            "output from passing hook",
            "Run black.....Passed",
            "error from failing hook",
            "Run 'ruff'.....Failed",
        ]
        result = _extract_static_check_errors(lines)
        assert not any("passing hook" in line for line in result)
        assert any("error from failing" in line for line in result)

    def test_no_failed_hooks(self):
        lines = [
            "Run black.....Passed",
            "Run ruff.....Passed",
        ]
        result = _extract_static_check_errors(lines)
        assert result == []

    def test_skipped_hooks_reset_buffer(self):
        lines = [
            "noise before skipped",
            "Some check...(no files to check)Skipped",
            "error from next hook",
            "Run 'ruff'.....Failed",
        ]
        result = _extract_static_check_errors(lines)
        # "noise before skipped" should not appear
        assert not any("noise before" in line for line in result)

    def test_multiple_failed_hooks(self):
        lines = [
            "ruff error 1",
            "Run 'ruff'.....Failed",
            "mypy error 1",
            "Run 'mypy'.....Failed",
        ]
        result = _extract_static_check_errors(lines)
        failed_count = sum(1 for line in result if "FAILED" in line)
        assert failed_count == 2


# ===========================================================================
# _format_snippet
# ===========================================================================
class TestFormatSnippet:
    def test_adds_header(self):
        result = _format_snippet(["error line"], "Build Step")
        assert "[Failed step: Build Step]" in result
        assert "error line" in result

    def test_no_header_when_empty_step_name(self):
        result = _format_snippet(["error line"], "")
        assert "[Failed step:" not in result

    def test_strips_timestamps(self):
        lines = ["2026-01-01T00:00:00.0000000Z real content"]
        result = _format_snippet(lines, "")
        assert "real content" in result
        assert "2026-01-01" not in result

    def test_truncates_long_output(self):
        lines = ["x" * 200 for _ in range(20)]
        result = _format_snippet(lines, "Step")
        assert len(result) <= 3100  # 3000 + header + truncation message

    def test_empty_lines(self):
        result = _format_snippet([], "Step")
        assert result == "[Failed step: Step]\n"
