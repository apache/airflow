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

import importlib.util
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

MODULE_PATH = Path(__file__).resolve().parents[3] / "scripts" / "ci" / "analyze_e2e_flaky_tests.py"


@pytest.fixture
def analyze_module():
    module_name = "test_analyze_e2e_module"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestGhApi:
    def test_forces_get_method(self, analyze_module):
        """`gh api` defaults to POST when -f is passed; we must force GET to avoid 404."""
        completed = subprocess.CompletedProcess(args=[], returncode=0, stdout="{}", stderr="")
        with patch.object(subprocess, "run", return_value=completed) as mock_run:
            analyze_module.gh_api("repos/apache/airflow/actions/workflows/x/runs", branch="main")
        args = mock_run.call_args[0][0]
        assert "--method" in args
        assert args[args.index("--method") + 1] == "GET"


class TestEscapeSlackMrkdwn:
    def test_escapes_ampersand(self, analyze_module):
        assert analyze_module.escape_slack_mrkdwn("a & b") == "a &amp; b"

    def test_escapes_angle_brackets(self, analyze_module):
        assert analyze_module.escape_slack_mrkdwn("<div>") == "&lt;div&gt;"

    def test_plain_text_unchanged(self, analyze_module):
        assert analyze_module.escape_slack_mrkdwn("hello world") == "hello world"

    def test_all_special_chars(self, analyze_module):
        result = analyze_module.escape_slack_mrkdwn("a < b & c > d")
        assert result == "a &lt; b &amp; c &gt; d"


class TestIdentifyFlakyCandidates:
    def test_empty_failures(self, analyze_module):
        assert analyze_module.identify_flaky_candidates({}, 5) == []

    def test_zero_runs(self, analyze_module):
        assert analyze_module.identify_flaky_candidates({"test": {}}, 0) == []

    def test_identifies_flaky_test(self, analyze_module):
        failures = {
            "test_a": {
                "count": 5,
                "browsers": ["chromium", "firefox"],
                "errors": [{"error": "timeout", "browser": "chromium", "run_id": 1}],
                "run_ids": [1, 2, 3],
            },
        }
        candidates = analyze_module.identify_flaky_candidates(failures, 5)
        assert len(candidates) == 1
        assert candidates[0]["test"] == "test_a"
        assert candidates[0]["failure_rate"] > 0

    def test_excludes_low_failure_rate(self, analyze_module):
        failures = {
            "test_a": {
                "count": 1,
                "browsers": ["chromium", "firefox", "webkit"],
                "errors": [],
                "run_ids": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            },
        }
        candidates = analyze_module.identify_flaky_candidates(failures, 10)
        assert len(candidates) == 0

    def test_sorted_by_failure_rate(self, analyze_module):
        failures = {
            "test_low": {
                "count": 3,
                "browsers": ["chromium"],
                "errors": [],
                "run_ids": [1, 2, 3],
            },
            "test_high": {
                "count": 10,
                "browsers": ["chromium"],
                "errors": [],
                "run_ids": [1, 2, 3],
            },
        }
        candidates = analyze_module.identify_flaky_candidates(failures, 10)
        if len(candidates) >= 2:
            assert candidates[0]["failure_rate"] >= candidates[1]["failure_rate"]


class TestFormatSlackMessage:
    def test_empty_results(self, analyze_module):
        msg = analyze_module.format_slack_message(
            failures_by_test={},
            fixme_tests={},
            flaky_candidates=[],
            runs=[],
            runs_with_data=0,
            repo="apache/airflow",
        )
        assert msg["channel"] == "airflow-3-ui"
        assert "text" in msg
        assert "blocks" in msg
        assert any("No flaky test candidates" in str(b) for b in msg["blocks"])
        assert any("No test failures" in str(b) for b in msg["blocks"])

    def test_with_failures_and_fixme(self, analyze_module):
        failures = {
            "test_a": {
                "count": 3,
                "browsers": ["chromium"],
                "errors": [{"error": "timeout error", "browser": "chromium", "run_id": 1}],
                "run_ids": [1, 2],
            },
        }
        fixme = {
            "test_b": {"spec_file": "test.spec.ts", "annotations": [{"type": "fixme"}]},
        }
        candidates = [
            {
                "test": "test_a",
                "failure_rate": 50.0,
                "failure_count": 3,
                "browsers": ["chromium"],
                "errors": [{"error": "timeout error"}],
            }
        ]
        runs = [{"id": 1, "run_number": 100, "html_url": "https://example.com"}]
        msg = analyze_module.format_slack_message(failures, fixme, candidates, runs, 1, "apache/airflow")
        assert msg["channel"] == "airflow-3-ui"
        blocks_text = str(msg["blocks"])
        assert "test_a" in blocks_text
        assert "test_b" in blocks_text
        assert "50.0%" in blocks_text

    def test_special_chars_in_test_names(self, analyze_module):
        failures = {
            "test <with> & special": {
                "count": 3,
                "browsers": ["chromium"],
                "errors": [{"error": "err <msg>", "browser": "chromium", "run_id": 1}],
                "run_ids": [1],
            },
        }
        candidates = [
            {
                "test": "test <with> & special",
                "failure_rate": 100.0,
                "failure_count": 3,
                "browsers": ["chromium"],
                "errors": [{"error": "err <msg>"}],
            }
        ]
        msg = analyze_module.format_slack_message(failures, {}, candidates, [], 1, "apache/airflow")
        blocks_text = str(msg["blocks"])
        # Angle brackets and ampersands should be escaped
        assert "&lt;" in blocks_text
        assert "&gt;" in blocks_text
        assert "&amp;" in blocks_text
