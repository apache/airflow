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
import json
import sys
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[3] / "scripts" / "ci" / "extract_e2e_test_results.py"


@pytest.fixture
def extract_module():
    module_name = "test_extract_e2e_module"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


SAMPLE_PLAYWRIGHT_REPORT = {
    "stats": {"expected": 5, "unexpected": 2, "skipped": 1},
    "suites": [
        {
            "title": "Test Suite",
            "file": "tests/e2e/specs/home.spec.ts",
            "specs": [
                {
                    "title": "should load home page",
                    "file": "tests/e2e/specs/home.spec.ts",
                    "location": {"file": "tests/e2e/specs/home.spec.ts"},
                    "titlePath": ["", "Home", "should load home page"],
                    "tests": [
                        {
                            "status": "unexpected",
                            "annotations": [],
                            "results": [
                                {
                                    "status": "failed",
                                    "retry": 0,
                                    "error": {"message": "Expected visible but element was hidden"},
                                }
                            ],
                        }
                    ],
                },
                {
                    "title": "should display dashboard",
                    "file": "tests/e2e/specs/home.spec.ts",
                    "location": {"file": "tests/e2e/specs/home.spec.ts"},
                    "titlePath": ["", "Home", "should display dashboard"],
                    "tests": [
                        {
                            "status": "expected",
                            "annotations": [],
                            "results": [{"status": "passed", "retry": 0}],
                        }
                    ],
                },
            ],
            "suites": [
                {
                    "title": "Nested Suite",
                    "file": "tests/e2e/specs/home.spec.ts",
                    "specs": [
                        {
                            "title": "should handle fixme test",
                            "file": "tests/e2e/specs/home.spec.ts",
                            "location": {"file": "tests/e2e/specs/home.spec.ts"},
                            "titlePath": ["", "Home", "Nested Suite", "should handle fixme test"],
                            "tests": [
                                {
                                    "status": "skipped",
                                    "annotations": [{"type": "fixme", "description": ""}],
                                    "results": [],
                                }
                            ],
                        }
                    ],
                    "suites": [],
                }
            ],
        }
    ],
}


class TestTruncateError:
    def test_short_message_unchanged(self, extract_module):
        assert extract_module.truncate_error("short error") == "short error"

    def test_long_message_truncated(self, extract_module):
        long_msg = "x" * 400
        result = extract_module.truncate_error(long_msg)
        assert len(result) == 303  # 300 + "..."
        assert result.endswith("...")

    def test_strips_whitespace_and_newlines(self, extract_module):
        assert extract_module.truncate_error("  error\nmessage\r\n  ") == "error message"


class TestExtractTestTitle:
    def test_with_title_path(self, extract_module):
        spec = {
            "location": {"file": "test.spec.ts"},
            "titlePath": ["", "Suite", "test name"],
        }
        assert extract_module.extract_test_title(spec) == "test.spec.ts: Suite > test name"

    def test_without_title_path(self, extract_module):
        spec = {"location": {"file": "test.spec.ts"}, "titlePath": []}
        assert extract_module.extract_test_title(spec) == "test.spec.ts"

    def test_missing_location(self, extract_module):
        spec = {"titlePath": ["", "Suite", "test"]}
        assert extract_module.extract_test_title(spec) == "unknown: Suite > test"


class TestExtractFailures:
    def test_extracts_failed_tests(self, extract_module):
        failures = extract_module.extract_failures(SAMPLE_PLAYWRIGHT_REPORT["suites"])
        assert len(failures) == 1
        assert failures[0]["status"] == "failed"
        assert "visible" in failures[0]["error"]
        assert failures[0]["spec_file"] == "tests/e2e/specs/home.spec.ts"

    def test_skips_passing_tests(self, extract_module):
        failures = extract_module.extract_failures(SAMPLE_PLAYWRIGHT_REPORT["suites"])
        test_names = [f["test"] for f in failures]
        assert not any("dashboard" in name for name in test_names)

    def test_empty_suites(self, extract_module):
        assert extract_module.extract_failures([]) == []


class TestExtractFixmeTests:
    def test_extracts_fixme_tests(self, extract_module):
        fixme = extract_module.extract_fixme_tests(SAMPLE_PLAYWRIGHT_REPORT["suites"])
        assert len(fixme) == 1
        assert "fixme" in fixme[0]["test"]
        assert fixme[0]["status"] == "skipped"
        assert any(a["type"] == "fixme" for a in fixme[0]["annotations"])

    def test_empty_suites(self, extract_module):
        assert extract_module.extract_fixme_tests([]) == []


class TestMainFunction:
    def test_missing_results_file_writes_empty(self, extract_module, tmp_path, monkeypatch):
        output_dir = tmp_path / "output"
        monkeypatch.setenv("RESULTS_JSON", str(tmp_path / "nonexistent.json"))
        monkeypatch.setenv("OUTPUT_DIR", str(output_dir))
        monkeypatch.setenv("BROWSER", "chromium")
        monkeypatch.setenv("RUN_ID", "12345")

        extract_module.main()

        failures = json.loads((output_dir / "failures.json").read_text())
        assert failures["failures"] == []
        assert failures["metadata"]["has_results"] is False

        fixme = json.loads((output_dir / "fixme_tests.json").read_text())
        assert fixme["fixme_tests"] == []

    def test_valid_results_file(self, extract_module, tmp_path, monkeypatch):
        results_file = tmp_path / "results.json"
        results_file.write_text(json.dumps(SAMPLE_PLAYWRIGHT_REPORT))
        output_dir = tmp_path / "output"

        monkeypatch.setenv("RESULTS_JSON", str(results_file))
        monkeypatch.setenv("OUTPUT_DIR", str(output_dir))
        monkeypatch.setenv("BROWSER", "firefox")
        monkeypatch.setenv("RUN_ID", "99999")

        extract_module.main()

        failures = json.loads((output_dir / "failures.json").read_text())
        assert len(failures["failures"]) == 1
        assert failures["metadata"]["browser"] == "firefox"
        assert failures["metadata"]["has_results"] is True

        fixme = json.loads((output_dir / "fixme_tests.json").read_text())
        assert len(fixme["fixme_tests"]) == 1
