#!/usr/bin/env python3
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
# /// script
# requires-python = ">=3.10"
# ///
"""
Extract E2E test results from Playwright JSON report.

Reads the Playwright JSON results file and produces two output files:
  - failures.json: list of failed tests with short error summaries
  - fixme_tests.json: list of tests marked with test.fixme and their status

Environment variables:
  RESULTS_JSON   - Path to Playwright results.json (default: test-results/results.json)
  OUTPUT_DIR     - Directory for output files (default: e2e-test-report)
  BROWSER        - Browser name to include in output (default: unknown)
  RUN_ID         - GitHub Actions run ID (default: unknown)
  RUN_ATTEMPT    - GitHub Actions run attempt (default: 1)
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

MAX_ERROR_LENGTH = 300


def truncate_error(message: str) -> str:
    """Truncate error message to a reasonable length for display."""
    message = message.strip().replace("\n", " ").replace("\r", "")
    if len(message) > MAX_ERROR_LENGTH:
        return message[:MAX_ERROR_LENGTH] + "..."
    return message


def extract_test_title(spec: dict) -> str:
    """Build full test title from spec file and test name."""
    spec_file = spec.get("location", {}).get("file", "unknown")
    title_parts = spec.get("titlePath", [])
    # titlePath is typically ["", "spec description", "test name"]
    # Filter empty parts and join
    title = " > ".join(part for part in title_parts if part)
    return f"{spec_file}: {title}" if title else spec_file


def extract_failures(suites: list[dict]) -> list[dict]:
    """Extract failed tests from Playwright JSON suites."""
    failures = []
    for suite in suites:
        failures.extend(_extract_failures_from_suite(suite))
    return failures


def _extract_failures_from_suite(suite: dict) -> list[dict]:
    """Recursively extract failures from a suite and its children."""
    failures = []
    for spec in suite.get("specs", []):
        for test in spec.get("tests", []):
            if test.get("status") == "expected":
                continue
            # Check if any result actually failed
            for result in test.get("results", []):
                if result.get("status") in ("failed", "timedOut"):
                    error_message = ""
                    if result.get("error", {}).get("message"):
                        error_message = truncate_error(result["error"]["message"])
                    elif result.get("errors"):
                        messages = [e.get("message", "") for e in result["errors"] if e.get("message")]
                        error_message = truncate_error("; ".join(messages))

                    failures.append(
                        {
                            "test": extract_test_title(spec),
                            "status": result.get("status", "failed"),
                            "error": error_message,
                            "spec_file": spec.get("file", suite.get("file", "unknown")),
                            "retry": result.get("retry", 0),
                        }
                    )
                    break  # Only report first failure per test

    for child_suite in suite.get("suites", []):
        failures.extend(_extract_failures_from_suite(child_suite))
    return failures


def extract_fixme_tests(suites: list[dict]) -> list[dict]:
    """Extract tests marked with test.fixme from Playwright JSON suites."""
    fixme_tests = []
    for suite in suites:
        fixme_tests.extend(_extract_fixme_from_suite(suite))
    return fixme_tests


def _extract_fixme_from_suite(suite: dict) -> list[dict]:
    """Recursively extract fixme tests from a suite and its children."""
    fixme_tests = []
    for spec in suite.get("specs", []):
        for test in spec.get("tests", []):
            # In Playwright JSON, fixme tests have status "skipped" and
            # annotations with type "fixme"
            annotations = test.get("annotations", [])
            is_fixme = any(a.get("type") == "fixme" for a in annotations)
            if is_fixme:
                fixme_tests.append(
                    {
                        "test": extract_test_title(spec),
                        "status": test.get("status", "skipped"),
                        "spec_file": spec.get("file", suite.get("file", "unknown")),
                        "annotations": [
                            {"type": a.get("type", ""), "description": a.get("description", "")}
                            for a in annotations
                        ],
                    }
                )

    for child_suite in suite.get("suites", []):
        fixme_tests.extend(_extract_fixme_from_suite(child_suite))
    return fixme_tests


def main() -> None:
    results_path = Path(os.environ.get("RESULTS_JSON", "test-results/results.json"))
    output_dir = Path(os.environ.get("OUTPUT_DIR", "e2e-test-report"))
    browser = os.environ.get("BROWSER", "unknown")
    run_id = os.environ.get("RUN_ID", "unknown")
    run_attempt = os.environ.get("RUN_ATTEMPT", "1")

    output_dir.mkdir(parents=True, exist_ok=True)

    if not results_path.exists():
        print(f"Results file not found: {results_path}")
        print("Writing empty results (no test data available).")
        metadata = {
            "browser": browser,
            "run_id": run_id,
            "run_attempt": run_attempt,
            "has_results": False,
        }
        (output_dir / "failures.json").write_text(
            json.dumps({"metadata": metadata, "failures": []}, indent=2)
        )
        (output_dir / "fixme_tests.json").write_text(
            json.dumps({"metadata": metadata, "fixme_tests": []}, indent=2)
        )
        return

    try:
        report = json.loads(results_path.read_text())
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in {results_path}: {e}", file=sys.stderr)
        sys.exit(1)

    suites = report.get("suites", [])
    failures = extract_failures(suites)
    fixme_tests = extract_fixme_tests(suites)

    metadata = {
        "browser": browser,
        "run_id": run_id,
        "run_attempt": run_attempt,
        "has_results": True,
        "total_tests": report.get("stats", {}).get("expected", 0)
        + report.get("stats", {}).get("unexpected", 0)
        + report.get("stats", {}).get("skipped", 0),
        "total_passed": report.get("stats", {}).get("expected", 0),
        "total_failed": report.get("stats", {}).get("unexpected", 0),
        "total_skipped": report.get("stats", {}).get("skipped", 0),
    }

    failures_output = {"metadata": metadata, "failures": failures}
    fixme_output = {"metadata": metadata, "fixme_tests": fixme_tests}

    (output_dir / "failures.json").write_text(json.dumps(failures_output, indent=2))
    (output_dir / "fixme_tests.json").write_text(json.dumps(fixme_output, indent=2))

    print(f"Browser: {browser}")
    print(f"Total tests: {metadata['total_tests']}")
    print(f"Failures: {len(failures)}")
    print(f"Fixme tests: {len(fixme_tests)}")
    print(f"Output written to: {output_dir}")


if __name__ == "__main__":
    main()
