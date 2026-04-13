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
Analyze E2E test results across multiple workflow runs to identify flaky tests.

Downloads e2e-test-report artifacts from recent workflow runs, aggregates
failure data, and produces a Slack-formatted report.

The script queries GitHub Actions for recent runs of the UI E2E test workflows,
downloads the test report artifacts, and analyzes failure patterns to identify
tests that are good candidates for marking with test.fixme.

Environment variables (required):
  GITHUB_REPOSITORY  - Owner/repo (e.g. apache/airflow)
  GITHUB_TOKEN       - GitHub token for API access

Environment variables (optional):
  MAX_RUNS           - Maximum number of workflow runs to analyze (default: 10)
  WORKFLOW_NAME      - Workflow file name to query (default: ci-amd-arm.yml)
  BRANCH             - Branch to filter runs (default: main)
  OUTPUT_FILE        - Path for the Slack message output (default: slack-message.json)
  GITHUB_OUTPUT      - Path to GitHub Actions output file
  GITHUB_STEP_SUMMARY - Path to GitHub Actions step summary file
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

BROWSERS = ["chromium", "firefox", "webkit"]
# A test failing in >= this fraction of runs is considered a flaky candidate
FLAKY_THRESHOLD = 0.3


def escape_slack_mrkdwn(text: str) -> str:
    """Escape special characters for Slack mrkdwn format."""
    text = text.replace("&", "&amp;")
    text = text.replace("<", "&lt;")
    text = text.replace(">", "&gt;")
    return text


def gh_api(endpoint: str, **kwargs: str) -> str | None:
    """Call GitHub API via gh CLI."""
    cmd = ["gh", "api", endpoint]
    for key, value in kwargs.items():
        cmd.extend(["-f", f"{key}={value}"])
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        print(f"gh api error for {endpoint}: {result.stderr}", file=sys.stderr)
        return None
    return result.stdout.strip()


def get_recent_run_ids(repo: str, workflow: str, branch: str, max_runs: int) -> list[dict]:
    """Get recent workflow run IDs and metadata."""
    output = gh_api(
        f"repos/{repo}/actions/workflows/{workflow}/runs",
        branch=branch,
        per_page=str(max_runs),
        status="completed",
    )
    if not output:
        return []
    try:
        data = json.loads(output)
    except json.JSONDecodeError:
        return []

    runs = []
    for run in data.get("workflow_runs", []):
        runs.append(
            {
                "id": run["id"],
                "run_number": run.get("run_number", "?"),
                "created_at": run.get("created_at", ""),
                "conclusion": run.get("conclusion", "unknown"),
                "html_url": run.get("html_url", ""),
            }
        )
    return runs[:max_runs]


def download_artifact(repo: str, run_id: int, artifact_name: str, dest_dir: Path) -> bool:
    """Download a specific artifact from a workflow run."""
    result = subprocess.run(
        [
            "gh",
            "run",
            "download",
            str(run_id),
            "--name",
            artifact_name,
            "--dir",
            str(dest_dir),
            "--repo",
            repo,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        # Artifact may not exist for this run (e.g. tests were skipped)
        return False
    return True


def load_json_safe(path: Path) -> dict | None:
    """Load JSON file, returning None on failure."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return None


def analyze_runs(repo: str, runs: list[dict]) -> tuple[dict[str, dict], dict[str, dict], int]:
    """Download and analyze test reports from multiple runs.

    Returns:
        - failures_by_test: {test_name: {count, browsers, errors, runs}}
        - fixme_tests: {test_name: {spec_file, annotations}}
        - runs_with_data: number of runs that had downloadable data
    """
    failures_by_test: dict[str, dict] = defaultdict(
        lambda: {"count": 0, "browsers": set(), "errors": [], "run_ids": []}
    )
    fixme_tests: dict[str, dict] = {}
    runs_with_data = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        for run in runs:
            run_id = run["id"]
            run_has_data = False

            for browser in BROWSERS:
                artifact_name = f"e2e-test-report-{browser}"
                dest = Path(tmpdir) / str(run_id) / browser
                dest.mkdir(parents=True, exist_ok=True)

                if not download_artifact(repo, run_id, artifact_name, dest):
                    continue

                run_has_data = True

                # Process failures
                failures_data = load_json_safe(dest / "failures.json")
                if failures_data:
                    for failure in failures_data.get("failures", []):
                        test_name = failure.get("test", "unknown")
                        entry = failures_by_test[test_name]
                        entry["count"] += 1
                        entry["browsers"].add(browser)
                        if failure.get("error") and len(entry["errors"]) < 3:
                            entry["errors"].append(
                                {
                                    "error": failure["error"],
                                    "browser": browser,
                                    "run_id": run_id,
                                }
                            )
                        if run_id not in entry["run_ids"]:
                            entry["run_ids"].append(run_id)

                # Process fixme tests (same across browsers, but collect once)
                fixme_data = load_json_safe(dest / "fixme_tests.json")
                if fixme_data:
                    for fixme in fixme_data.get("fixme_tests", []):
                        test_name = fixme.get("test", "unknown")
                        if test_name not in fixme_tests:
                            fixme_tests[test_name] = {
                                "spec_file": fixme.get("spec_file", "unknown"),
                                "annotations": fixme.get("annotations", []),
                            }

            if run_has_data:
                runs_with_data += 1

    # Convert sets to sorted lists for JSON serialization
    for entry in failures_by_test.values():
        entry["browsers"] = sorted(entry["browsers"])

    return dict(failures_by_test), fixme_tests, runs_with_data


def identify_flaky_candidates(failures_by_test: dict[str, dict], runs_with_data: int) -> list[dict]:
    """Identify tests that are good candidates for marking as fixme."""
    if runs_with_data == 0:
        return []

    candidates = []
    for test_name, data in failures_by_test.items():
        # Use runs where this test appeared rather than total runs * browsers,
        # since not every test runs in every browser or every run.
        runs_seen = len(data["run_ids"]) if data["run_ids"] else 1
        failure_rate = data["count"] / (runs_seen * len(data["browsers"])) if data["browsers"] else 0
        if failure_rate >= FLAKY_THRESHOLD:
            candidates.append(
                {
                    "test": test_name,
                    "failure_rate": round(failure_rate * 100, 1),
                    "failure_count": data["count"],
                    "browsers": data["browsers"],
                    "errors": data["errors"][:2],
                }
            )
    # Sort by failure rate descending
    candidates.sort(key=lambda c: c["failure_rate"], reverse=True)
    return candidates


def format_slack_message(
    failures_by_test: dict[str, dict],
    fixme_tests: dict[str, dict],
    flaky_candidates: list[dict],
    runs: list[dict],
    runs_with_data: int,
    repo: str,
) -> dict:
    """Format the analysis results as a Slack Block Kit message."""
    total_runs = len(runs)
    total_unique_failures = len(failures_by_test)
    total_fixme = len(fixme_tests)

    blocks: list[dict] = []

    # Header
    blocks.append(
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "UI E2E Flaky Test Report",
            },
        }
    )

    # Summary section
    summary_lines = [
        f"*Runs analyzed:* {runs_with_data}/{total_runs}",
        f"*Unique test failures:* {total_unique_failures}",
        f"*Flaky candidates (>={int(FLAKY_THRESHOLD * 100)}% failure rate):* {len(flaky_candidates)}",
        f"*Tests marked fixme:* {total_fixme}",
    ]
    blocks.append(
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(summary_lines)},
        }
    )
    blocks.append({"type": "divider"})

    # Flaky candidates section
    if flaky_candidates:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Flaky Test Candidates* (consider marking with `test.fixme`):",
                },
            }
        )
        for candidate in flaky_candidates[:10]:
            error_summary = ""
            if candidate["errors"]:
                first_error = escape_slack_mrkdwn(candidate["errors"][0]["error"])
                # Truncate for Slack display
                if len(first_error) > 150:
                    first_error = first_error[:150] + "..."
                error_summary = f"\n>  _Error: {first_error}_"

            test_name = escape_slack_mrkdwn(candidate["test"])
            browsers_str = ", ".join(candidate["browsers"])
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"*{test_name}*\n"
                            f"  Failure rate: *{candidate['failure_rate']}%* "
                            f"({candidate['failure_count']} failures) | "
                            f"Browsers: {browsers_str}"
                            f"{error_summary}"
                        ),
                    },
                }
            )
        if len(flaky_candidates) > 10:
            blocks.append(
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"_...and {len(flaky_candidates) - 10} more flaky candidates_",
                        }
                    ],
                }
            )
    else:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*No flaky test candidates detected* — all tests are stable or already marked.",
                },
            }
        )

    blocks.append({"type": "divider"})

    # All failures table
    if failures_by_test:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*All Test Failures* (across last runs):",
                },
            }
        )
        # Sort by count descending
        sorted_failures = sorted(failures_by_test.items(), key=lambda x: x[1]["count"], reverse=True)
        failure_lines = []
        for test_name, data in sorted_failures[:15]:
            escaped_name = escape_slack_mrkdwn(test_name)
            browsers_str = ", ".join(data["browsers"])
            error_hint = ""
            if data["errors"]:
                short_err = escape_slack_mrkdwn(data["errors"][0]["error"])
                if len(short_err) > 100:
                    short_err = short_err[:100] + "..."
                error_hint = f"\n>  _{short_err}_"
            failure_lines.append(
                f"• *{escaped_name}* — {data['count']}x failures | {browsers_str}{error_hint}"
            )
        # Slack has a 3000 char limit per text block; split if needed
        text = "\n".join(failure_lines)
        if len(text) > 2900:
            text = text[:2900] + "\n_...truncated_"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
        if len(sorted_failures) > 15:
            blocks.append(
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"_...and {len(sorted_failures) - 15} more failing tests_",
                        }
                    ],
                }
            )
    else:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*No test failures detected* in the analyzed runs.",
                },
            }
        )

    blocks.append({"type": "divider"})

    # Fixme tests section
    if fixme_tests:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Tests Marked `fixme`* (waiting for fixes):",
                },
            }
        )
        fixme_lines = []
        for test_name, data in sorted(fixme_tests.items()):
            escaped_name = escape_slack_mrkdwn(test_name)
            spec = escape_slack_mrkdwn(data.get("spec_file", "unknown"))
            fixme_lines.append(f"• {escaped_name} (`{spec}`)")
        text = "\n".join(fixme_lines)
        if len(text) > 2900:
            text = text[:2900] + "\n_...truncated_"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
    else:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*No tests are currently marked `fixme`.*",
                },
            }
        )

    # Footer with link
    if runs:
        latest_run = runs[0]
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": (
                            f"<https://github.com/{repo}/actions|View all workflow runs> | "
                            f"Latest: <{latest_run['html_url']}|Run #{latest_run['run_number']}>"
                        ),
                    }
                ],
            }
        )

    # Build the plain text fallback
    fallback = (
        f"UI E2E Flaky Test Report: "
        f"{total_unique_failures} unique failures, "
        f"{len(flaky_candidates)} flaky candidates, "
        f"{total_fixme} fixme tests "
        f"(from {runs_with_data} runs)"
    )

    return {
        "channel": "airflow-3-ui",
        "text": fallback,
        "blocks": blocks,
    }


def write_step_summary(
    failures_by_test: dict[str, dict],
    fixme_tests: dict[str, dict],
    flaky_candidates: list[dict],
    runs_with_data: int,
    total_runs: int,
) -> None:
    """Write a GitHub Actions step summary in markdown."""
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return

    lines = [
        "## UI E2E Flaky Test Report",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Runs analyzed | {runs_with_data}/{total_runs} |",
        f"| Unique test failures | {len(failures_by_test)} |",
        f"| Flaky candidates | {len(flaky_candidates)} |",
        f"| Tests marked fixme | {len(fixme_tests)} |",
        "",
    ]

    if flaky_candidates:
        lines.append("### Flaky Candidates")
        lines.append("")
        lines.append("| Test | Failure Rate | Count | Browsers |")
        lines.append("|------|-------------|-------|----------|")
        for c in flaky_candidates[:20]:
            browsers = ", ".join(c["browsers"])
            lines.append(f"| {c['test']} | {c['failure_rate']}% | {c['failure_count']} | {browsers} |")
        lines.append("")

    if failures_by_test:
        lines.append("### All Failures")
        lines.append("")
        lines.append("| Test | Failures | Browsers | Error |")
        lines.append("|------|----------|----------|-------|")
        sorted_failures = sorted(failures_by_test.items(), key=lambda x: x[1]["count"], reverse=True)
        for test_name, data in sorted_failures[:20]:
            browsers = ", ".join(data["browsers"])
            error = data["errors"][0]["error"][:100] + "..." if data["errors"] else ""
            error = error.replace("|", "\\|")
            lines.append(f"| {test_name} | {data['count']} | {browsers} | {error} |")
        lines.append("")

    if fixme_tests:
        lines.append("### Fixme Tests")
        lines.append("")
        for test_name, data in sorted(fixme_tests.items()):
            lines.append(f"- {test_name} (`{data.get('spec_file', 'unknown')}`)")
        lines.append("")

    with open(summary_path, "a") as f:
        f.write("\n".join(lines))


def main() -> None:
    repo = os.environ.get("GITHUB_REPOSITORY", "apache/airflow")
    max_runs = int(os.environ.get("MAX_RUNS", "10"))
    workflow = os.environ.get("WORKFLOW_NAME", "ci-amd-arm.yml")
    branch = os.environ.get("BRANCH", "main")
    output_file = Path(os.environ.get("OUTPUT_FILE", "slack-message.json"))

    print(f"Analyzing E2E test results for {repo} ({workflow} on {branch})")
    print(f"Looking at up to {max_runs} recent runs...")

    # Get recent runs
    runs = get_recent_run_ids(repo, workflow, branch, max_runs)
    if not runs:
        print("No workflow runs found.")
        sys.exit(0)

    print(f"Found {len(runs)} runs to analyze.")

    # Download and analyze
    failures_by_test, fixme_tests, runs_with_data = analyze_runs(repo, runs)

    print(f"Runs with data: {runs_with_data}/{len(runs)}")
    print(f"Unique failing tests: {len(failures_by_test)}")
    print(f"Fixme tests: {len(fixme_tests)}")

    # Identify flaky candidates
    flaky_candidates = identify_flaky_candidates(failures_by_test, runs_with_data)
    print(f"Flaky candidates: {len(flaky_candidates)}")

    # Format Slack message
    slack_message = format_slack_message(
        failures_by_test, fixme_tests, flaky_candidates, runs, runs_with_data, repo
    )
    output_file.write_text(json.dumps(slack_message, indent=2))
    print(f"Slack message written to: {output_file}")

    # Write step summary
    write_step_summary(failures_by_test, fixme_tests, flaky_candidates, runs_with_data, len(runs))

    # Set outputs for GitHub Actions
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        has_failures = len(failures_by_test) > 0
        has_flaky = len(flaky_candidates) > 0
        with open(github_output, "a") as f:
            f.write(f"has-failures={str(has_failures).lower()}\n")
            f.write(f"has-flaky-candidates={str(has_flaky).lower()}\n")
            f.write(f"unique-failures={len(failures_by_test)}\n")
            f.write(f"flaky-candidates={len(flaky_candidates)}\n")
            f.write(f"fixme-tests={len(fixme_tests)}\n")
            f.write(f"runs-analyzed={runs_with_data}\n")


if __name__ == "__main__":
    main()
