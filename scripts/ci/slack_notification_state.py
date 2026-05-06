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
Determine whether to send a Slack notification based on previous state.

Downloads previous state from GitHub Actions artifacts, compares with current
failures, and outputs the appropriate action to take.

Outputs (written to GITHUB_OUTPUT):
  action             - One of: notify_new, notify_reminder, notify_recovery, skip
  current-failures   - JSON list of current failure names
  previous-failures  - JSON list of previous failure names

Environment variables (required):
  ARTIFACT_NAME      - Name of the artifact storing notification state
  GITHUB_REPOSITORY  - Owner/repo (e.g. apache/airflow)

Environment variables (optional):
  CURRENT_FAILURES   - Newline-separated list of current failures (empty if none)
  GITHUB_OUTPUT      - Path to GitHub Actions output file
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REMINDER_INTERVAL_HOURS = 24
STATE_DIR = Path("./slack-state")
PREV_STATE_DIR = Path("./prev-slack-state")


def download_previous_state(artifact_name: str, repo: str) -> dict | None:
    """Download previous notification state artifact from GitHub Actions."""
    # `gh api` defaults to POST when -f/-F parameters are passed, which makes
    # the artifacts list endpoint return 404. Force GET so the parameters are
    # encoded as query string.
    result = subprocess.run(
        [
            "gh",
            "api",
            "--method",
            "GET",
            f"repos/{repo}/actions/artifacts",
            "-f",
            f"name={artifact_name}",
            "-f",
            "per_page=1",
            "--jq",
            ".artifacts[0]",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        print(f"Could not query artifacts API: {result.stderr}", file=sys.stderr)
        return None

    output = result.stdout.strip()
    if not output or output == "null":
        print("No previous state artifact found.")
        return None

    try:
        artifact = json.loads(output)
    except json.JSONDecodeError:
        print(f"Invalid JSON from artifacts API: {output}", file=sys.stderr)
        return None

    if artifact.get("expired", False):
        print("Previous state artifact has expired.")
        return None

    run_id = artifact.get("workflow_run", {}).get("id")
    if not run_id:
        print("No workflow run ID in artifact metadata.")
        return None

    PREV_STATE_DIR.mkdir(parents=True, exist_ok=True)
    dl_result = subprocess.run(
        [
            "gh",
            "run",
            "download",
            str(run_id),
            "--name",
            artifact_name,
            "--dir",
            str(PREV_STATE_DIR),
            "--repo",
            repo,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if dl_result.returncode != 0:
        print(f"Could not download previous state: {dl_result.stderr}", file=sys.stderr)
        return None

    state_file = PREV_STATE_DIR / "state.json"
    if not state_file.exists():
        print("Downloaded artifact does not contain state.json.")
        return None

    try:
        return json.loads(state_file.read_text())
    except json.JSONDecodeError:
        print("Invalid JSON in state.json.", file=sys.stderr)
        return None


def determine_action(current_failures: list[str], prev_state: dict | None) -> str:
    """Determine what notification action to take.

    Returns one of: notify_new, notify_reminder, notify_recovery, skip.
    """
    prev_failures = sorted(prev_state.get("failures", [])) if prev_state else []
    prev_notified = prev_state.get("last_notified") if prev_state else None
    now = datetime.now(timezone.utc)

    if current_failures:
        if not prev_failures:
            return "notify_new"
        if current_failures != prev_failures:
            return "notify_new"
        # Same failures as before — check if reminder is due
        if prev_notified:
            prev_time = datetime.fromisoformat(prev_notified)
            hours_since = (now - prev_time).total_seconds() / 3600
            if hours_since >= REMINDER_INTERVAL_HOURS:
                return "notify_reminder"
        else:
            return "notify_new"
    elif prev_failures:
        # Was failing, now all clear
        return "notify_recovery"

    return "skip"


def save_state(current_failures: list[str], action: str, prev_notified: str | None) -> None:
    """Save current state to file for upload as artifact."""
    now = datetime.now(timezone.utc)
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    new_state = {
        "failures": current_failures,
        "last_notified": (now.isoformat() if action != "skip" else (prev_notified or now.isoformat())),
    }
    (STATE_DIR / "state.json").write_text(json.dumps(new_state, indent=2))
    print(f"Saved state to {STATE_DIR / 'state.json'}: {new_state}")


def main() -> None:
    artifact_name = os.environ.get("ARTIFACT_NAME")
    if not artifact_name:
        print("ERROR: ARTIFACT_NAME environment variable is required.", file=sys.stderr)
        sys.exit(1)

    repo = os.environ.get("GITHUB_REPOSITORY", "apache/airflow")
    current_failures_str = os.environ.get("CURRENT_FAILURES", "")
    current_failures = sorted([f.strip() for f in current_failures_str.strip().splitlines() if f.strip()])

    # Download previous state
    print(f"Looking up previous state for artifact: {artifact_name}")
    prev_state = download_previous_state(artifact_name, repo)
    print(f"Previous state: {prev_state}")

    # Determine action
    action = determine_action(current_failures, prev_state)
    prev_failures = sorted(prev_state.get("failures", [])) if prev_state else []

    print(f"Action: {action}")
    print(f"Current failures: {current_failures}")
    print(f"Previous failures: {prev_failures}")

    # Save new state
    prev_notified = prev_state.get("last_notified") if prev_state else None
    save_state(current_failures, action, prev_notified)

    # Output for GitHub Actions
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"action={action}\n")
            f.write(f"current-failures={json.dumps(current_failures)}\n")
            f.write(f"previous-failures={json.dumps(prev_failures)}\n")


if __name__ == "__main__":
    main()
