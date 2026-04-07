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
"""Workflow run management helpers for PR triage.

Provides functions to find, approve, cancel, and rerun GitHub Actions
workflow runs associated with pull requests.
"""
from __future__ import annotations

from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.pr_github import github_rest


def find_workflow_runs_by_status(
    token: str, github_repository: str, head_sha: str, status: str
) -> list[dict]:
    """Find workflow runs with a given status for a commit SHA.

    Common statuses: ``action_required``, ``in_progress``, ``queued``.
    """
    import requests

    url = f"https://api.github.com/repos/{github_repository}/actions/runs"
    try:
        response = requests.get(
            url,
            params={"head_sha": head_sha, "status": status, "per_page": "50"},
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            timeout=(10, 20),
        )
    except (requests.ConnectionError, requests.Timeout, OSError):
        return []
    if response.status_code != 200:
        return []
    return response.json().get("workflow_runs", [])


def find_pending_workflow_runs(token: str, github_repository: str, head_sha: str) -> list[dict]:
    """Find workflow runs awaiting approval for a given commit SHA."""
    return find_workflow_runs_by_status(token, github_repository, head_sha, "action_required")


def has_in_progress_workflows(token: str, github_repository: str, head_sha: str) -> bool:
    """Check whether a PR has any workflow runs currently in progress or queued."""
    for status in ("in_progress", "queued"):
        if find_workflow_runs_by_status(token, github_repository, head_sha, status):
            return True
    return False


def post_workflow_run_action(token: str, github_repository: str, run: dict, action: str) -> bool:
    """POST to a workflow run endpoint (approve/cancel/rerun). Returns True on success."""
    run_id = run["id"]
    url = f"https://api.github.com/repos/{github_repository}/actions/runs/{run_id}/{action}"
    return github_rest(token, "post", url, ok_codes=(201, 202, 204))


def approve_workflow_runs(token: str, github_repository: str, pending_runs: list[dict]) -> int:
    """Approve pending workflow runs. Returns number successfully approved."""
    approved = 0
    for run in pending_runs:
        if post_workflow_run_action(token, github_repository, run, "approve"):
            approved += 1
        else:
            console_print(
                f"  [warning]Failed to approve run {run.get('name', run['id'])}: check permissions[/]"
            )
    return approved


def cancel_workflow_run(token: str, github_repository: str, run: dict) -> bool:
    """Cancel a single workflow run. Returns True if successful."""
    return post_workflow_run_action(token, github_repository, run, "cancel")


def rerun_workflow_run(token: str, github_repository: str, run: dict) -> bool:
    """Rerun a complete workflow run (all jobs). Returns True if successful."""
    return post_workflow_run_action(token, github_repository, run, "rerun")


def rerun_failed_workflow_runs(
    token: str, github_repository: str, head_sha: str, failed_check_names: list[str]
) -> int:
    """Rerun failed workflow runs for a commit. Returns number of runs rerun."""
    import requests

    # Find completed (failed) workflow runs for this SHA
    runs = find_workflow_runs_by_status(token, github_repository, head_sha, "completed")
    if not runs:
        return 0

    # Filter to runs whose names match failed checks
    failed_set = set(failed_check_names)
    failed_runs = [r for r in runs if r.get("name") in failed_set or r.get("conclusion") == "failure"]

    rerun_count = 0
    for run in failed_runs:
        run_id = run["id"]
        url = f"https://api.github.com/repos/{github_repository}/actions/runs/{run_id}/rerun-failed-jobs"
        response = requests.post(
            url,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            timeout=(10, 20),
        )
        if response.status_code in (201, 204):
            console_print(f"  [success]Rerun triggered for: {run.get('name', run_id)}[/]")
            rerun_count += 1
        else:
            console_print(f"  [warning]Failed to rerun {run.get('name', run_id)}: {response.status_code}[/]")
    return rerun_count


def cancel_and_rerun_in_progress_workflows(token: str, github_repository: str, head_sha: str) -> int:
    """Cancel in-progress/queued workflow runs and rerun them. Returns number rerun."""
    import time as time_mod

    in_progress = []
    for status in ("in_progress", "queued"):
        in_progress.extend(find_workflow_runs_by_status(token, github_repository, head_sha, status))
    if not in_progress:
        return 0

    # Cancel all in-progress runs
    cancelled = 0
    for run in in_progress:
        name = run.get("name", run["id"])
        if cancel_workflow_run(token, github_repository, run):
            console_print(f"  [info]Cancelled workflow run: {name}[/]")
            cancelled += 1
        else:
            console_print(f"  [warning]Failed to cancel: {name}[/]")

    if not cancelled:
        return 0

    # Brief pause to let GitHub process the cancellations
    console_print("  [dim]Waiting for cancellations to complete...[/]")
    time_mod.sleep(3)

    # Rerun the cancelled runs
    rerun_count = 0
    for run in in_progress:
        name = run.get("name", run["id"])
        if rerun_workflow_run(token, github_repository, run):
            console_print(f"  [success]Rerun triggered for: {name}[/]")
            rerun_count += 1
        else:
            console_print(f"  [warning]Failed to rerun: {name}[/]")
    return rerun_count
