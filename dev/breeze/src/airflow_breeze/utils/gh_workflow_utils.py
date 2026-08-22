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
import re
import subprocess
import sys
import time
from shutil import which

from airflow_breeze.global_constants import MIN_GH_VERSION
from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.github import run_gh_command
from airflow_breeze.utils.shared_options import get_dry_run

NEW_RUN_TIMEOUT_SECONDS = 180
NEW_RUN_POLL_SECONDS = 5


def tigger_workflow(workflow_name: str, repo: str, branch: str = "main", **kwargs):
    """
    Trigger a GitHub Actions workflow using the `gh` CLI.

    :param workflow_name: The name of the workflow to trigger.
    :param repo: Workflow repository example: 'apache/airflow'
    :param branch: The branch to run the workflow on.
    :param kwargs: Additional parameters to pass to the workflow.
    """
    command = ["gh", "workflow", "run", workflow_name, "--ref", branch, "--repo", repo]

    # These are the input parameters to workflow
    for key, value_raw in kwargs.items():
        # GH cli requires bool inputs to be converted to string format
        if isinstance(value_raw, bool):
            value = "true" if value_raw else "false"
        else:
            value = value_raw

        command.extend(["-f", f"{key}={value}"])

    console_print(f"[blue]Running command: {' '.join(command)}[/blue]")
    result = run_gh_command(command, capture_output=True)

    if result.returncode != 0:
        console_print(f"[red]Error running workflow: {result.stderr}[/red]")
        sys.exit(1)

    if get_dry_run():
        # A dry run dispatches nothing, so `gh run list` comes back empty.
        console_print(f"[info]Dry run: not looking up or monitoring a run of {workflow_name}.")
        return

    # Wait for a few seconds to start the workflow run
    time.sleep(5)


def make_sure_gh_is_installed():
    if not which("gh"):
        console_print(
            "[red]Error! The `gh` tool is not installed.[/]\n\n"
            "[yellow]You need to install `gh` tool (see https://github.com/cli/cli) and "
            "run `gh auth login` to connect your repo to GitHub."
        )
        sys.exit(1)
    version_string = subprocess.check_output(["gh", "version"]).decode("utf-8")
    match = re.search(r"gh version (\d+\.\d+\.\d+)", version_string)
    if match:
        version = match.group(1)
        from packaging.version import Version

        if Version(version) < Version(MIN_GH_VERSION):
            console_print(
                f"[red]Error! The `gh` tool version is too old. "
                f"Please upgrade to at least version {MIN_GH_VERSION}[/]"
            )
            sys.exit(1)
    else:
        console_print(
            "[red]Error! Could not determine the version of the `gh` tool. Please ensure it is installed correctly.[/]"
        )
        sys.exit(1)


def get_latest_workflow_run_id(workflow_name: str, repo: str) -> int | None:
    """
    Get the latest workflow run ID for a given workflow name and repository.

    :param workflow_name: The name of the workflow to check.
    :param repo: The repository in the format 'owner/repo'.
    :return: The run id, or None when the workflow has never run.
    """
    make_sure_gh_is_installed()
    command = [
        "gh",
        "run",
        "list",
        "--workflow",
        workflow_name,
        "--repo",
        repo,
        "--limit",
        "1",
        "--json",
        "databaseId",
    ]

    result = run_gh_command(command, capture_output=True)
    if result.returncode != 0:
        console_print(f"[red]Error fetching workflow run ID: {result.stderr}[/red]")
        sys.exit(1)

    runs_data = result.stdout.strip()
    if not runs_data:
        return None

    runs = json.loads(runs_data)
    return runs[0].get("databaseId") if runs else None


def wait_for_new_workflow_run(workflow_name: str, repo: str, previous_run_id: int | None) -> int:
    """
    Wait until a run newer than ``previous_run_id`` shows up and return its id.

    Run ids increase monotonically, so anything above the id observed just before the dispatch is
    the run we started. Taking whatever run is newest would instead latch onto an unrelated one -
    a scheduled run, or another maintainer's - whenever ours has not registered yet, and report
    that run's result as ours.

    :param workflow_name: The name of the workflow that was dispatched.
    :param repo: The repository in the format 'owner/repo'.
    :param previous_run_id: The newest run id seen before dispatching, or None if there was none.
    """
    deadline = time.monotonic() + NEW_RUN_TIMEOUT_SECONDS
    while True:
        run_id = get_latest_workflow_run_id(workflow_name, repo)
        if run_id is not None and (previous_run_id is None or run_id > previous_run_id):
            console_print(
                f"[blue]Running workflow {workflow_name} at "
                f"https://github.com/{repo}/actions/runs/{run_id}[/blue]",
            )
            return run_id
        if time.monotonic() >= deadline:
            console_print(
                f"[red]Timed out after {NEW_RUN_TIMEOUT_SECONDS}s waiting for the dispatched run of "
                f"{workflow_name} in {repo} to appear.[/red]"
            )
            sys.exit(1)
        time.sleep(NEW_RUN_POLL_SECONDS)


def get_workflow_run_info(run_id: str, repo: str, fields: str) -> dict:
    """
    Get the workflow information for a specific run ID and return the specified fields.

    :param run_id: The ID of the workflow run to check.
    :param repo: Workflow repository example: 'apache/airflow'
    :param fields: Comma-separated fields to retrieve from the workflow run to fetch. eg: "status,conclusion,name,jobs"
    """
    make_sure_gh_is_installed()
    command = ["gh", "run", "view", run_id, "--json", fields, "--repo", repo]

    result = run_gh_command(command, capture_output=True)
    if result.returncode != 0:
        console_print(f"[red]Error fetching workflow run status: {result.stderr}[/red]")
        sys.exit(1)

    return json.loads(result.stdout.strip())


def monitor_workflow_run(run_id: str, repo: str):
    """
    Monitor the status of a workflow run until it completes.

    :param run_id: The ID of the workflow run to monitor.
    :param repo: Workflow repository example: 'apache/airflow'
    """

    completed_jobs = []

    while True:
        jobs_data = get_workflow_run_info(run_id, repo, "jobs")

        for job in jobs_data.get("jobs", []):
            name = job["name"]
            status = job["status"]
            conclusion = job["conclusion"]

            if name not in completed_jobs and status != "completed":
                console_print(f"[yellow]- Job: {name} | Status: {status} | Conclusion: {conclusion}[/yellow]")
                continue

            if name not in completed_jobs:
                console_print(f"[green]- Job: {name} | Status: {status} | Conclusion: {conclusion}[/green]")
                completed_jobs.append(name)

        workflow_run_status_conclusion = get_workflow_run_info(run_id, repo, "status,conclusion,name")

        status = workflow_run_status_conclusion.get("status")
        conclusion = workflow_run_status_conclusion.get("conclusion")
        name = workflow_run_status_conclusion.get("name")

        if status == "completed":
            if conclusion == "success":
                console_print(f"[green]Workflow {name} run {run_id} completed successfully.[/green]")
                break
            # Anything else - failure, cancelled, timed_out, action_required - means the run did not
            # produce what the caller is about to chain further work onto.
            console_print(
                f"[red]Workflow {name} run {run_id} finished with conclusion '{conclusion}', "
                f"see for more info: https://github.com/{repo}/actions/runs/{run_id}[/red]"
            )
            sys.exit(1)

        # Check status of jobs every 30 seconds
        time.sleep(30)


def trigger_workflow_and_monitor(
    workflow_name: str, repo: str, branch: str = "main", monitor=True, **workflow_fields
):
    make_sure_gh_is_installed()
    previous_run_id = None if get_dry_run() else get_latest_workflow_run_id(workflow_name, repo)
    tigger_workflow(
        workflow_name=workflow_name,
        repo=repo,
        branch=branch,
        **workflow_fields,
    )

    if get_dry_run():
        return

    workflow_run_id = wait_for_new_workflow_run(
        workflow_name=workflow_name,
        repo=repo,
        previous_run_id=previous_run_id,
    )

    console_print(
        f"[blue]Workflow run ID: {workflow_run_id}[/blue]",
    )

    if not monitor:
        return

    monitor_workflow_run(
        run_id=str(workflow_run_id),
        repo=repo,
    )
