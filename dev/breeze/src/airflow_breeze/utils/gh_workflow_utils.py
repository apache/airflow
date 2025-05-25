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
import sys
import time

from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.run_utils import run_command


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
    for key, value in kwargs.items():
        command.extend(["-f", f"{key}={value}"])

    get_console().print(f"[blue]Running command: {' '.join(command)}[/blue]")
    result = run_command(command, capture_output=True, check=False)

    if result.returncode != 0:
        get_console().print(f"[red]Error running workflow: {result.stderr}[/red]")
        sys.exit(1)

    # Wait for a few seconds to start the workflow run
    time.sleep(5)


def get_workflow_run_id(workflow_name: str, repo: str) -> int:
    """
    Get the latest workflow run ID for a given workflow name and repository.

    :param workflow_name: The name of the workflow to check.
    :param repo: The repository in the format 'owner/repo'.
    """
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

    result = run_command(command, capture_output=True, check=False)
    if result.returncode != 0:
        get_console().print(f"[red]Error fetching workflow run ID: {result.stderr}[/red]")
        sys.exit(1)

    runs_data = result.stdout.strip()
    if not runs_data:
        get_console().print("[red]No workflow runs found.[/red]")
        sys.exit(1)

    return json.loads(runs_data)[0]["databaseId"]


def get_workflow_run_info(run_id: str, repo: str, fields: str) -> dict:
    """
    Get the workflow information for a specific run ID and return the specified fields.

    :param run_id: The ID of the workflow run to check.
    :param repo: Workflow repository example: 'apache/airflow'
    :param fields: Comma-separated fields to retrieve from the workflow run to fetch. eg: "status,conclusion,name,jobs"
    """
    command = ["gh", "run", "view", run_id, "--json", fields, "--repo", repo]

    result = run_command(command, capture_output=True, check=False)
    if result.returncode != 0:
        get_console().print(f"[red]Error fetching workflow run status: {result.stderr}[/red]")
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
                get_console().print(
                    f"[yellow]- Job: {name} | Status: {status} | Conclusion: {conclusion}[/yellow]"
                )
                continue

            if name not in completed_jobs:
                get_console().print(
                    f"[green]- Job: {name} | Status: {status} | Conclusion: {conclusion}[/green]"
                )
                completed_jobs.append(name)

        workflow_run_status_conclusion = get_workflow_run_info(run_id, repo, "status,conclusion,name")

        status = workflow_run_status_conclusion.get("status")
        conclusion = workflow_run_status_conclusion.get("conclusion")
        name = workflow_run_status_conclusion.get("name")

        if status == "completed":
            if conclusion == "success":
                get_console().print(f"[green]Workflow {name} run {run_id} completed successfully.[/green]")
            elif conclusion == "failure":
                get_console().print(
                    f"[red]Workflow {name} run {run_id} failed, see for more info: https://github.com/{repo}/actions/runs/{run_id}[/red]"
                )
                sys.exit(1)
            break

        # Check status of jobs every 30 seconds
        time.sleep(30)


def trigger_workflow_and_monitor(
    workflow_name: str, repo: str, branch: str = "main", monitor=True, **workflow_fields
):
    tigger_workflow(
        workflow_name=workflow_name,
        repo=repo,
        branch=branch,
        **workflow_fields,
    )

    workflow_run_id = get_workflow_run_id(
        workflow_name=workflow_name,
        repo=repo,
    )

    get_console().print(
        f"[blue]Workflow run ID: {workflow_run_id}[/blue]",
    )

    if not monitor:
        return

    monitor_workflow_run(
        run_id=str(workflow_run_id),
        repo=repo,
    )
