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
# dependencies = [
#     "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import json
import os
import subprocess
import sys

from rich.console import Console

console = Console(width=400, color_system="standard")


def workflow_status(
    branch: str,
    workflow_id: str,
) -> list[dict]:
    """Check the status of a GitHub Actions workflow run."""
    cmd = [
        "gh",
        "run",
        "list",
        "--workflow",
        workflow_id,
        "--branch",
        branch,
        "--limit",
        "1",  # Limit to the most recent run
        "--repo",
        "apache/airflow",
        "--json",
        "conclusion,url",
    ]
    result = subprocess.run(
        cmd,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        console.print(f"[red]Error fetching workflow run ID: {str(result.stderr)}[/red]")
        sys.exit(1)

    runs_data = result.stdout.strip()
    if not runs_data:
        console.print("[red]No workflow runs found.[/red]")
        sys.exit(1)
    run_info = json.loads(runs_data)
    console.print(f"[blue]Workflow status for {workflow_id}: {run_info}[/blue]")
    return run_info


if __name__ == "__main__":
    branch = os.environ.get("workflow_branch")
    workflow_id = os.environ.get("workflow_id")

    if not branch or not workflow_id:
        console.print("[red]Both workflow-branch and workflow-id environment variables must be set.[/red]")
        sys.exit(1)

    console.print(f"[blue]Checking workflow status for branch: {branch}, workflow_id: {workflow_id}[/blue]")

    data: list[dict] = workflow_status(branch, workflow_id)
    conclusion = data[0].get("conclusion")
    url = data[0].get("url")

    if os.environ.get("GITHUB_OUTPUT") is None:
        console.print("[red]GITHUB_OUTPUT environment variable is not set. Cannot write output.[/red]")
        sys.exit(1)

    with open(os.environ["GITHUB_OUTPUT"], "a") as f:
        f.write(f"conclusion={conclusion}\n")
        f.write(f"run-url={url}\n")
