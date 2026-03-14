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

"""
Breeze context detector for agent skills.

Zero-dependency (stdlib only). Called by AI tools to determine whether
they are running on the HOST or inside a Breeze container, then returns
the correct command for the requested workflow.

Usage:
    python3 scripts/ci/prek/breeze_context_detect.py
    python3 scripts/ci/prek/breeze_context_detect.py --workflow run-tests
"""

import os
from pathlib import Path


def is_inside_breeze() -> bool:
    """
    Returns True if running inside a Breeze container.

    Detection priority (most reliable first):
    1. AIRFLOW_BREEZE_CONTAINER=true  (explicit env marker)
    2. /.dockerenv exists             (Docker sets in all containers)
    3. /opt/airflow exists            (Breeze canonical mount point)
    """
    if os.getenv("AIRFLOW_BREEZE_CONTAINER") == "true":
        return True
    if Path("/.dockerenv").exists():
        return True
    if Path("/opt/airflow").exists():
        return True
    return False


def get_context() -> str:
    return "breeze" if is_inside_breeze() else "host"


WORKFLOWS: dict[str, dict[str, str]] = {
    "static-checks": {
        "host": "prek",
        "breeze": "prek",
        "note": "Runs identically on host and inside container",
    },
    "run-tests": {
        "host": "uv run --project {distribution_folder} pytest {test_path} -xvs",
        "breeze": "pytest {test_path} -xvs",
        "note": "Local-first. Fall back to breeze exec if missing system deps",
    },
    "enter-breeze": {
        "host": "breeze shell",
        "breeze": "ERROR: already inside Breeze container",
        "note": "Only valid on host",
    },
    "start-airflow": {
        "host": "breeze start-airflow",
        "breeze": "ERROR: already inside Breeze container",
        "note": "Starts full Airflow stack for system verification",
    },
    "exec-command": {
        "host": "breeze exec -- {command}",
        "breeze": "{command}",
        "note": "Run single command in container from host, or directly if already inside",
    },
    "build-docs": {
        "host": "breeze build-docs",
        "breeze": "ERROR: build-docs runs on host only",
        "note": "Doc builds always run on host",
    },
    "git-operations": {
        "host": "git {args}",
        "breeze": "ERROR: git operations must run on host",
        "note": "All git operations run on host only",
    },
}


def get_command(workflow: str, **params: str) -> dict[str, str]:
    if workflow not in WORKFLOWS:
        raise ValueError(f"Unknown workflow '{workflow}'. Available: {sorted(WORKFLOWS.keys())}")
    context = get_context()
    template = WORKFLOWS[workflow][context]
    command = template.format(**params) if params else template
    return {
        "context": context,
        "workflow": workflow,
        "command": command,
        "note": WORKFLOWS[workflow].get("note", ""),
    }


def _main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Detect Breeze context and get recommended agent skill commands"
    )
    parser.add_argument("--workflow", choices=sorted(WORKFLOWS.keys()))
    parser.add_argument("--test-path", default="{test_path}")
    parser.add_argument("--distribution-folder", default="{distribution_folder}")
    args = parser.parse_args()

    context = get_context()
    print(f"Context: {context.upper()}")
    print()

    if args.workflow:
        result = get_command(
            args.workflow,
            test_path=args.test_path,
            distribution_folder=args.distribution_folder,
        )
        print(f"Workflow : {result['workflow']}")
        print(f"Command  : {result['command']}")
        print(f"Note     : {result['note']}")
    else:
        print("All workflows for this context:")
        print("-" * 60)
        for wf_name in sorted(WORKFLOWS.keys()):
            result = get_command(wf_name)
            print(f"  {wf_name:<20} {result['command']}")


if __name__ == "__main__":
    _main()
