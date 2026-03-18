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
"""Helpers for printing local reproduction instructions in CI logs."""

from __future__ import annotations

import os
import shlex
from dataclasses import dataclass
from typing import TYPE_CHECKING

from rich.markup import escape

from airflow_breeze.global_constants import APACHE_AIRFLOW_GITHUB_REPOSITORY
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.run_utils import commit_sha

if TYPE_CHECKING:
    from airflow_breeze.params.build_ci_params import BuildCiParams
    from airflow_breeze.params.shell_params import ShellParams


@dataclass
class ReproductionCommand:
    argv: list[str]
    comment: str | None = None


def build_checkout_reproduction_commands(github_repository: str) -> list[ReproductionCommand]:
    """Build git commands needed to reproduce the current CI checkout locally."""
    github_ref = os.environ.get("GITHUB_REF", "")
    github_ref_parts = github_ref.split("/")
    if len(github_ref_parts) == 4 and github_ref_parts[:2] == ["refs", "pull"]:
        pull_request_ref_kind = github_ref_parts[3]
        return [
            ReproductionCommand(
                argv=[
                    "git",
                    "fetch",
                    f"https://github.com/{github_repository}.git",
                    github_ref,
                ],
                comment=f"Check out the same code as CI (pull request {pull_request_ref_kind} ref)",
            ),
            ReproductionCommand(
                argv=["git", "checkout", "FETCH_HEAD"],
            ),
        ]

    current_commit_sha = os.environ.get("GITHUB_SHA") or os.environ.get("COMMIT_SHA") or commit_sha()
    if not current_commit_sha or current_commit_sha == "COMMIT_SHA_NOT_FOUND":
        return []
    return [
        ReproductionCommand(
            argv=["git", "checkout", current_commit_sha],
            comment="Check out the same commit",
        )
    ]


def build_ci_image_reproduction_command(command_params: ShellParams | BuildCiParams) -> ReproductionCommand:
    """Build the CI image preparation command for local reproduction."""
    # Current CI jobs restore images from stash keys rather than GitHub Actions artifacts,
    # so building locally is the reliable reproduction path.
    command = ["breeze", "ci-image", "build"]
    if command_params.github_repository != APACHE_AIRFLOW_GITHUB_REPOSITORY:
        command.extend(["--github-repository", command_params.github_repository])
    command.extend(["--platform", command_params.platform, "--python", command_params.python])
    return ReproductionCommand(
        argv=command,
        comment="Build the CI image locally",
    )


def build_local_reproduction_commands(
    command_params: ShellParams | BuildCiParams,
    main_command: ReproductionCommand,
) -> list[ReproductionCommand]:
    """Build the ordered list of local reproduction commands."""
    commands = build_checkout_reproduction_commands(command_params.github_repository)
    commands.append(build_ci_image_reproduction_command(command_params))
    commands.append(main_command)
    return commands


def should_print_local_reproduction() -> bool:
    """Return True when local reproduction instructions should be printed."""
    return (
        os.environ.get("CI", "").lower() == "true" and os.environ.get("GITHUB_ACTIONS", "").lower() == "true"
    )


def print_local_reproduction(commands: list[ReproductionCommand]) -> None:
    """Print local reproduction commands in CI logs."""
    if not should_print_local_reproduction() or not commands:
        return
    lines: list[str] = []
    step_number = 0
    for command in commands:
        if command.comment:
            step_number += 1
            lines.append(f"# {step_number}. {command.comment}")
        lines.append(shlex.join(command.argv))
        lines.append("")
    rendered = "\n".join(lines).rstrip()
    get_console().print("\n[warning]HOW TO REPRODUCE LOCALLY[/]\n")
    get_console().print(f"[info]{escape(rendered)}[/]\n", soft_wrap=True)
