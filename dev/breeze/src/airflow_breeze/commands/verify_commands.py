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

import contextlib
import io
import json
import sys

import click
from rich.markup import escape
from rich.table import Table

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH, DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.commands.common_options import option_verbose
from airflow_breeze.commands.main_command import main
from airflow_breeze.global_constants import GithubEvents
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.shared_options import get_verbose
from airflow_breeze.utils.verification_plan import build_local_verification_plan


def get_changed_files_against(base_ref: str) -> tuple[str, ...]:
    def _git(*args: str) -> list[str]:
        result = run_command(
            ["git", *args], capture_output=True, text=True, check=False, cwd=AIRFLOW_ROOT_PATH
        )
        if result.returncode != 0:
            raise click.ClickException(
                f"git {args[0]} failed for base ref {base_ref!r}: {result.stderr.strip()}"
            )
        return result.stdout.splitlines()

    merge_base = _git("merge-base", base_ref, "HEAD")[0]
    changed = _git("diff", "--name-only", merge_base) + _git("ls-files", "--others", "--exclude-standard")
    return tuple(sorted(set(changed)))


@main.command(
    name="verify",
    help="List the local verification CI would require for the current changes. Nothing is executed.",
)
@click.option(
    "--base-ref",
    default=AIRFLOW_BRANCH,
    show_default=True,
    help="Git ref the change will be merged into. Changed files are computed from its merge-base with HEAD.",
)
@click.option("--json", "as_json", is_flag=True, help="Print machine-readable JSON instead of a table.")
@option_verbose
def verify(base_ref: str, as_json: bool):
    from airflow_breeze.utils.selective_checks import SelectiveChecks

    # SelectiveChecks narrates its decisions on stdout; keep stdout for the result only.
    with contextlib.redirect_stdout(sys.stderr if get_verbose() else io.StringIO()):
        changed_files = get_changed_files_against(base_ref)
        sc = SelectiveChecks(
            files=changed_files,
            commit_ref="HEAD",
            default_branch=AIRFLOW_BRANCH,
            default_constraints_branch=DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH,
            github_event=GithubEvents.PULL_REQUEST,
        )
        result = build_local_verification_plan(sc, changed_files, base_ref)
    if as_json:
        print(json.dumps(result, indent=2))
        return
    console = get_console()
    console.print(
        f"[info]{len(changed_files)} changed file(s) against {base_ref}, "
        f"CI default Python {result['default_python_version']}[/]\n"
    )
    table = Table(title="Run locally to match CI", show_lines=False)
    for column in ("kind", "runs_in", "required", "command"):
        table.add_column(column)
    for item in result["items"]:
        table.add_row(item["kind"], item["runs_in"], str(item["required"]).lower(), escape(item["command"]))
    console.print(table)
    if result["full_tests_needed"]:
        console.print(
            "\n[warning]CI runs the full suite for this change because it touches CI tooling or dependency "
            "files. Run what covers your change locally and leave the rest to CI.[/]"
        )
    console.print(
        f"\n[warning]Covers Python {result['default_python_version']} on sqlite only; other Python versions, "
        "Postgres, MySQL, the provider compatibility matrix and CI-only jobs run in CI.[/]"
    )
