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
import re
import sys
from subprocess import CompletedProcess

import click
from rich.markup import escape
from rich.table import Table

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH, DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.commands.common_options import option_verbose
from airflow_breeze.commands.main_command import main
from airflow_breeze.global_constants import GithubEvents
from airflow_breeze.utils.console import get_console, get_stderr_console
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH
from airflow_breeze.utils.reproduce_ci import SKIP_LOCAL_REPRODUCTION
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.shared_options import get_verbose
from airflow_breeze.utils.verification_plan import LeanSelectiveChecks, build_local_verification_plan

APACHE_AIRFLOW_URL = re.compile(r"github\.com[:/]apache/airflow(\.git)?/?$")


def _run_git(*args: str) -> CompletedProcess:
    return run_command(["git", *args], capture_output=True, text=True, check=False, cwd=AIRFLOW_ROOT_PATH)


def find_default_base_ref() -> str | None:
    """The target branch on the remote that points at apache/airflow, preferring ``upstream``."""
    remotes = []
    for line in _run_git("config", "--get-regexp", r"^remote\..*\.url$").stdout.splitlines():
        key, _, url = line.partition(" ")
        if APACHE_AIRFLOW_URL.search(url.strip()):
            remotes.append(key.removeprefix("remote.").removesuffix(".url"))
    for remote in sorted(remotes, key=lambda name: (name != "upstream", name)):
        ref = f"{remote}/{AIRFLOW_BRANCH}"
        if _run_git("rev-parse", "--verify", "--quiet", f"refs/remotes/{ref}").returncode == 0:
            return ref
    return None


def has_merged_commits_missing_from(base_ref: str) -> bool:
    """Whether a merge on the branch brought in commits that ``base_ref`` does not have, as after
    GitHub's "Update branch" when the local copy of the target branch was not fetched since."""
    for line in _run_git("rev-list", "--merges", "--parents", f"{base_ref}..HEAD").stdout.splitlines():
        for merged_parent in line.split()[2:]:
            if _run_git("merge-base", "--is-ancestor", merged_parent, base_ref).returncode != 0:
                return True
    return False


def get_changed_files_against(base_ref: str) -> tuple[str, ...]:
    def _git(*args: str) -> list[str]:
        result = _run_git(*args)
        if result.returncode != 0:
            raise click.ClickException(
                f"git {args[0]} failed for base ref {base_ref!r}: {result.stderr.strip()}"
            )
        return result.stdout.splitlines()

    merge_base = _git("merge-base", base_ref, "HEAD")[0]
    # CI lists changed files with diff-tree, which reports a rename as a delete plus an add.
    changed = _git("diff", "--name-only", "--no-renames", merge_base) + _git(
        "ls-files", "--others", "--exclude-standard"
    )
    return tuple(sorted(set(changed)))


@main.command(
    name="verify",
    help="List the local verification CI would require for the current changes. Nothing is executed.",
)
@click.option(
    "--base-ref",
    help=f"Git ref the change will be merged into. Changed files are computed from its merge-base with "
    f"HEAD. Defaults to {AIRFLOW_BRANCH} on the remote that points at apache/airflow, or the local "
    f"{AIRFLOW_BRANCH} branch if there is no such remote.",
)
@click.option(
    "--full",
    is_flag=True,
    help="List everything CI runs for the default matrix cell except static checks, including the full "
    "suite CI adds when a change touches CI tooling or dependency files.",
)
@click.option("--json", "as_json", is_flag=True, help="Print machine-readable JSON instead of a table.")
@option_verbose
@click.pass_context
def verify(ctx: click.Context, base_ref: str | None, full: bool, as_json: bool):
    from airflow_breeze.utils.selective_checks import SelectiveChecks

    if as_json:
        ctx.meta[SKIP_LOCAL_REPRODUCTION] = True
    no_apache_remote = False
    if base_ref is None:
        base_ref = find_default_base_ref()
        if base_ref is None:
            base_ref, no_apache_remote = AIRFLOW_BRANCH, True
    # SelectiveChecks narrates its decisions on stdout; keep stdout for the result only.
    with contextlib.redirect_stdout(sys.stderr if get_verbose() else io.StringIO()):
        changed_files = get_changed_files_against(base_ref)
        kwargs = dict(
            files=changed_files,
            commit_ref="HEAD",
            default_branch=AIRFLOW_BRANCH,
            default_constraints_branch=DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH,
            github_event=GithubEvents.PULL_REQUEST,
        )
        ci = SelectiveChecks(**kwargs)
        sc = ci if full else LeanSelectiveChecks(**kwargs)
        result = build_local_verification_plan(
            sc, changed_files, base_ref, full_tests_needed=ci.full_tests_needed, full=full
        )
    warnings = []
    if no_apache_remote:
        warnings.append(
            f"No git remote points at apache/airflow, so changes are compared with the local "
            f"{AIRFLOW_BRANCH} branch."
        )
    if has_merged_commits_missing_from(base_ref):
        warnings.append(
            f"Your branch has merged commits that are not in {base_ref}, so they are listed as your "
            f"changes. Update {base_ref} (for example with git fetch) and run again."
        )
    if as_json:
        print(json.dumps(result, indent=2))
        for warning in warnings:
            get_stderr_console().print(f"[warning]{warning}[/]")
        return
    console = get_console()
    for warning in warnings:
        console.print(f"[warning]{warning}[/]\n")
    console.print(
        f"[info]{len(changed_files)} changed file(s) against {base_ref}, "
        f"CI default Python {result['default_python_version']}[/]\n"
    )
    table = Table(title="What CI runs for this change" if full else "What to run for this change")
    table.add_column("kind")
    table.add_column("runs_in")
    table.add_column("command", overflow="fold")
    for item in result["items"]:
        table.add_row(item["kind"], item["runs_in"], escape(item["command"]))
    console.print(table)
    console.print(
        "\n[info]Static checks are not listed. Run prek as usual. It picks the hooks for the changed files.[/]"
    )
    if result["full_tests_needed"] and not full:
        console.print(
            "\n[warning]CI also runs the full suite for this change because it touches CI tooling or "
            "dependency files. Pass --full to list it.[/]"
        )
    console.print(
        f"\n[warning]Covers Python {result['default_python_version']} on sqlite only; other Python versions, "
        "Postgres, MySQL, the provider compatibility matrix and CI-only jobs run in CI.[/]"
    )
