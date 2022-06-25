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
import sys
from typing import Optional, Tuple

import click

from airflow_breeze.commands.main_command import main
from airflow_breeze.global_constants import (
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    MOUNT_ALL,
    GithubEvents,
    github_events,
)
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.common_options import (
    option_airflow_constraints_reference,
    option_answer,
    option_dry_run,
    option_github_repository,
    option_max_age,
    option_python,
    option_timezone,
    option_updated_on_or_after,
    option_verbose,
)
from airflow_breeze.utils.confirm import Answer, user_confirm
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.custom_param_types import BetterChoice
from airflow_breeze.utils.docker_command_utils import (
    check_docker_resources,
    get_env_variables_for_docker_commands,
    get_extra_docker_flags,
    perform_environment_checks,
)
from airflow_breeze.utils.find_newer_dependencies import find_newer_dependencies
from airflow_breeze.utils.image import find_available_ci_image
from airflow_breeze.utils.run_utils import run_command

CI_COMMANDS = {
    "name": "CI commands",
    "commands": [
        "fix-ownership",
        "free-space",
        "resource-check",
        "selective-check",
        "find-newer-dependencies",
    ],
}

CI_PARAMETERS = {
    "breeze selective-check": [
        {
            "name": "Selective check flags",
            "options": [
                "--commit-ref",
                "--pr-labels",
                "--default-branch",
                "--github-event-name",
            ],
        }
    ],
    "breeze find-newer-dependencies": [
        {
            "name": "Find newer dependencies flags",
            "options": [
                "--python",
                "--timezone",
                "--constraints-branch",
                "--updated-on-or-after",
                "--max-age",
            ],
        }
    ],
}


@main.command(name="free-space", help="Free space for jobs run in CI.")
@option_verbose
@option_dry_run
@option_answer
def free_space(verbose: bool, dry_run: bool, answer: str):
    if user_confirm("Are you sure to run free-space and perform cleanup?") == Answer.YES:
        run_command(["sudo", "swapoff", "-a"], verbose=verbose, dry_run=dry_run)
        run_command(["sudo", "rm", "-f", "/swapfile"], verbose=verbose, dry_run=dry_run)
        run_command(["sudo", "apt-get", "clean"], verbose=verbose, dry_run=dry_run, check=False)
        run_command(
            ["docker", "system", "prune", "--all", "--force", "--volumes"], verbose=verbose, dry_run=dry_run
        )
        run_command(["df", "-h"], verbose=verbose, dry_run=dry_run)
        run_command(["docker", "logout", "ghcr.io"], verbose=verbose, dry_run=dry_run, check=False)


@main.command(name="resource-check", help="Check if available docker resources are enough.")
@option_verbose
@option_dry_run
def resource_check(verbose: bool, dry_run: bool):
    perform_environment_checks(verbose=verbose)
    shell_params = ShellParams(verbose=verbose, python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION)
    check_docker_resources(shell_params.airflow_image_name, verbose=verbose, dry_run=dry_run)


@main.command(name="fix-ownership", help="Fix ownership of source files to be same as host user.")
@option_github_repository
@option_verbose
@option_dry_run
def fix_ownership(github_repository: str, verbose: bool, dry_run: bool):
    perform_environment_checks(verbose=verbose)
    shell_params = find_available_ci_image(github_repository, dry_run, verbose)
    extra_docker_flags = get_extra_docker_flags(MOUNT_ALL)
    env = get_env_variables_for_docker_commands(shell_params)
    cmd = [
        "docker",
        "run",
        "-t",
        *extra_docker_flags,
        "--pull",
        "never",
        shell_params.airflow_image_name_with_tag,
        "/opt/airflow/scripts/in_container/run_fix_ownership.sh",
    ]
    run_command(
        cmd, verbose=verbose, dry_run=dry_run, text=True, env=env, check=False, enabled_output_group=True
    )
    # Always succeed
    sys.exit(0)


def get_changed_files(commit_ref: Optional[str], dry_run: bool, verbose: bool) -> Tuple[str, ...]:
    if commit_ref is None:
        return ()
    cmd = [
        "git",
        "diff-tree",
        "--no-commit-id",
        "--name-only",
        "-r",
        commit_ref + "^",
        commit_ref,
    ]
    result = run_command(cmd, dry_run=dry_run, verbose=verbose, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        get_console().print(
            f"[warning] Error when running diff-tree command [/]\n{result.stdout}\n{result.stderr}"
        )
        return ()
    changed_files = tuple(result.stdout.splitlines()) if result.stdout else ()
    get_console().print("\n[info]Changed files:[/]\n")
    get_console().print(changed_files)
    get_console().print()
    return changed_files


@main.command(name="selective-check", help="Checks what kind of tests should be run for an incoming commit.")
@click.option(
    '--commit-ref',
    help="Commit-ish reference to the commit that should be checked",
    envvar='COMMIT_REF',
)
@click.option(
    '--pr-labels',
    help="Space-separate list of labels which are valid for the PR",
    default="",
    envvar="PR_LABELS",
)
@click.option(
    '--default-branch',
    help="Branch against which the PR should be run",
    default="main",
    envvar="DEFAULT_BRANCH",
    show_default=True,
)
@click.option(
    '--github-event-name',
    type=BetterChoice(github_events()),
    default=github_events()[0],
    help="Name of the GitHub event that triggered the check",
    envvar="GITHUB_EVENT_NAME",
    show_default=True,
)
@option_verbose
@option_dry_run
def selective_check(
    commit_ref: Optional[str],
    pr_labels: str,
    default_branch: str,
    github_event_name: str,
    verbose: bool,
    dry_run: bool,
):
    from airflow_breeze.utils.selective_checks import SelectiveChecks

    github_event = GithubEvents(github_event_name)
    if github_event == GithubEvents.PULL_REQUEST:
        changed_files = get_changed_files(commit_ref=commit_ref, dry_run=dry_run, verbose=verbose)
    else:
        changed_files = ()
    sc = SelectiveChecks(
        commit_ref=commit_ref,
        files=changed_files,
        default_branch=default_branch,
        pr_labels=tuple(" ".split(pr_labels)) if pr_labels else (),
        github_event=github_event,
    )
    print(str(sc))


@main.command(name="find-newer-dependencies", help="Finds which dependencies are being upgraded.")
@option_timezone
@option_airflow_constraints_reference
@option_python
@option_updated_on_or_after
@option_max_age
def breeze_find_newer_dependencies(
    airflow_constraints_reference: str, python: str, timezone: str, updated_on_or_after: str, max_age: int
):
    return find_newer_dependencies(
        constraints_branch=airflow_constraints_reference,
        python=python,
        timezone=timezone,
        updated_on_or_after=updated_on_or_after,
        max_age=max_age,
    )
