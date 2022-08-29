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
import ast
import json
import os
import platform
import re
import subprocess
import sys
import tempfile
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

import click

from airflow_breeze.global_constants import (
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    MOUNT_ALL,
    RUNS_ON_PUBLIC_RUNNER,
    RUNS_ON_SELF_HOSTED_RUNNER,
    GithubEvents,
    github_events,
)
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.click_utils import BreezeGroup
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
from airflow_breeze.utils.github_actions import get_ga_output
from airflow_breeze.utils.image import find_available_ci_image
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT
from airflow_breeze.utils.run_utils import run_command


@click.group(cls=BreezeGroup, name='ci', help='Tools that CI workflows use to cleanup/manage CI environment')
def ci_group():
    pass


@ci_group.command(name="free-space", help="Free space for jobs run in CI.")
@option_verbose
@option_dry_run
@option_answer
def free_space(verbose: bool, dry_run: bool, answer: str):
    if user_confirm("Are you sure to run free-space and perform cleanup?") == Answer.YES:
        run_command(["sudo", "swapoff", "-a"], verbose=verbose, dry_run=dry_run)
        run_command(["sudo", "rm", "-f", "/swapfile"], verbose=verbose, dry_run=dry_run)
        for file in Path(tempfile.gettempdir()).iterdir():
            if file.name.startswith("parallel"):
                run_command(
                    ["sudo", "rm", "-rvf", os.fspath(file)],
                    verbose=verbose,
                    dry_run=dry_run,
                    check=False,
                    title=f"rm -rvf {file}",
                )
        run_command(["sudo", "apt-get", "clean"], verbose=verbose, dry_run=dry_run, check=False)
        run_command(
            ["docker", "system", "prune", "--all", "--force", "--volumes"], verbose=verbose, dry_run=dry_run
        )
        run_command(["df", "-h"], verbose=verbose, dry_run=dry_run)
        run_command(["docker", "logout", "ghcr.io"], verbose=verbose, dry_run=dry_run, check=False)


@ci_group.command(name="resource-check", help="Check if available docker resources are enough.")
@option_verbose
@option_dry_run
def resource_check(verbose: bool, dry_run: bool):
    perform_environment_checks(verbose=verbose)
    shell_params = ShellParams(verbose=verbose, python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION)
    check_docker_resources(shell_params.airflow_image_name, verbose=verbose, dry_run=dry_run)


HOME_DIR = Path(os.path.expanduser('~')).resolve()

DIRECTORIES_TO_FIX = [
    AIRFLOW_SOURCES_ROOT,
    HOME_DIR / ".aws",
    HOME_DIR / ".azure",
    HOME_DIR / ".config/gcloud",
    HOME_DIR / ".docker",
    AIRFLOW_SOURCES_ROOT,
]


def fix_ownership_for_file(file: Path, dry_run: bool, verbose: bool):
    get_console().print(f"[info]Fixing ownership of {file}")
    result = run_command(
        ["sudo", "chown", f"{os.getuid}:{os.getgid()}", str(file.resolve())],
        check=False,
        stderr=subprocess.STDOUT,
        dry_run=dry_run,
        verbose=verbose,
    )
    if result.returncode != 0:
        get_console().print(f"[warning]Could not fix ownership for {file}: {result.stdout}")


def fix_ownership_for_path(path: Path, dry_run: bool, verbose: bool):
    if path.is_dir():
        for p in Path(path).rglob('*'):
            if p.owner == 'root':
                fix_ownership_for_file(p, dry_run=dry_run, verbose=verbose)
    else:
        if path.owner == 'root':
            fix_ownership_for_file(path, dry_run=dry_run, verbose=verbose)


def fix_ownership_without_docker(dry_run: bool, verbose: bool):
    for directory_to_fix in DIRECTORIES_TO_FIX:
        fix_ownership_for_path(directory_to_fix, dry_run=dry_run, verbose=verbose)


@ci_group.command(name="fix-ownership", help="Fix ownership of source files to be same as host user.")
@click.option(
    '--use-sudo',
    is_flag=True,
    help="Use sudo instead of docker image to fix the ownership. You need to be a `sudoer` to run it",
    envvar='USE_SUDO',
)
@option_github_repository
@option_verbose
@option_dry_run
def fix_ownership(github_repository: str, use_sudo: bool, verbose: bool, dry_run: bool):
    system = platform.system().lower()
    if system != 'linux':
        get_console().print(
            f"[warning]You should only need to run fix-ownership on Linux and your system is {system}"
        )
        sys.exit(0)
    if use_sudo:
        get_console().print("[info]Fixing ownership using sudo.")
        fix_ownership_without_docker(dry_run=dry_run, verbose=verbose)
        sys.exit(0)
    get_console().print("[info]Fixing ownership using docker.")
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
    run_command(cmd, verbose=verbose, dry_run=dry_run, text=True, env=env, check=False)
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


@ci_group.command(
    name="selective-check", help="Checks what kind of tests should be run for an incoming commit."
)
@click.option(
    '--commit-ref',
    help="Commit-ish reference to the commit that should be checked",
    envvar='COMMIT_REF',
)
@click.option(
    '--pr-labels',
    help="Python array formatted PR labels assigned to the PR",
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
    '--default-constraints-branch',
    help="Constraints Branch against which the PR should be run",
    default="constraints-main",
    envvar="DEFAULT_CONSTRAINTS_BRANCH",
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
    default_constraints_branch: str,
    github_event_name: str,
    verbose: bool,
    dry_run: bool,
):
    from airflow_breeze.utils.selective_checks import SelectiveChecks

    github_event = GithubEvents(github_event_name)
    if commit_ref is not None:
        changed_files = get_changed_files(commit_ref=commit_ref, dry_run=dry_run, verbose=verbose)
    else:
        changed_files = ()
    sc = SelectiveChecks(
        commit_ref=commit_ref,
        files=changed_files,
        default_branch=default_branch,
        default_constraints_branch=default_constraints_branch,
        pr_labels=tuple(ast.literal_eval(pr_labels)) if pr_labels else (),
        github_event=github_event,
    )
    print(str(sc))


@ci_group.command(name="find-newer-dependencies", help="Finds which dependencies are being upgraded.")
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


TEST_BRANCH_MATCHER = re.compile(r"^v.*test$")


class WorkflowInfo(NamedTuple):
    event_name: str
    pull_request_labels: List[str]
    target_repo: str
    head_repo: str
    ref: Optional[str]
    ref_name: Optional[str]
    pr_number: Optional[int]

    def print_ga_outputs(self):
        print(get_ga_output(name="pr_labels", value=str(self.pull_request_labels)))
        print(get_ga_output(name="target_repo", value=self.target_repo))
        print(get_ga_output(name="head_repo", value=self.head_repo))
        print(get_ga_output(name="pr_number", value=str(self.pr_number) if self.pr_number else ""))
        print(get_ga_output(name="event_name", value=str(self.event_name)))
        print(get_ga_output(name="runs-on", value=self.get_runs_on()))
        print(get_ga_output(name='in-workflow-build', value=self.in_workflow_build()))
        print(get_ga_output(name="build-job-description", value=self.get_build_job_description()))
        print(get_ga_output(name="merge-run", value=self.is_merge_run()))
        print(get_ga_output(name="run-coverage", value=self.run_coverage()))

    def get_runs_on(self) -> str:
        for label in self.pull_request_labels:
            if "use public runners" in label:
                get_console().print("[info]Force running on public runners")
                return RUNS_ON_PUBLIC_RUNNER
        if not os.environ.get("AIRFLOW_SELF_HOSTED_RUNNER"):
            return RUNS_ON_PUBLIC_RUNNER
        return RUNS_ON_SELF_HOSTED_RUNNER

    def in_workflow_build(self) -> str:
        if self.event_name == "push" or self.head_repo == "apache/airflow":
            return "true"
        return "false"

    def get_build_job_description(self) -> str:
        if self.in_workflow_build() == 'true':
            return "Build"
        return "Skip Build (look in pull_request_target)"

    def is_merge_run(self) -> str:
        if (
            self.event_name == 'push'
            and self.head_repo == "apache/airflow"
            and self.ref_name
            and (self.ref_name == "main" or TEST_BRANCH_MATCHER.match(self.ref_name))
        ):
            return "true"
        return "false"

    def run_coverage(self) -> str:
        if self.event_name == 'push' and self.head_repo == "apache/airflow" and self.ref == "refs/head/main":
            return "true"
        return "false"


def workflow_info(context: str) -> WorkflowInfo:
    ctx: Dict[Any, Any] = json.loads(context)
    event_name = ctx.get("event_name")
    if not event_name:
        get_console().print(f"[error]Missing event_name in: {ctx}")
        sys.exit(1)
    pull_request_labels = []
    head_repo = ""
    target_repo = ""
    pr_number: Optional[int] = None
    ref_name = ctx.get("ref_name")
    ref = ctx.get("ref")
    if event_name == "pull_request":
        event = ctx.get('event')
        if event:
            pr = event.get('pull_request')
            if pr:
                labels = pr.get('labels')
                if labels:
                    for label in labels:
                        pull_request_labels.append(label['name'])
                target_repo = pr["base"]["repo"]["full_name"]
                head_repo = pr["head"]["repo"]["full_name"]
                pr_number = pr["number"]
    elif event_name == 'push':
        target_repo = ctx["repository"]
        head_repo = ctx["repository"]
        event_name = ctx["event_name"]
    elif event_name == 'schedule':
        target_repo = ctx["repository"]
        head_repo = ctx["repository"]
        event_name = ctx["event_name"]
    elif event_name == 'pull_request_target':
        target_repo = ctx["repository"]
        head_repo = ctx["repository"]
        event_name = ctx["event_name"]
    else:
        get_console().print(f"[error]Wrong event name: {event_name}")
        sys.exit(1)
    return WorkflowInfo(
        event_name=event_name,
        pull_request_labels=pull_request_labels,
        target_repo=target_repo,
        head_repo=head_repo,
        pr_number=pr_number,
        ref=ref,
        ref_name=ref_name,
    )


@ci_group.command(
    name="get-workflow-info",
    help="Retrieve information about current workflow in the CI"
    "and produce github actions output extracted from it.",
)
@click.option('--github-context', help="JSON-formatted github context", envvar='GITHUB_CONTEXT')
@click.option(
    '--github-context-input',
    help="file input (might be `-`) with JSON-formatted github context",
    type=click.File('rt'),
    envvar='GITHUB_CONTEXT_INPUT',
)
def get_workflow_info(github_context: str, github_context_input: StringIO):
    if github_context and github_context_input:
        get_console().print(
            "[error]You can only specify one of the two --github-context or --github-context-file"
        )
        sys.exit(1)
    if github_context:
        context = github_context
    elif github_context_input:
        context = github_context_input.read()
    else:
        get_console().print(
            "[error]You must specify one of the two --github-context or --github-context-file"
        )
        sys.exit(1)
    wi = workflow_info(context=context)
    wi.print_ga_outputs()
