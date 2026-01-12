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

import ast
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Iterable
from io import StringIO
from pathlib import Path
from typing import Any, NamedTuple

import click

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH, DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.commands.common_options import (
    option_answer,
    option_dry_run,
    option_github_repository,
    option_verbose,
)
from airflow_breeze.global_constants import (
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    PUBLIC_AMD_RUNNERS,
    GithubEvents,
    github_events,
)
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.confirm import Answer, user_confirm
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.custom_param_types import BetterChoice
from airflow_breeze.utils.docker_command_utils import (
    check_docker_resources,
    fix_ownership_using_docker,
    perform_environment_checks,
)
from airflow_breeze.utils.path_utils import AIRFLOW_HOME_PATH, AIRFLOW_ROOT_PATH
from airflow_breeze.utils.run_utils import run_command


@click.group(cls=BreezeGroup, name="ci", help="Tools that CI workflows use to cleanup/manage CI environment")
def ci_group():
    pass


@ci_group.command(name="free-space", help="Free space for jobs run in CI.")
@option_verbose
@option_dry_run
@option_answer
def free_space():
    if user_confirm("Are you sure to run free-space and perform cleanup?") == Answer.YES:
        run_command(["sudo", "swapoff", "-a"])
        run_command(["sudo", "rm", "-f", "/swapfile"])
        for file in Path(tempfile.gettempdir()).iterdir():
            if file.name.startswith("parallel"):
                run_command(
                    ["sudo", "rm", "-rvf", os.fspath(file)],
                    check=False,
                    title=f"rm -rvf {file}",
                )
        run_command(["sudo", "apt-get", "clean"], check=False)
        run_command(["docker", "system", "prune", "--all", "--force", "--volumes"])
        run_command(["df", "-h"])
        run_command(["docker", "logout", "ghcr.io"], check=False)
        shutil.rmtree(AIRFLOW_HOME_PATH, ignore_errors=True)
        AIRFLOW_HOME_PATH.mkdir(exist_ok=True, parents=True)
        run_command(["pip", "uninstall", "apache-airflow", "--yes"], check=False)


@ci_group.command(name="resource-check", help="Check if available docker resources are enough.")
@option_verbose
@option_dry_run
def resource_check():
    perform_environment_checks()
    shell_params = ShellParams(python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION)
    check_docker_resources(shell_params.airflow_image_name)


HOME_DIR = Path(os.path.expanduser("~")).resolve()

DIRECTORIES_TO_FIX = [
    AIRFLOW_ROOT_PATH,
    HOME_DIR / ".aws",
    HOME_DIR / ".azure",
    HOME_DIR / ".config/gcloud",
    HOME_DIR / ".docker",
]


def fix_ownership_for_file(file: Path):
    get_console().print(f"[info]Fixing ownership of {file}")
    result = run_command(
        ["sudo", "chown", f"{os.getuid}:{os.getgid()}", str(file.resolve())],
        check=False,
        stderr=subprocess.STDOUT,
    )
    if result.returncode != 0:
        get_console().print(f"[warning]Could not fix ownership for {file}: {result.stdout}")


def fix_ownership_for_path(path: Path):
    if path.is_dir():
        for p in Path(path).rglob("*"):
            if p.owner == "root":
                fix_ownership_for_file(p)
    else:
        if path.owner == "root":
            fix_ownership_for_file(path)


def fix_ownership_without_docker():
    for directory_to_fix in DIRECTORIES_TO_FIX:
        fix_ownership_for_path(directory_to_fix)


@ci_group.command(name="fix-ownership", help="Fix ownership of source files to be same as host user.")
@click.option(
    "--use-sudo",
    is_flag=True,
    help="Use sudo instead of docker image to fix the ownership. You need to be a `sudoer` to run it",
    envvar="USE_SUDO",
)
@option_verbose
@option_dry_run
def fix_ownership(use_sudo: bool):
    system = platform.system().lower()
    if system != "linux":
        get_console().print(
            f"[warning]You should only need to run fix-ownership on Linux and your system is {system}"
        )
        sys.exit(0)
    if use_sudo:
        get_console().print("[info]Fixing ownership using sudo.")
        fix_ownership_without_docker()
        sys.exit(0)
    get_console().print("[info]Fixing ownership using docker.")
    fix_ownership_using_docker(quiet=False)
    # Always succeed
    sys.exit(0)


def get_changed_files(commit_ref: str | None) -> tuple[str, ...]:
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
    result = run_command(cmd, check=False, capture_output=True, text=True)
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
    "--commit-ref",
    help="Commit-ish reference to the commit that should be checked",
    envvar="COMMIT_REF",
)
@click.option(
    "--pr-labels",
    help="Python array formatted PR labels assigned to the PR",
    default="",
    envvar="PR_LABELS",
)
@click.option(
    "--default-branch",
    help="Branch against which the PR should be run",
    default=AIRFLOW_BRANCH,
    envvar="DEFAULT_BRANCH",
    show_default=True,
)
@click.option(
    "--default-constraints-branch",
    help="Constraints Branch against which the PR should be run",
    default=DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH,
    envvar="DEFAULT_CONSTRAINTS_BRANCH",
    show_default=True,
)
@click.option(
    "--github-event-name",
    type=BetterChoice(github_events()),
    default=github_events()[0],
    help="Name of the GitHub event that triggered the check",
    envvar="GITHUB_EVENT_NAME",
    show_default=True,
)
@option_github_repository
@click.option(
    "--github-actor",
    help="Actor that triggered the event (GitHub user)",
    envvar="GITHUB_ACTOR",
    type=str,
    default="",
)
@click.option(
    "--github-context",
    help="GitHub context (JSON formatted) passed by GitHub Actions",
    envvar="GITHUB_CONTEXT",
    type=str,
    default="",
)
@option_verbose
@option_dry_run
def selective_check(
    commit_ref: str | None,
    pr_labels: str,
    default_branch: str,
    default_constraints_branch: str,
    github_event_name: str,
    github_repository: str,
    github_actor: str,
    github_context: str,
):
    try:
        from airflow_breeze.utils.selective_checks import SelectiveChecks

        github_context_dict = json.loads(github_context) if github_context else {}
        github_event = GithubEvents(github_event_name)
        if commit_ref is not None:
            changed_files = get_changed_files(commit_ref=commit_ref)
        else:
            changed_files = ()
        sc = SelectiveChecks(
            commit_ref=commit_ref,
            files=changed_files,
            default_branch=default_branch,
            default_constraints_branch=default_constraints_branch,
            pr_labels=tuple(ast.literal_eval(pr_labels)) if pr_labels else (),
            github_event=github_event,
            github_repository=github_repository,
            github_actor=github_actor,
            github_context_dict=github_context_dict,
        )
        print(str(sc), file=sys.stderr)
    except Exception:
        get_console().print_exception(show_locals=True)
        sys.exit(1)


TEST_BRANCH_MATCHER = re.compile(r"^v.*test$")


class WorkflowInfo(NamedTuple):
    event_name: str
    pull_request_labels: list[str]
    target_repo: str
    head_repo: str
    ref: str | None
    ref_name: str | None
    pr_number: int | None
    head_ref: str | None = None

    def get_all_ga_outputs(self) -> Iterable[str]:
        from airflow_breeze.utils.github import get_ga_output

        yield get_ga_output(name="pr_labels", value=str(self.pull_request_labels))
        yield get_ga_output(name="target_repo", value=self.target_repo)
        yield get_ga_output(name="head_repo", value=self.head_repo)
        yield get_ga_output(name="pr_number", value=str(self.pr_number) if self.pr_number else "")
        yield get_ga_output(name="event_name", value=str(self.event_name))
        yield get_ga_output(name="runs-on", value=self.get_runs_on())
        yield get_ga_output(name="canary-run", value=self.is_canary_run())
        yield get_ga_output(name="run-coverage", value=self.run_coverage())
        yield get_ga_output(name="head-ref", value=self.head_ref)

    def print_all_ga_outputs(self):
        for output in self.get_all_ga_outputs():
            print(output, file=sys.stderr)

    def get_runs_on(self) -> str:
        for label in self.pull_request_labels:
            if "use public runners" in label:
                get_console().print("[info]Force running on public runners")
                return PUBLIC_AMD_RUNNERS
        return PUBLIC_AMD_RUNNERS

    def is_canary_run(self) -> str:
        if (
            self.event_name
            in [
                GithubEvents.PUSH.value,
                GithubEvents.WORKFLOW_DISPATCH.value,
                GithubEvents.SCHEDULE.value,
            ]
            and self.head_repo == "apache/airflow"
            and self.ref_name
            and (self.ref_name == "main" or TEST_BRANCH_MATCHER.match(self.ref_name))
        ):
            return "true"
        if "canary" in self.pull_request_labels:
            return "true"
        return "false"

    def run_coverage(self) -> str:
        if (
            self.event_name == GithubEvents.PUSH.value
            and self.head_repo == "apache/airflow"
            and self.ref == "refs/heads/main"
        ):
            return "true"
        return "false"


def workflow_info(context: str) -> WorkflowInfo:
    ctx: dict[Any, Any] = json.loads(context)
    event_name = ctx.get("event_name")
    if not event_name:
        get_console().print(f"[error]Missing event_name in: {ctx}")
        sys.exit(1)
    pull_request_labels = []
    head_repo = ""
    target_repo = ""
    pr_number: int | None = None
    ref_name = ctx.get("ref_name")
    ref = ctx.get("ref")
    head_ref = ctx.get("head_ref")
    if event_name == GithubEvents.PULL_REQUEST.value:
        event = ctx.get("event")
        if event:
            pr = event.get(GithubEvents.PULL_REQUEST.value)
            if pr:
                labels = pr.get("labels")
                if labels:
                    for label in labels:
                        pull_request_labels.append(label["name"])
                target_repo = pr["base"]["repo"]["full_name"]
                head_repo = pr["head"]["repo"]["full_name"]
                pr_number = pr["number"]
    elif event_name == GithubEvents.PUSH.value:
        target_repo = ctx["repository"]
        head_repo = ctx["repository"]
        event_name = ctx["event_name"]
    elif event_name == GithubEvents.SCHEDULE.value:
        target_repo = ctx["repository"]
        head_repo = ctx["repository"]
        event_name = ctx["event_name"]
    elif event_name == GithubEvents.WORKFLOW_DISPATCH.value:
        target_repo = ctx["repository"]
        head_repo = ctx["repository"]
        event_name = ctx["event_name"]
    elif event_name == GithubEvents.PULL_REQUEST_TARGET.value:
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
        head_ref=head_ref,
    )


@ci_group.command(
    name="get-workflow-info",
    help="Retrieve information about current workflow in the CI"
    "and produce github actions output extracted from it.",
)
@click.option("--github-context", help="JSON-formatted github context", envvar="GITHUB_CONTEXT")
@click.option(
    "--github-context-input",
    help="file input (might be `-`) with JSON-formatted github context",
    type=click.File("rt"),
    envvar="GITHUB_CONTEXT_INPUT",
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
    wi.print_all_ga_outputs()


@ci_group.command(
    name="upgrade",
    help="Perform important upgrade steps of the CI environment. And create a PR",
)
@click.option(
    "--target-branch",
    default=AIRFLOW_BRANCH,
    help="Branch to work on and make PR against (e.g., 'main' or 'vX-Y-test')",
    show_default=True,
)
@click.option(
    "--create-pr/--no-create-pr",
    default=None,
    help="Automatically create a PR with the upgrade changes (if not specified, will ask)",
    is_flag=True,
)
@click.option(
    "--switch-to-base/--no-switch-to-base",
    default=None,
    help="Automatically switch to the base branch if not already on it (if not specified, will ask)",
    is_flag=True,
)
@option_answer
@option_verbose
@option_dry_run
def upgrade(target_branch: str, create_pr: bool | None, switch_to_base: bool | None):
    # Validate target_branch pattern
    target_branch_pattern = re.compile(r"^(main|v\d+-\d+-test)$")
    if not target_branch_pattern.match(target_branch):
        get_console().print(
            f"[error]Invalid target branch: '{target_branch}'. "
            "Must be 'main' or follow pattern 'vX-Y-test' where X and Y are numbers (e.g., 'v2-10-test').[/]"
        )
        sys.exit(1)

    # Check if we're on the main branch
    branch_result = run_command(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True, check=False
    )
    current_branch = branch_result.stdout.strip() if branch_result.returncode == 0 else ""

    # Store the original branch/commit to restore later if needed
    original_branch = current_branch
    original_commit_result = run_command(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False
    )
    original_commit = original_commit_result.stdout.strip() if original_commit_result.returncode == 0 else ""

    # Check if the working directory is clean
    status_result = run_command(["git", "status", "--porcelain"], capture_output=True, text=True, check=False)
    is_clean = status_result.returncode == 0 and not status_result.stdout.strip()

    # Check if we have the apache remote and get its name
    remote_result = run_command(["git", "remote", "-v"], capture_output=True, text=True, check=False)
    apache_remote_name = None
    origin_remote_name = None
    origin_repo = None  # Store the user's fork repo (e.g., "username/airflow")

    if remote_result.returncode == 0:
        # Parse remote output to find apache/airflow remote and origin remote
        # Format: remote_name\turl (fetch|push)
        for line in remote_result.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 2:
                remote_name = parts[0]
                remote_url = parts[1]
                if "apache/airflow" in remote_url and apache_remote_name is None:
                    apache_remote_name = remote_name
                # Also track origin remote for pushing
                if remote_name == "origin" and origin_remote_name is None:
                    origin_remote_name = remote_name
                    # Extract repo from origin URL (supports both HTTPS and SSH formats)
                    # HTTPS: https://github.com/username/airflow.git
                    # SSH: git@github.com:username/airflow.git
                    if "github.com" in remote_url:
                        if "git@github.com:" in remote_url:
                            # SSH format
                            repo_part = remote_url.split("git@github.com:")[1]
                        elif "github.com/" in remote_url:
                            # HTTPS format
                            repo_part = remote_url.split("github.com/")[1]
                        else:
                            repo_part = None

                        if repo_part:
                            # Remove .git suffix if present
                            origin_repo = repo_part.replace(".git", "").strip()

    has_apache_remote = apache_remote_name is not None

    # Check if we're up to date with apache/airflow on the specified branch
    if has_apache_remote:
        # Fetch apache remote to get latest info
        run_command(["git", "fetch", apache_remote_name], check=False)

        # Check if the target branch exists in the apache remote
        branch_exists = run_command(
            ["git", "rev-parse", "--verify", f"{apache_remote_name}/{target_branch}"],
            capture_output=True,
            check=False,
        )

        if branch_exists.returncode != 0:
            get_console().print(
                f"[error]Target branch '{target_branch}' does not exist in remote '{apache_remote_name}'.[/]"
            )
            sys.exit(1)

        # Check if current HEAD matches apache_remote/<branch>
        local_head = run_command(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False)
        remote_head = run_command(
            ["git", "rev-parse", f"{apache_remote_name}/{target_branch}"],
            capture_output=True,
            text=True,
            check=False,
        )

        at_apache_branch = (
            current_branch == target_branch
            and local_head.returncode == 0
            and remote_head.returncode == 0
            and local_head.stdout.strip() == remote_head.stdout.strip()
        )
    else:
        at_apache_branch = False
        get_console().print(
            "[warning]No apache remote found. The command expects remote pointing to apache/airflow[/]"
        )

    # Track whether user chose to reset to target branch
    user_switched_to_target = False

    if not at_apache_branch or not is_clean:
        get_console().print()
        if not at_apache_branch:
            get_console().print(
                f"[warning]You are not at the top of apache/airflow {target_branch} branch.[/]"
            )
            get_console().print(f"[info]Current branch: {current_branch}[/]")
        if not is_clean:
            get_console().print("[warning]Your repository has uncommitted changes.[/]")
        get_console().print()

        # Determine whether to switch to base branch
        should_switch = switch_to_base
        if should_switch is None:
            # Not specified, ask the user
            get_console().print(
                f"[warning]Attempting to switch to switch to {target_branch}. "
                f"This will lose not committed code.[/]\n\n"
                "NO will continue to get changes on top of current branch, QUIT will exit."
            )
            response = user_confirm("Do you want to switch")
            if response == Answer.YES:
                should_switch = True
            elif response == Answer.QUIT:
                get_console().print(
                    f"[error]Upgrade cancelled. Please ensure you are on apache/airflow {target_branch} with a clean repository.[/]"
                )
                sys.exit(1)
            else:
                should_switch = False

        if should_switch:
            user_switched_to_target = True
            get_console().print(f"[info]Resetting to apache/airflow {target_branch}...[/]")
            if current_branch != target_branch:
                run_command(["git", "checkout", target_branch])
            run_command(["git", "fetch", apache_remote_name])
            run_command(["git", "reset", "--hard", f"{apache_remote_name}/{target_branch}"])
            run_command(["git", "clean", "-fdx"])
            get_console().print(
                f"[success]Successfully reset to apache/airflow {target_branch} and cleaned repository.[/]"
            )
        else:
            get_console().print(
                f"[info]Continuing with current branch {current_branch}. Changes will be on top of it.[/]"
            )

    get_console().print("[info]Running upgrade of important CI environment.[/]")

    # Get GitHub token from gh CLI and set it in environment copy
    gh_token_result = run_command(
        ["gh", "auth", "token"],
        capture_output=True,
        text=True,
        check=False,
    )

    # Create a copy of the environment to pass to commands
    command_env = os.environ.copy()

    if gh_token_result.returncode == 0 and gh_token_result.stdout.strip():
        github_token = gh_token_result.stdout.strip()
        command_env["GITHUB_TOKEN"] = github_token
        get_console().print("[success]GitHub token retrieved from gh CLI and set in environment.[/]")
    else:
        get_console().print(
            "[warning]Could not retrieve GitHub token from gh CLI. "
            "Commands may fail if they require authentication.[/]"
        )

    # Define all upgrade commands to run (all run with check=False to continue on errors)
    upgrade_commands = [
        "prek autoupdate --cooldown-days 4 --freeze",
        "prek autoupdate --bleeding-edge --freeze --repo https://github.com/Lucas-C/pre-commit-hooks",
        "prek autoupdate --bleeding-edge --freeze --repo https://github.com/eclipse-csi/octopin",
        "prek --all-files --verbose --hook-stage manual pin-versions",
        "prek --all-files --show-diff-on-failure --color always --verbose --hook-stage manual update-chart-dependencies",
        "prek --all-files --show-diff-on-failure --color always --verbose --hook-stage manual upgrade-important-versions",
    ]

    # Execute all upgrade commands with the environment containing GitHub token
    for command in upgrade_commands:
        run_command(command.split(), check=False, env=command_env)

    res = run_command(["git", "diff", "--exit-code"], check=False)
    if res.returncode == 0:
        get_console().print("[success]No changes were made during the upgrade. Exiting[/]")
        sys.exit(0)

    # Determine whether to create a PR
    should_create_pr = create_pr
    if should_create_pr is None:
        # Not specified, ask the user
        should_create_pr = user_confirm("Do you want to create a PR with the upgrade changes?") == Answer.YES

    if should_create_pr:
        # Get current HEAD commit hash for unique branch name
        head_result = run_command(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=False
        )
        commit_hash = head_result.stdout.strip() if head_result.returncode == 0 else "unknown"
        branch_name = f"ci-upgrade-{commit_hash}"

        # Check if branch already exists and delete it
        branch_check = run_command(
            ["git", "rev-parse", "--verify", branch_name], capture_output=True, check=False
        )
        if branch_check.returncode == 0:
            get_console().print(f"[info]Branch {branch_name} already exists, deleting it...[/]")
            run_command(["git", "branch", "-D", branch_name])

        run_command(["git", "checkout", "-b", branch_name])
        run_command(["git", "add", "."])
        run_command(["git", "commit", "-m", "CI: Upgrade important CI environment"])

        # Push the branch to origin (use detected origin or fallback to 'origin')
        push_remote = origin_remote_name if origin_remote_name else "origin"
        get_console().print(f"[info]Pushing branch {branch_name} to {push_remote}...[/]")
        push_result = run_command(
            ["git", "push", "-u", push_remote, branch_name, "--force"],
            capture_output=True,
            text=True,
            check=False,
        )
        if push_result.returncode != 0:
            get_console().print(
                f"[error]Failed to push branch:\n{push_result.stdout}\n{push_result.stderr}[/]"
            )
            sys.exit(1)
        get_console().print(f"[success]Branch {branch_name} pushed to {push_remote}.[/]")

        # Create PR from the pushed branch
        # gh pr create needs --head in format "username:branch" when creating a PR from a fork
        # Extract username from origin_repo (e.g., "username/airflow" -> "username")
        if origin_repo:
            owner = origin_repo.split("/")[0]
            head_ref = f"{owner}:{branch_name}"
            get_console().print(
                f"[info]Creating PR from {origin_repo} branch {branch_name} to apache/airflow {target_branch}...[/]"
            )
        else:
            # Fallback to just branch name if we couldn't determine the fork
            head_ref = branch_name
            get_console().print("[warning]Could not determine fork repository. Using branch name only.[/]")

        pr_result = run_command(
            [
                "gh",
                "pr",
                "create",
                "-w",
                "--repo",
                "apache/airflow",
                "--head",
                head_ref,
                "--base",
                target_branch,
                "--title",
                f"[{target_branch}] Upgrade important CI environment",
                "--body",
                "This PR upgrades important dependencies of the CI environment.",
            ],
            capture_output=True,
            text=True,
            check=False,
            env=command_env,
        )
        if pr_result.returncode != 0:
            get_console().print(f"[error]Failed to create PR:\n{pr_result.stdout}\n{pr_result.stderr}[/]")
            sys.exit(1)
        pr_url = pr_result.stdout.strip() if pr_result.returncode == 0 else ""
        get_console().print(f"[success]PR created successfully: {pr_url}.[/]")

        # Switch back to appropriate branch and delete the temporary branch
        get_console().print(f"[info]Cleaning up temporary branch {branch_name}...[/]")
        if user_switched_to_target:
            # User explicitly chose to switch to target branch, so stay there
            run_command(["git", "checkout", target_branch])
        else:
            # User didn't switch initially, restore to original branch/commit
            if original_branch == "HEAD":
                # Detached HEAD state, restore to original commit
                get_console().print(f"[info]Restoring to original commit {original_commit[:8]}...[/]")
                run_command(["git", "checkout", original_commit])
            else:
                # Named branch, restore to it
                get_console().print(f"[info]Restoring to original branch {original_branch}...[/]")
                run_command(["git", "checkout", original_branch])

        # Delete local branch
        run_command(["git", "branch", "-D", branch_name])
        get_console().print(f"[success]Local branch {branch_name} deleted.[/]")
    else:
        get_console().print("[info]PR creation skipped. Changes are committed locally.[/]")
