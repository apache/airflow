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
from typing import TYPE_CHECKING, Any, NamedTuple

import click

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH, DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.commands.common_options import (
    option_answer,
    option_dry_run,
    option_github_repository,
    option_github_token,
    option_verbose,
)
from airflow_breeze.global_constants import (
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    MILESTONE_BUG_LABELS,
    MILESTONE_SKIP_LABELS,
    PUBLIC_AMD_RUNNERS,
    GithubEvents,
    github_events,
)
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.confirm import Answer, user_confirm
from airflow_breeze.utils.console import console_print, get_console
from airflow_breeze.utils.custom_param_types import BetterChoice
from airflow_breeze.utils.docker_command_utils import (
    check_docker_resources,
    fix_ownership_using_docker,
    perform_environment_checks,
)
from airflow_breeze.utils.path_utils import AIRFLOW_HOME_PATH, AIRFLOW_ROOT_PATH
from airflow_breeze.utils.run_utils import run_command

if TYPE_CHECKING:
    from github import Github
    from github.Repository import Issue, Milestone, Repository


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
    console_print(f"[info]Fixing ownership of {file}")
    result = run_command(
        ["sudo", "chown", f"{os.getuid}:{os.getgid()}", str(file.resolve())],
        check=False,
        stderr=subprocess.STDOUT,
    )
    if result.returncode != 0:
        console_print(f"[warning]Could not fix ownership for {file}: {result.stdout}")


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
        console_print(
            f"[warning]You should only need to run fix-ownership on Linux and your system is {system}"
        )
        sys.exit(0)
    if use_sudo:
        console_print("[info]Fixing ownership using sudo.")
        fix_ownership_without_docker()
        sys.exit(0)
    console_print("[info]Fixing ownership using docker.")
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
        console_print(f"[warning] Error when running diff-tree command [/]\n{result.stdout}\n{result.stderr}")
        return ()
    changed_files = tuple(result.stdout.splitlines()) if result.stdout else ()
    console_print("\n[info]Changed files:[/]\n")
    console_print(changed_files)
    console_print()
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
@click.option(
    "--github-context-input",
    help="File input (might be `-`) with JSON-formatted github context. "
    "Use this instead of --github-context for large contexts that would exceed ARG_MAX as an env var.",
    type=click.File("rt"),
    envvar="GITHUB_CONTEXT_INPUT",
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
    github_context_input: StringIO | None,
):
    try:
        from airflow_breeze.utils.selective_checks import SelectiveChecks

        if github_context and github_context_input:
            console_print("[error]You can only specify one of --github-context or --github-context-input")
            sys.exit(1)
        if github_context_input:
            github_context = github_context_input.read()
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
                console_print("[info]Force running on public runners")
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
        console_print(f"[error]Missing event_name in: {ctx}")
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
        console_print(f"[error]Wrong event name: {event_name}")
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
        console_print("[error]You can only specify one of the two --github-context or --github-context-file")
        sys.exit(1)
    if github_context:
        context = github_context
    elif github_context_input:
        context = github_context_input.read()
    else:
        console_print("[error]You must specify one of the two --github-context or --github-context-file")
        sys.exit(1)
    wi = workflow_info(context=context)
    wi.print_all_ga_outputs()


def _check_k8s_schema_published(version: str) -> bool:
    """Check if K8s schemas for a given version are published on airflow.apache.org."""
    from urllib.error import HTTPError, URLError
    from urllib.request import Request, urlopen

    url = f"https://airflow.apache.org/k8s-schemas/v{version}-standalone-strict/configmap-v1.json"
    req = Request(url, method="HEAD")
    try:
        resp = urlopen(req, timeout=15)
        return resp.status == 200
    except (HTTPError, URLError):
        return False


def _sync_k8s_schemas_to_airflow_site(airflow_site: Path, force: bool, command_env: dict[str, str]) -> None:
    """Sync K8s schemas to airflow-site directory if needed."""
    from airflow_breeze.global_constants import ALLOWED_KUBERNETES_VERSIONS

    versions = [v.lstrip("v") for v in ALLOWED_KUBERNETES_VERSIONS]
    missing: list[str] = []
    for version in versions:
        if not _check_k8s_schema_published(version):
            missing.append(version)

    if not missing and not force:
        console_print("[success]All K8s schema versions are already published. Skipping sync.[/]")
        return

    if missing:
        console_print(f"[warning]K8s schemas missing for versions: {', '.join(f'v{v}' for v in missing)}[/]")
    else:
        console_print("[info]Force sync requested.[/]")

    if not airflow_site.is_dir():
        console_print(
            f"[error]airflow-site directory not found at {airflow_site}. "
            "Use --airflow-site to specify the path to the airflow-site checkout.[/]"
        )
        return

    # Verify this is the airflow-site repo by checking git remote
    remote_result = run_command(
        ["git", "-C", str(airflow_site), "remote", "-v"],
        capture_output=True,
        text=True,
        check=False,
    )
    if remote_result.returncode != 0 or "airflow-site" not in remote_result.stdout:
        console_print(
            f"[error]{airflow_site} does not appear to be a clone of the airflow-site repository.[/]"
        )
        return

    static_dir = airflow_site / "landing-pages" / "site" / "static"
    if not static_dir.is_dir():
        console_print(
            f"[error]Expected directory structure not found: {static_dir}\n"
            "The airflow-site checkout should contain landing-pages/site/static/.[/]"
        )
        return

    output_dir = static_dir / "k8s-schemas"

    # Filter out versions already present in the local airflow-site checkout
    versions_to_download = missing if (missing and not force) else versions
    versions_to_download = [
        v
        for v in versions_to_download
        if not (output_dir / f"v{v}-standalone-strict").is_dir()
        or not any((output_dir / f"v{v}-standalone-strict").iterdir())
    ]

    if not versions_to_download:
        console_print(
            "[success]All required K8s schema versions already exist in airflow-site. Skipping download.[/]"
        )
        return

    console_print(
        f"[info]Downloading K8s schemas for versions "
        f"{', '.join(f'v{v}' for v in versions_to_download)} to {output_dir}...[/]"
    )
    cmd = [
        "uv",
        "run",
        str(AIRFLOW_ROOT_PATH / "scripts" / "ci" / "prek" / "download_k8s_schemas.py"),
        "--output-dir",
        str(output_dir),
        "--versions",
        *versions_to_download,
    ]
    run_command(cmd, check=False, env=command_env)


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
    "--draft/--no-draft",
    default=False,
    show_default=True,
    help="Create the PR as a draft (useful for scheduled CI runs where a human undrafts to trigger CI)",
    is_flag=True,
)
@click.option(
    "--switch-to-base/--no-switch-to-base",
    default=None,
    help="Automatically switch to the base branch if not already on it (if not specified, will ask)",
    is_flag=True,
)
@click.option(
    "--airflow-site",
    default="../airflow-site",
    show_default=True,
    type=click.Path(file_okay=False, dir_okay=True, resolve_path=True, path_type=Path),
    help="Path to airflow-site checkout for publishing K8s schemas",
)
@click.option(
    "--force-k8s-schema-sync",
    is_flag=True,
    default=False,
    help="Force syncing K8s schemas to airflow-site even if all versions appear published",
)
@click.option(
    "--autoupdate/--no-autoupdate",
    default=True,
    show_default=True,
    help="Run prek autoupdate to update hook revisions",
)
@click.option(
    "--update-chart-dependencies/--no-update-chart-dependencies",
    default=True,
    show_default=True,
    help="Run update-chart-dependencies to update Helm chart dependencies",
)
@click.option(
    "--upgrade-important-versions/--no-upgrade-important-versions",
    default=True,
    show_default=True,
    help="Run upgrade-important-versions to bump key dependency versions",
)
@click.option(
    "--update-uv-lock/--no-update-uv-lock",
    default=True,
    show_default=True,
    help="Run update-uv-lock to regenerate uv.lock with latest resolutions inside Breeze CI image",
)
@click.option(
    "--k8s-schema-sync/--no-k8s-schema-sync",
    default=True,
    show_default=True,
    help="Sync K8s JSON schemas to airflow-site",
)
@option_github_token
@option_answer
@option_verbose
@option_dry_run
def upgrade(
    target_branch: str,
    create_pr: bool | None,
    draft: bool,
    switch_to_base: bool | None,
    airflow_site: Path,
    force_k8s_schema_sync: bool,
    autoupdate: bool,
    update_chart_dependencies: bool,
    upgrade_important_versions: bool,
    update_uv_lock: bool,
    k8s_schema_sync: bool,
    github_token: str | None,
):
    # Validate target_branch pattern
    target_branch_pattern = re.compile(r"^(main|v\d+-\d+-test)$")
    if not target_branch_pattern.match(target_branch):
        console_print(
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
            console_print(
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
        console_print(
            "[warning]No remote pointing to apache/airflow was found. "
            "Airflow uses `upstream` for this remote by convention — "
            "run `git remote add upstream https://github.com/apache/airflow.git` to add it.[/]"
        )

    # Track whether user chose to reset to target branch
    user_switched_to_target = False

    if not at_apache_branch or not is_clean:
        console_print()
        if not at_apache_branch:
            console_print(f"[warning]You are not at the top of apache/airflow {target_branch} branch.[/]")
            console_print(f"[info]Current branch: {current_branch}[/]")
        if not is_clean:
            console_print("[warning]Your repository has uncommitted changes.[/]")
        console_print()

        # Determine whether to switch to base branch
        should_switch = switch_to_base
        if should_switch is None:
            # Not specified, ask the user
            console_print(
                f"[warning]Attempting to switch to switch to {target_branch}. "
                f"This will lose not committed code.[/]\n\n"
                "NO will continue to get changes on top of current branch, QUIT will exit."
            )
            response = user_confirm("Do you want to switch")
            if response == Answer.YES:
                should_switch = True
            elif response == Answer.QUIT:
                console_print(
                    f"[error]Upgrade cancelled. Please ensure you are on apache/airflow {target_branch} with a clean repository.[/]"
                )
                sys.exit(1)
            else:
                should_switch = False

        if should_switch:
            user_switched_to_target = True
            console_print(f"[info]Resetting to apache/airflow {target_branch}...[/]")
            if current_branch != target_branch:
                run_command(["git", "checkout", target_branch])
            run_command(["git", "fetch", apache_remote_name])
            run_command(["git", "reset", "--hard", f"{apache_remote_name}/{target_branch}"])
            run_command(["git", "clean", "-fdx"])
            console_print(
                f"[success]Successfully reset to apache/airflow {target_branch} and cleaned repository.[/]"
            )
        else:
            console_print(
                f"[info]Continuing with current branch {current_branch}. Changes will be on top of it.[/]"
            )

    console_print("[info]Running upgrade of important CI environment.[/]")

    # Resolve GitHub token: prefer --github-token / GITHUB_TOKEN env var, fall back to gh CLI
    if not github_token:
        gh_token_result = run_command(
            ["gh", "auth", "token"],
            capture_output=True,
            text=True,
            check=False,
        )
        if gh_token_result.returncode == 0 and gh_token_result.stdout.strip():
            github_token = gh_token_result.stdout.strip()

    # Create a copy of the environment to pass to commands
    command_env = os.environ.copy()

    if github_token:
        command_env["GITHUB_TOKEN"] = github_token
        console_print("[success]GitHub token set in environment.[/]")
    else:
        console_print(
            "[warning]Could not retrieve GitHub token from --github-token or gh CLI. "
            "Commands may fail if they require authentication.[/]"
        )

    # All upgrade commands run locally with check=False to continue on errors.
    # The uv lock --upgrade step must run last so it can incorporate changes from the other steps.
    upgrade_commands: list[tuple[str, str]] = [
        ("autoupdate", "prek autoupdate --cooldown-days 4 --freeze"),
        (
            "update-chart-dependencies",
            "prek --all-files --show-diff-on-failure --color always --verbose --stage manual update-chart-dependencies",
        ),
        (
            "upgrade-important-versions",
            "prek --all-files --show-diff-on-failure --color always --verbose --stage manual upgrade-important-versions",
        ),
        (
            "update-uv-lock",
            "uv lock --upgrade",
        ),
    ]

    step_enabled = {
        "autoupdate": autoupdate,
        "update-chart-dependencies": update_chart_dependencies,
        "upgrade-important-versions": upgrade_important_versions,
        "update-uv-lock": update_uv_lock,
    }

    # Execute upgrade commands
    for step_name, command in upgrade_commands:
        if step_enabled[step_name]:
            run_command(command.split(), check=False, env=command_env)
        else:
            console_print(f"[info]Skipping {step_name} (disabled).[/]")

    # Sync K8s schemas to airflow-site
    if k8s_schema_sync:
        _sync_k8s_schemas_to_airflow_site(airflow_site, force_k8s_schema_sync, command_env)
    else:
        console_print("[info]Skipping K8s schema sync (disabled).[/]")

    res = run_command(["git", "diff", "--exit-code"], check=False)
    if res.returncode == 0:
        console_print("[success]No changes were made during the upgrade. Exiting[/]")
        sys.exit(0)

    # Determine whether to create a PR
    should_create_pr = create_pr
    if should_create_pr is None:
        # Not specified, ask the user
        should_create_pr = user_confirm("Do you want to create a PR with the upgrade changes?") == Answer.YES

    if should_create_pr:
        # Use a stable branch name based on target branch so scheduled runs can reuse/update the same PR
        branch_name = f"ci-upgrade-{target_branch}"

        # Check if branch already exists and delete it
        branch_check = run_command(
            ["git", "rev-parse", "--verify", branch_name], capture_output=True, check=False
        )
        if branch_check.returncode == 0:
            console_print(f"[info]Branch {branch_name} already exists, deleting it...[/]")
            run_command(["git", "branch", "-D", branch_name])

        run_command(["git", "checkout", "-b", branch_name])
        run_command(["git", "add", "."])
        try:
            run_command(
                ["git", "commit", "--message", f"[{target_branch}] CI: Upgrade important CI environment"]
            )
        except subprocess.CalledProcessError:
            console_print("[info]Commit failed, assume some auto-fixes might have been made...[/]")
            run_command(["git", "add", "."])
            run_command(
                [
                    "git",
                    "commit",
                    # postpone pre-commit checks to CI, not to fail in automation if e.g. mypy changes force code checks
                    "--no-verify",
                    "--message",
                    f"[{target_branch}] CI: Upgrade important CI environment",
                ]
            )

        # Push the branch to origin (use detected origin or fallback to 'origin')
        push_remote = origin_remote_name if origin_remote_name else "origin"
        console_print(f"[info]Pushing branch {branch_name} to {push_remote}...[/]")
        push_result = run_command(
            ["git", "push", "-u", push_remote, branch_name, "--force"],
            capture_output=True,
            text=True,
            check=False,
        )
        if push_result.returncode != 0:
            console_print(f"[error]Failed to push branch:\n{push_result.stdout}\n{push_result.stderr}[/]")
            sys.exit(1)
        console_print(f"[success]Branch {branch_name} pushed to {push_remote}.[/]")

        # Create PR from the pushed branch
        # gh pr create needs --head in format "username:branch" when creating a PR from a fork
        # Extract username from origin_repo (e.g., "username/airflow" -> "username")
        if origin_repo:
            owner = origin_repo.split("/")[0]
            head_ref = f"{owner}:{branch_name}"
            console_print(
                f"[info]Creating PR from {origin_repo} branch {branch_name} to apache/airflow {target_branch}...[/]"
            )
        else:
            # Fallback to just branch name if we couldn't determine the fork
            head_ref = branch_name
            console_print("[warning]Could not determine fork repository. Using branch name only.[/]")

        pr_title = f"[{target_branch}] Upgrade important CI environment"
        pr_body = "This PR upgrades important dependencies of the CI environment."

        # Check if there's already an open PR for this branch
        existing_pr_result = run_command(
            [
                "gh",
                "pr",
                "list",
                "--repo",
                "apache/airflow",
                "--head",
                head_ref,
                "--base",
                target_branch,
                "--state",
                "open",
                "--json",
                "number,url",
                "--jq",
                ".[0]",
            ],
            capture_output=True,
            text=True,
            check=False,
            env=command_env,
        )

        existing_pr = existing_pr_result.stdout.strip() if existing_pr_result.returncode == 0 else ""

        if existing_pr and existing_pr != "null" and existing_pr != "":
            console_print(f"[success]Existing PR found and updated with force push: {existing_pr}[/]")
            if draft:
                # Convert back to draft so a human must undraft to trigger CI
                run_command(
                    [
                        "gh",
                        "pr",
                        "ready",
                        "--repo",
                        "apache/airflow",
                        "--undo",
                        head_ref,
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                    env=command_env,
                )
                console_print("[info]Existing PR converted back to draft.[/]")
        else:
            # Create a new PR
            gh_create_cmd = [
                "gh",
                "pr",
                "create",
                "--repo",
                "apache/airflow",
                "--head",
                head_ref,
                "--base",
                target_branch,
                "--title",
                pr_title,
                "--body",
                pr_body,
            ]
            if draft:
                gh_create_cmd.append("--draft")

            pr_result = run_command(
                gh_create_cmd,
                capture_output=True,
                text=True,
                check=False,
                env=command_env,
            )
            if pr_result.returncode != 0:
                console_print(f"[error]Failed to create PR:\n{pr_result.stdout}\n{pr_result.stderr}[/]")
                sys.exit(1)
            pr_url = pr_result.stdout.strip() if pr_result.returncode == 0 else ""
            console_print(f"[success]PR created successfully: {pr_url}.[/]")

        # Switch back to appropriate branch and delete the temporary branch
        console_print(f"[info]Cleaning up temporary branch {branch_name}...[/]")
        if user_switched_to_target:
            # User explicitly chose to switch to target branch, so stay there
            run_command(["git", "checkout", target_branch])
        else:
            # User didn't switch initially, restore to original branch/commit
            if original_branch == "HEAD":
                # Detached HEAD state, restore to original commit
                console_print(f"[info]Restoring to original commit {original_commit[:8]}...[/]")
                run_command(["git", "checkout", original_commit])
            else:
                # Named branch, restore to it
                console_print(f"[info]Restoring to original branch {original_branch}...[/]")
                run_command(["git", "checkout", original_branch])

        # Delete local branch
        run_command(["git", "branch", "-D", branch_name])
        console_print(f"[success]Local branch {branch_name} deleted.[/]")
    else:
        console_print("[info]PR creation skipped. Changes are committed locally.[/]")


VERSION_BRANCH_PATTERN = re.compile(r"^v(\d+)-(\d+)-test$")
BACKPORT_LABEL_PATTERN = re.compile(r"^backport-to-v(\d+)-(\d+)-test$")


def _parse_version_from_branch(branch: str) -> tuple[int, int] | None:
    """Parse major and minor version from a branch name like 'v3-1-test'."""
    match = VERSION_BRANCH_PATTERN.match(branch)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None


def _parse_version_from_backport_label(label: str) -> tuple[int, int] | None:
    """Parse major and minor version from a backport label like 'backport-to-v3-1-test'."""
    match = BACKPORT_LABEL_PATTERN.match(label)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None


def _get_milestone_prefix(major: int, minor: int) -> str:
    """Get the milestone prefix for a given version like 'Airflow 3.1'."""
    return f"Airflow {major}.{minor}"


def _get_github_client(github_token: str) -> Github:
    """Create a GitHub client with the given token."""
    from github import Github

    return Github(github_token)


def _find_matching_milestone(repo: Repository, milestone_prefix: str) -> Milestone | None:
    """Find the latest matching milestone that starts with the given prefix."""
    try:
        milestones = list(repo.get_milestones(state="open"))
        matching = [m for m in milestones if m.title.startswith(milestone_prefix)]
        if not matching:
            return None
        # Sort by title to get the latest patch version (e.g., Airflow 3.1.8 > Airflow 3.1.7)
        matching.sort(key=lambda m: m.title, reverse=True)
        return matching[0]
    except Exception as e:
        console_print(f"[error]Failed to get milestones: {e}[/]")
        return None


def _parse_milestone_version(title: str) -> tuple[int, int, int] | None:
    """Parse version from milestone title like 'Airflow 3.1.8' or 'Airflow 3.2'."""
    if not title.startswith("Airflow "):
        return None
    version_part = title.replace("Airflow ", "")
    parts = version_part.split(".")
    if len(parts) < 2:
        return None
    try:
        major = int(parts[0])
        minor = int(parts[1])
        patch = int(parts[2]) if len(parts) > 2 else 0
        return major, minor, patch
    except ValueError:
        return None


def _find_latest_milestone(repo: Repository) -> Milestone | None:
    """Find the latest (highest version) open milestone."""
    try:
        milestones = list(repo.get_milestones(state="open"))
        # Filter for Airflow milestones and parse versions
        airflow_milestones: list[tuple[Milestone, tuple[int, int, int]]] = []
        for m in milestones:
            version = _parse_milestone_version(m.title)
            if version:
                airflow_milestones.append((m, version))

        if not airflow_milestones:
            return None

        # Sort by version (major, minor, patch) descending to get the latest
        airflow_milestones.sort(key=lambda x: x[1], reverse=True)
        return airflow_milestones[0][0]
    except Exception as e:
        console_print(f"[error]Failed to get milestones: {e}[/]")
        return None


def _get_mention(merged_by_login: str) -> str:
    """Get the mention string for a user."""
    return f"@{merged_by_login}" if merged_by_login and merged_by_login != "unknown" else "maintainer"


def _get_milestone_notification_comment(
    milestone_title: str, milestone_number: int, merged_by_login: str, reason: str, github_repository: str
) -> str:
    """Generate the notification comment for auto-set milestone."""
    mention = _get_mention(merged_by_login)

    return f"""Hi {mention}, this PR was merged without a milestone set.
We've automatically set the milestone to **[{milestone_title}](https://github.com/{github_repository}/milestone/{milestone_number})** based on: {reason}
If this milestone is not correct, please update it to the appropriate milestone.

> This comment was generated by [Milestone Tag Assistant](https://github.com/{github_repository}/blob/main/.github/workflows/milestone-tag-assistant.yml).
"""


def _get_milestone_not_found_comment(
    merged_by_login: str, reason: str, github_repository: str, search_criteria: str
) -> str:
    """Generate the notification comment when no matching milestone is found."""
    mention = _get_mention(merged_by_login)

    return f"""Hi {mention}, this PR was merged without a milestone set.
We tried to automatically set a milestone based on: {reason}
However, **no open milestone was found** matching: {search_criteria}

**Action required:** Please manually set the appropriate milestone for this PR.

> This comment was generated by [Milestone Tag Assistant](https://github.com/{github_repository}/blob/main/.github/workflows/milestone-tag-assistant.yml).
"""


def _has_bug_fix_indicators(title: str, labels: list[str]) -> bool:
    """Check if the PR has indicators that it's a bug fix."""
    title_lower = title.lower()
    if "fix" in title_lower or "bug" in title_lower:
        return True
    if set(labels) & MILESTONE_BUG_LABELS:
        return True
    return False


def _should_skip_milestone_tagging(labels: list[str]) -> bool:
    """Check if the PR should be skipped from milestone auto-tagging."""
    return bool(set(labels) & MILESTONE_SKIP_LABELS)


def _get_backport_version_from_labels(labels: list[str]) -> tuple[int, int] | None:
    """Find the first backport label and extract version from it."""
    for label in labels:
        if label.startswith("backport-to-"):
            version = _parse_version_from_backport_label(label)
            if version:
                return version
    return None


def _determine_milestone_version(
    labels: list[str], title: str, base_branch: str
) -> tuple[tuple[int, int] | None, str]:
    """Determine which milestone version to use based on PR criteria.

    :returns: Tuple of (version, reason) where version can be:
        - (major, minor) tuple for specific version milestone (patch releases)
        - None if no milestone should be set
    """
    # Priority 1: Check for backport labels - use specific version milestone
    backport_version = _get_backport_version_from_labels(labels)
    if backport_version:
        return backport_version, f"backport label targeting v{backport_version[0]}-{backport_version[1]}-test"

    # Priority 2: Check if merged to a version branch - use that version's milestone
    version = _parse_version_from_branch(base_branch)
    if version:
        if _has_bug_fix_indicators(title, labels):
            return version, "bug fix merged to version branch"
        # Non-bug fix merged to version branch still gets that version's milestone
        return version, "merged to version branch"

    return None, "no backport label and not merged to a version branch"


@ci_group.command(
    name="set-milestone",
    help="Set milestone on a merged PR if it doesn't have one. Used by the milestone-tag-assistant workflow.",
)
@click.option(
    "--pr-number",
    help="The PR number to set milestone on",
    envvar="PR_NUMBER",
    required=True,
    type=int,
)
@click.option(
    "--pr-title",
    help="The PR title",
    envvar="PR_TITLE",
    default="",
)
@click.option(
    "--pr-labels",
    help="JSON array of PR label names",
    envvar="PR_LABELS",
    default="[]",
)
@click.option(
    "--base-branch",
    help="The base branch the PR was merged to",
    envvar="BASE_BRANCH",
    default="",
)
@click.option(
    "--merged-by",
    help="GitHub username of the person who merged the PR",
    envvar="MERGED_BY",
    default="unknown",
)
@click.option(
    "--github-token",
    help="GitHub token for API access",
    envvar="GH_TOKEN",
    required=True,
)
@option_github_repository
@option_verbose
@option_dry_run
def set_milestone(
    pr_number: int,
    pr_title: str,
    pr_labels: str,
    base_branch: str,
    merged_by: str,
    github_token: str,
    github_repository: str,
):
    """Set milestone on a merged PR based on backport labels or bug fix indicators."""
    from github import UnknownObjectException

    console_print(f"[info]Processing PR #{pr_number}[/]")
    console_print(f"[info]Title: {pr_title}[/]")
    console_print(f"[info]Base branch: {base_branch}[/]")
    console_print(f"[info]Merged by: {merged_by}[/]")

    # Parse labels from JSON
    try:
        labels = json.loads(pr_labels)
    except json.JSONDecodeError:
        console_print(f"[warning]Could not parse labels JSON: {pr_labels}[/]")
        labels = []

    console_print(f"[info]Labels: {labels}[/]")

    # Check if we should skip
    if _should_skip_milestone_tagging(labels):
        console_print(
            f"[info]Skipping milestone tagging - PR has skip label(s): {set(labels) & MILESTONE_SKIP_LABELS}[/]"
        )
        return

    # Determine which milestone to use
    version, reason = _determine_milestone_version(labels, pr_title, base_branch)
    if version is None:
        console_print(f"[info]No milestone to set: {reason}[/]")
        return

    # Initialize GitHub client and get repository
    try:
        gh = _get_github_client(github_token)
        repo: Repository = gh.get_repo(github_repository)
    except Exception as e:
        console_print(f"[error]Failed to connect to GitHub: {e}[/]")
        return

    # Double check whether the PR already has a milestone set - if so, we don't want to override it
    try:
        issue: Issue = repo.get_issue(pr_number)
        if issue.milestone is not None:
            console_print(
                f"[info]PR #{pr_number} already has milestone '{issue.milestone.title}' set. Skipping.[/]"
            )
            return
    except UnknownObjectException:
        console_print(f"[error]PR #{pr_number} not found when checking existing milestone[/]")
        return
    except Exception as e:
        console_print(f"[error]Failed to check existing milestone: {e}[/]")
        return

    major, minor = version
    milestone_prefix = _get_milestone_prefix(major, minor)
    console_print(f"[info]Looking for milestone with prefix: {milestone_prefix}[/]")
    milestone = _find_matching_milestone(repo, milestone_prefix)
    search_criteria = f"prefix '{milestone_prefix}'"

    if not milestone:
        console_print(f"[warning]No open milestone found matching: {search_criteria}[/]")
        # Add reminder comment for committer
        try:
            issue = repo.get_issue(pr_number)
            comment = _get_milestone_not_found_comment(merged_by, reason, github_repository, search_criteria)
            issue.create_comment(comment)
            console_print(f"[info]Added reminder comment to PR #{pr_number}[/]")
        except Exception as e:
            console_print(f"[warning]Failed to add reminder comment: {e}[/]")
        return

    console_print(f"[info]Found milestone: {milestone.title} (#{milestone.number})[/]")

    # Get the issue (PRs are issues in GitHub API)
    try:
        issue = repo.get_issue(pr_number)
    except UnknownObjectException:
        console_print(f"[error]PR #{pr_number} not found[/]")
        return

    # Set milestone on PR
    try:
        issue.edit(milestone=milestone)
        console_print(f"[success]Successfully set milestone '{milestone.title}' on PR #{pr_number}[/]")
    except Exception as e:
        console_print(f"[error]Failed to set milestone on PR #{pr_number}: {e}[/]")
        return

    # Add notification comment
    comment = _get_milestone_notification_comment(
        milestone.title, milestone.number, merged_by, reason, github_repository
    )
    try:
        issue.create_comment(comment)
        console_print(f"[success]Added notification comment to PR #{pr_number}[/]")
    except Exception as e:
        console_print(f"[warning]Failed to add notification comment to PR #{pr_number}: {e}[/]")
