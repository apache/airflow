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

import os
import re
import sys
import tempfile
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rich.markup import escape

from airflow_breeze.utils.confirm import Answer, user_confirm
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH
from airflow_breeze.utils.shared_options import get_dry_run, get_verbose

if TYPE_CHECKING:
    from requests import Response


def get_ga_output(name: str, value: Any) -> str:
    output_name = name.replace("_", "-")
    printed_value = str(value).lower() if isinstance(value, bool) else value
    get_console().print(f"[info]{output_name}[/] = [green]{escape(str(printed_value))}[/]")
    return f"{output_name}={printed_value}"


def log_github_rate_limit_error(response: Response) -> None:
    """
    Logs info about GitHub rate limit errors (primary or secondary).
    """
    if response.status_code not in (403, 429):
        return

    remaining = response.headers.get("x-rateLimit-remaining")
    reset = response.headers.get("x-rateLimit-reset")
    retry_after = response.headers.get("retry-after")

    try:
        message = response.json().get("message", "")
    except Exception:
        message = response.text or ""

    remaining_int = int(remaining) if remaining and remaining.isdigit() else None

    if reset and reset.isdigit():
        reset_dt = datetime.fromtimestamp(int(reset), tz=timezone.utc)
        reset_time = reset_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        reset_time = "unknown"

    if remaining_int == 0:
        print(f"Primary rate limit exceeded. No requests remaining. Reset at {reset_time}.")
        return

    # Message for secondary looks like: "You have exceeded a secondary rate limit"
    if "secondary rate limit" in message.lower():
        if retry_after and retry_after.isdigit():
            print(f"Secondary rate limit exceeded. Retry after {retry_after} seconds.")
        else:
            print(f"Secondary rate limit exceeded. Please wait until {reset_time} or at least 60 seconds.")
        return

    print(f"Rate limit error. Status: {response.status_code}, Message: {message}")


def download_file_from_github(
    reference: str, path: str, output_file: Path, github_token: str | None = None, timeout: int = 60
) -> bool:
    """
    Downloads a file from the GitHub repository of Apache Airflow using the GitHub API.

    In case of any error different from 404, it will exit the process with error code 1.

    :param reference: tag to download from
    :param path: path of the file relative to the repository root
    :param output_file: Path where the file should be downloaded
    :param github_token: GitHub token to use for authentication
    :param timeout: timeout in seconds for the download request, default is 60 seconds
    :return: whether the file was successfully downloaded (False if the file is missing)
    """
    import requests

    url = f"https://api.github.com/repos/apache/airflow/contents/{path}?ref={reference}"
    get_console().print(f"[info]Downloading {url} to {output_file}")
    if not get_dry_run():
        headers = {"Accept": "application/vnd.github.v3.raw"}
        if github_token:
            headers["Authorization"] = f"Bearer {github_token}"
            headers["X-GitHub-Api-Version"] = "2022-11-28"
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            log_github_rate_limit_error(response)
            if response.status_code == 403:
                get_console().print(
                    f"[error]Access denied to {url}. This may be caused by:\n"
                    f"   1. Network issues or VPN settings\n"
                    f"   2. GitHub API rate limiting\n"
                    f"   3. Invalid or missing GitHub token"
                )
                sys.exit(1)
            if response.status_code == 404:
                get_console().print(f"[warning]The {url} has not been found. Skipping")
                return False
            if response.status_code != 200:
                get_console().print(
                    f"[error]{url} could not be downloaded. Status code {response.status_code}"
                )
                sys.exit(1)
            output_file.write_bytes(response.content)
        except requests.Timeout:
            get_console().print(f"[error]The request to {url} timed out after {timeout} seconds.")
            sys.exit(1)
    get_console().print(f"[success]Downloaded {url} to {output_file}")
    return True


ACTIVE_TAG_MATCH = re.compile(r"^(\d+)\.\d+\.\d+$")


def get_active_airflow_versions(
    confirm: bool = True, remote_name: str = "apache"
) -> tuple[list[str], dict[str, str]]:
    """
    Gets list of active Airflow versions from GitHub.

    :param confirm: if True, will ask the user before proceeding with the versions found
    :param remote_name: name of the remote to fetch tags from (e.g., 'apache')
    :return: tuple: list of active Airflow versions and dict of Airflow release dates (in iso format)
    """
    from git import GitCommandError, Repo
    from packaging.version import Version

    airflow_release_dates: dict[str, str] = {}
    get_console().print(
        f"\n[warning]Make sure you have `{remote_name}` remote added pointing to apache/airflow repository\n"
    )
    get_console().print("[info]Fetching all released Airflow 2/3 versions from GitHub[/]\n")
    repo = Repo(AIRFLOW_ROOT_PATH)
    all_active_tags: list[str] = []
    try:
        ref_tags = repo.git.ls_remote("--tags", remote_name).splitlines()
    except GitCommandError as ex:
        get_console().print(
            "[error]Could not fetch tags from `apache` remote! Make sure to have it configured.\n"
        )
        get_console().print(f"{ex}\n")
        get_console().print(
            "[info]You can add apache remote with on of those commands (depend which protocol you use):\n"
            " * git remote add apache https://github.com/apache/airflow.git\n"
            " * git remote add apache git@github.com:apache/airflow.git\n"
        )
        sys.exit(1)
    tags = [tag.split("refs/tags/")[1].strip() for tag in ref_tags if "refs/tags/" in tag]
    for tag in tags:
        match = ACTIVE_TAG_MATCH.match(tag)
        if match and (match.group(1) == "2" or match.group(1) == "3"):
            all_active_tags.append(tag)
    airflow_versions = sorted(all_active_tags, key=Version)
    for version in airflow_versions:
        date = get_tag_date(version)
        if not date:
            get_console().print("[error]Error fetching tag date for Airflow {version}")
            sys.exit(1)
        airflow_release_dates[version] = date
    get_console().print("[info]All Airflow 2/3 versions loaded from GitHub[/]")
    if get_verbose():
        get_console().print("[info]Found active Airflow versions:[/]")
        get_console().print(airflow_versions)
    if confirm:
        for version in airflow_versions:
            get_console().print(f"  {version}: [info]{airflow_release_dates[version]}[/]")
        answer = user_confirm(
            "Should we continue with those versions?", quit_allowed=False, default_answer=Answer.YES
        )
        if answer == Answer.NO:
            get_console().print("[red]Aborting[/]")
            sys.exit(1)
    return airflow_versions, airflow_release_dates


def download_constraints_file(
    constraints_reference: str,
    python_version: str,
    github_token: str | None,
    airflow_constraints_mode: str,
    output_file: Path,
) -> bool:
    """
    Downloads constraints file from GitHub repository of Apache Airflow

    :param constraints_reference: airflow version
    :param python_version: python version
    :param github_token: GitHub token
    :param airflow_constraints_mode: constraints mode
    :param output_file: the file where to store the constraint file
    :return: true if the file was successfully downloaded
    """
    constraints_file_path = f"{airflow_constraints_mode}-{python_version}.txt"
    return download_file_from_github(
        reference=constraints_reference,
        path=constraints_file_path,
        github_token=github_token,
        output_file=output_file,
    )


def get_tag_date(tag: str) -> str | None:
    """
    Returns UTC timestamp of the tag in the repo in iso time format 8601
    :param tag: tag to get date for
    :return: iso time format 8601 of the tag date
    """
    from git import Repo

    repo = Repo(AIRFLOW_ROOT_PATH)
    try:
        tag_object = repo.tags[tag].object
    except IndexError:
        get_console().print(f"[warning]Tag {tag} not found in the repository")
        return None
    timestamp: int = (
        tag_object.committed_date if hasattr(tag_object, "committed_date") else tag_object.tagged_date
    )
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def download_artifact_from_run_id(run_id: str, output_file: Path, github_repository: str, github_token: str):
    """
    Downloads a file from GitHub Actions artifact

    :param run_id: run_id of the workflow
    :param output_file: Path where the file should be downloaded
    :param github_repository: GitHub repository
    :param github_token: GitHub token
    """
    import requests
    from tqdm import tqdm

    url = f"https://api.github.com/repos/{github_repository}/actions/runs/{run_id}/artifacts"
    headers = {"Accept": "application/vnd.github.v3+json"}

    session = requests.Session()
    headers["Authorization"] = f"Bearer {github_token}"
    artifact_response = requests.get(url, headers=headers)

    if artifact_response.status_code != 200:
        get_console().print(
            "[error]Describing artifacts failed with status code "
            f"{artifact_response.status_code}: {artifact_response.text}",
        )
        sys.exit(1)

    download_url = None
    file_name = os.path.splitext(os.path.basename(output_file))[0]
    for artifact in artifact_response.json()["artifacts"]:
        if artifact["name"].startswith(file_name):
            download_url = artifact["archive_download_url"]
            break

    if not download_url:
        get_console().print(f"[error]No artifact found for {file_name}")
        sys.exit(1)

    get_console().print(f"[info]Downloading artifact from {download_url} to {output_file}")

    response = session.get(download_url, stream=True, headers=headers)

    if response.status_code != 200:
        get_console().print(
            f"[error]Downloading artifacts failed with status code {response.status_code}: {response.text}",
        )
        sys.exit(1)

    total_size = int(response.headers.get("content-length", 0))
    temp_file = tempfile.NamedTemporaryFile().name + "/file.zip"
    os.makedirs(os.path.dirname(temp_file), exist_ok=True)

    with tqdm(total=total_size, unit="B", unit_scale=True, desc=temp_file, ascii=True) as progress_bar:
        with open(temp_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=1 * 1024 * 1024):
                if chunk:
                    f.write(chunk)
                    progress_bar.update(len(chunk))

    with zipfile.ZipFile(temp_file, "r") as zip_ref:
        zip_ref.extractall("/tmp/")

    os.remove(temp_file)


def download_artifact_from_pr(pr: str, output_file: Path, github_repository: str, github_token: str):
    import requests

    pr_number = pr.lstrip("#")
    pr_url = f"https://api.github.com/repos/{github_repository}/pulls/{pr_number}"
    workflow_run_url = f"https://api.github.com/repos/{github_repository}/actions/runs"

    headers = {"Accept": "application/vnd.github.v3+json"}

    session = requests.Session()
    headers["Authorization"] = f"Bearer {github_token}"

    pull_response = session.get(pr_url, headers=headers)

    if pull_response.status_code != 200:
        get_console().print(
            f"[error]Fetching PR failed with status codee {pull_response.status_code}: {pull_response.text}",
        )
        sys.exit(1)

    ref = pull_response.json()["head"]["ref"]

    workflow_runs = session.get(
        workflow_run_url, headers=headers, params={"event": "pull_request", "branch": ref}
    )

    if workflow_runs.status_code != 200:
        get_console().print(
            "[error]Fetching workflow runs failed with status code %s, %s, "
            "you might need to provide GITHUB_TOKEN, set it as environment variable",
            workflow_runs.status_code,
            workflow_runs.content,
        )
        sys.exit(1)

    data = workflow_runs.json()["workflow_runs"]
    sorted_data = sorted(data, key=lambda x: datetime.fromisoformat(x["created_at"]), reverse=True)
    run_id = None
    # Filter only workflow with ci.yml, we may get multiple workflows for a PR ex: codeql-analysis.yml

    for run in sorted_data:
        if run.get("path").endswith("ci.yml"):
            run_id = run["id"]
            break

    get_console().print(f"[info]Found run id {run_id} for PR {pr}")

    download_artifact_from_run_id(str(run_id), output_file, github_repository, github_token)


_CONTRIBUTING_DOCS_URL = "https://github.com/apache/airflow/blob/main/contributing-docs"
_STATIC_CHECKS_URL = f"{_CONTRIBUTING_DOCS_URL}/08_static_code_checks.rst"
_TESTING_URL = f"{_CONTRIBUTING_DOCS_URL}/09_testing.rst"

# Patterns to categorize failing CI check names
_CHECK_CATEGORIES: list[tuple[str, list[str], str, str]] = [
    # (category, name_patterns, fix_instructions, doc_url)
    (
        "Pre-commit / static checks",
        ["static checks", "pre-commit", "prek"],
        "Run `prek run --from-ref main` locally to find and fix issues.",
        _STATIC_CHECKS_URL,
    ),
    (
        "Ruff (linting / formatting)",
        ["ruff"],
        "Run `prek run ruff --from-ref main` and `prek run ruff-format --from-ref main` to fix.",
        f"{_STATIC_CHECKS_URL}#using-prek",
    ),
    (
        "mypy (type checking)",
        ["mypy"],
        "",  # dynamically generated in assess_pr_checks based on which mypy hooks failed
        f"{_STATIC_CHECKS_URL}#mypy-checks",
    ),
    (
        "Unit tests",
        ["unit test", "test-"],
        "Run failing tests with `breeze run pytest <path> -xvs`.",
        _TESTING_URL,
    ),
    (
        "Build docs",
        ["docs", "spellcheck-docs", "build-docs"],
        "Run `breeze build-docs` locally to reproduce.",
        f"{_CONTRIBUTING_DOCS_URL}/16_documentation.rst",
    ),
    (
        "Helm tests",
        ["helm"],
        "Run Helm tests with `breeze k8s run-complete-tests`.",
        _TESTING_URL,
    ),
    (
        "Kubernetes tests",
        ["k8s", "kubernetes"],
        "See the K8s testing documentation.",
        _TESTING_URL,
    ),
    (
        "Image build",
        ["build ci image", "build prod image", "ci-image", "prod-image"],
        "Check that Dockerfiles and dependencies are correct.",
        f"{_CONTRIBUTING_DOCS_URL}/03a_contributors_quick_start_beginners.rst",
    ),
    (
        "Provider tests",
        ["provider"],
        "Run provider tests with `breeze run pytest <provider-test-path> -xvs`.",
        f"{_CONTRIBUTING_DOCS_URL}/12_provider_distributions.rst",
    ),
]


@dataclass
class Violation:
    category: str
    explanation: str  # short description of the problem (shown in terminal)
    severity: str  # "error" or "warning"
    details: str = ""  # fix suggestions and doc links (included only in GitHub comment)


@dataclass
class PRAssessment:
    should_flag: bool
    violations: list[Violation] = field(default_factory=list)
    summary: str = ""
    error: bool = False


_MYPY_HOOK_RE = re.compile(r"\b(mypy-[\w-]+)\b", re.IGNORECASE)


_ALL_MYPY_HOOKS = [
    "mypy-airflow-core",
    "mypy-providers",
    "mypy-dev",
    "mypy-task-sdk",
    "mypy-devel-common",
    "mypy-airflow-ctl",
]


def _build_mypy_fix(checks: list[str]) -> str:
    """Build a mypy fix instruction from the list of failing mypy check names."""
    hooks: list[str] = []
    for check in checks:
        m = _MYPY_HOOK_RE.search(check)
        if m:
            hooks.append(m.group(1).lower())
    if not hooks:
        hooks = _ALL_MYPY_HOOKS
    commands = " && ".join(f"`prek --stage manual {hook} --all-files`" for hook in hooks)
    return (
        f"Run {commands} locally to reproduce. "
        "You need `breeze ci-image build --python 3.10` for Docker-based mypy."
    )


def _categorize_check(check_name: str) -> tuple[str, str, str] | None:
    """Match a failing check name to a category. Returns (category, fix_instructions, doc_url) or None."""
    lower = check_name.lower()
    for category, patterns, fix_instructions, url in _CHECK_CATEGORIES:
        if any(p in lower for p in patterns):
            return category, fix_instructions, url
    return None


def assess_pr_checks(pr_number: int, checks_state: str, failed_checks: list[str]) -> PRAssessment | None:
    """Deterministically flag a PR if CI checks are failing. Returns None if checks pass.

    Uses the statusCheckRollup.state as the authoritative signal.
    failed_checks is a best-effort list of individual failing check names.
    """
    if checks_state != "FAILURE":
        return None

    violations: list[Violation] = []

    if failed_checks:
        # Group failing checks by category
        categorized: dict[str, tuple[list[str], str, str]] = {}
        uncategorized: list[str] = []

        for check in failed_checks:
            match = _categorize_check(check)
            if match:
                category, fix_instructions, url = match
                if category not in categorized:
                    categorized[category] = ([], fix_instructions, url)
                categorized[category][0].append(check)
            else:
                uncategorized.append(check)

        for category, (checks, fix_instructions, url) in categorized.items():
            checks_list = ", ".join(checks[:5])
            if len(checks) > 5:
                checks_list += f" (+{len(checks) - 5} more)"
            fix_text = _build_mypy_fix(checks) if category == "mypy (type checking)" else fix_instructions
            violations.append(
                Violation(
                    category=category,
                    explanation=f"Failing: {checks_list}.",
                    severity="error",
                    details=f"{fix_text} See [{category} docs]({url}).",
                )
            )

        if uncategorized:
            checks_list = ", ".join(uncategorized[:5])
            if len(uncategorized) > 5:
                checks_list += f" (+{len(uncategorized) - 5} more)"
            violations.append(
                Violation(
                    category="Other failing CI checks",
                    explanation=f"Failing: {checks_list}.",
                    severity="error",
                    details=(
                        f"Run `prek run --from-ref main` locally to reproduce. "
                        f"See [static checks docs]({_STATIC_CHECKS_URL})."
                    ),
                )
            )

        summary = f"PR #{pr_number} has {len(failed_checks)} failing CI check(s)."
    else:
        violations.append(
            Violation(
                category="Failing CI checks",
                explanation="CI checks are failing (individual check names not available).",
                severity="error",
                details=(
                    f"Run `prek run --from-ref main` locally to reproduce. "
                    f"See [static checks docs]({_STATIC_CHECKS_URL}) "
                    f"and [testing docs]({_TESTING_URL})."
                ),
            )
        )
        summary = f"PR #{pr_number} has failing CI checks."

    return PRAssessment(
        should_flag=True,
        violations=violations,
        summary=summary,
    )


def assess_pr_conflicts(
    pr_number: int, mergeable: str, base_ref: str, commits_behind: int
) -> PRAssessment | None:
    """Deterministically flag a PR if it has merge conflicts. Returns None if no conflicts."""
    if mergeable != "CONFLICTING":
        return None

    behind_note = ""
    if commits_behind > 0:
        behind_note = (
            f" Your branch is {commits_behind} commit{'s' if commits_behind != 1 else ''} "
            f"behind `{base_ref}`."
        )

    return PRAssessment(
        should_flag=True,
        violations=[
            Violation(
                category="Merge conflicts",
                explanation=(f"This PR has merge conflicts with the `{base_ref}` branch.{behind_note}"),
                severity="error",
                details=(
                    f"Please rebase your branch "
                    f"(`git fetch origin && git rebase origin/{base_ref}`), "
                    f"resolve the conflicts, and push again. "
                    f"See [contributing quick start]"
                    f"({_CONTRIBUTING_DOCS_URL}/03a_contributors_quick_start_beginners.rst)."
                ),
            )
        ],
        summary=f"PR #{pr_number} has merge conflicts.",
    )


def assess_pr_unresolved_comments(pr_number: int, unresolved_review_comments: int) -> PRAssessment | None:
    """Deterministically flag a PR if it has unresolved review comments from maintainers.

    Returns None if there are no unresolved comments.
    """
    if unresolved_review_comments <= 0:
        return None

    thread_word = "thread" if unresolved_review_comments == 1 else "threads"
    return PRAssessment(
        should_flag=True,
        violations=[
            Violation(
                category="Unresolved review comments",
                explanation=(
                    f"This PR has {unresolved_review_comments} unresolved review "
                    f"{thread_word} from maintainers."
                ),
                severity="warning",
                details=(
                    "Please review and resolve all inline review comments before requesting "
                    "another review. You can resolve a conversation by clicking 'Resolve conversation' "
                    "on each thread after addressing the feedback. "
                    f"See [pull request guidelines]"
                    f"({_CONTRIBUTING_DOCS_URL}/05_pull_requests.rst)."
                ),
            )
        ],
        summary=f"PR #{pr_number} has {unresolved_review_comments} unresolved review {thread_word}.",
    )
