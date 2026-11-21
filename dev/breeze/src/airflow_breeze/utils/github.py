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
import subprocess
import sys
import tempfile
import zipfile
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from rich.markup import escape

from airflow_breeze.global_constants import APACHE_AIRFLOW_GITHUB_REPOSITORY
from airflow_breeze.utils.confirm import Answer, user_confirm
from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH
from airflow_breeze.utils.shared_options import get_dry_run, get_verbose

if TYPE_CHECKING:
    from requests import Response

GITHUB_TOKEN_ENV_VARS = ("GH_TOKEN", "GITHUB_TOKEN")
# The only workflow that builds images for pull requests: ci-arm.yml does not run on them.
PR_IMAGES_WORKFLOW = "ci-amd.yml"


def env_without_github_tokens(env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Return a copy of *env* with ambient GitHub CLI token variables removed."""
    cleaned_env = dict(os.environ if env is None else env)
    for token_env_var in GITHUB_TOKEN_ENV_VARS:
        cleaned_env.pop(token_env_var, None)
    return cleaned_env


def get_github_token_from_env(env: Mapping[str, str] | None = None) -> str | None:
    """Return an ambient GitHub token using the same precedence as the GitHub CLI."""
    source_env = os.environ if env is None else env
    for token_env_var in GITHUB_TOKEN_ENV_VARS:
        token = source_env.get(token_env_var)
        if token:
            return token
    return None


def run_gh_command(
    command: Sequence[str],
    *,
    retry_with_github_token: bool = True,
    env: Mapping[str, str] | None = None,
    **kwargs: Any,
) -> subprocess.CompletedProcess[Any]:
    """
    Run a ``gh`` command using stored ``gh auth login`` credentials before ambient token env vars.

    Locally, ``GH_TOKEN``/``GITHUB_TOKEN`` can shadow the user's normal GitHub CLI login. We first
    run with those variables removed, then retry with the original environment only when that fails.
    """
    command_env = os.environ.copy() if env is None else dict(env)
    check = kwargs.pop("check", False)
    if get_dry_run():
        return subprocess.CompletedProcess(command, returncode=0, stdout="", stderr="")
    result = subprocess.run(command, env=env_without_github_tokens(command_env), check=False, **kwargs)
    if result.returncode == 0:
        return result
    if not retry_with_github_token or not get_github_token_from_env(command_env):
        if check:
            result.check_returncode()
        return result
    return subprocess.run(command, env=command_env, check=check, **kwargs)


def format_github_token_scope_guidance(*, description: str | None = None, scopes: str | None = None) -> str:
    purpose = f" for {description}" if description else ""
    if scopes:
        return f"If creating a token manually{purpose}, it needs {scopes} scope."
    return f"If creating a token manually{purpose}, make sure it has the permissions required by the command."


def retrieve_github_token(
    token: str | None = None,
    *,
    description: str | None = None,
    scopes: str | None = None,
    env: Mapping[str, str] | None = None,
) -> str | None:
    """
    Resolve a GitHub token for local Breeze commands.

    ``description`` and ``scopes`` document what the caller needs from the token. Retrieval itself
    does not validate scopes.

    Non-empty token arguments are preserved when they do not match ``GH_TOKEN`` or
    ``GITHUB_TOKEN`` from the environment. Matching values are treated as ambient env input because
    Click can populate ``--github-token`` from ``envvar="GITHUB_TOKEN"``. Ambient env tokens are used
    only after trying the user's stored ``gh auth login`` credential.
    """
    env_token = get_github_token_from_env(env)
    source_env = os.environ if env is None else env
    env_tokens = {
        source_env[token_env_var] for token_env_var in GITHUB_TOKEN_ENV_VARS if source_env.get(token_env_var)
    }
    if token and token not in env_tokens:
        return token
    try:
        gh_token_result = run_gh_command(
            ["gh", "auth", "token"],
            capture_output=True,
            text=True,
            check=False,
            retry_with_github_token=False,
            env=env,
        )
    except FileNotFoundError:
        return token or env_token
    if gh_token_result.returncode == 0 and gh_token_result.stdout.strip():
        return gh_token_result.stdout.strip()
    return token or env_token


def get_ga_output(name: str, value: Any) -> str:
    output_name = name.replace("_", "-")
    printed_value = str(value).lower() if isinstance(value, bool) else value
    console_print(f"[info]{output_name}[/] = [green]{escape(str(printed_value))}[/]")
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
        reset_dt = datetime.fromtimestamp(int(reset), tz=UTC)
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
    reference: str,
    path: str,
    output_file: Path,
    github_token: str | None = None,
    timeout: int = 60,
    github_repository: str = APACHE_AIRFLOW_GITHUB_REPOSITORY,
) -> bool:
    """
    Downloads a file from a GitHub repository (Apache Airflow by default) using the GitHub API.

    In case of any error different from 404, it will exit the process with error code 1.

    :param reference: tag to download from
    :param path: path of the file relative to the repository root
    :param output_file: Path where the file should be downloaded
    :param github_token: GitHub token to use for authentication
    :param timeout: timeout in seconds for the download request, default is 60 seconds
    :param github_repository: repository to download from, default is apache/airflow
    :return: whether the file was successfully downloaded (False if the file is missing)
    """
    import requests

    url = f"https://api.github.com/repos/{github_repository}/contents/{path}?ref={reference}"
    console_print(f"[info]Downloading {url} to {output_file}")
    if not get_dry_run():
        headers = {"Accept": "application/vnd.github.v3.raw"}
        if github_token:
            headers["Authorization"] = f"Bearer {github_token}"
            headers["X-GitHub-Api-Version"] = "2022-11-28"
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            log_github_rate_limit_error(response)
            if response.status_code == 403:
                console_print(
                    f"[error]Access denied to {url}. This may be caused by:\n"
                    f"   1. Network issues or VPN settings\n"
                    f"   2. GitHub API rate limiting\n"
                    f"   3. Invalid or missing GitHub token"
                )
                sys.exit(1)
            if response.status_code == 404:
                console_print(f"[warning]The {url} has not been found. Skipping")
                return False
            if response.status_code != 200:
                console_print(f"[error]{url} could not be downloaded. Status code {response.status_code}")
                sys.exit(1)
            output_file.write_bytes(response.content)
        except requests.Timeout:
            console_print(f"[error]The request to {url} timed out after {timeout} seconds.")
            sys.exit(1)
    console_print(f"[success]Downloaded {url} to {output_file}")
    return True


ACTIVE_TAG_MATCH = re.compile(r"^(\d+)\.\d+\.\d+$")


def get_active_airflow_versions(
    confirm: bool = True, remote_name: str = "upstream"
) -> tuple[list[str], dict[str, str]]:
    """
    Gets list of active Airflow versions from GitHub.

    :param confirm: if True, will ask the user before proceeding with the versions found
    :param remote_name: name of the remote to fetch tags from (default: 'upstream' per the
        project's git remote naming convention)
    :return: tuple: list of active Airflow versions and dict of Airflow release dates (in iso format)
    """
    from git import GitCommandError, Repo
    from packaging.version import Version

    airflow_release_dates: dict[str, str] = {}
    console_print(
        f"\n[warning]Make sure you have `{remote_name}` remote added pointing to apache/airflow repository\n"
    )
    console_print("[info]Fetching all released Airflow 2/3 versions from GitHub[/]\n")
    repo = Repo(AIRFLOW_ROOT_PATH)
    all_active_tags: list[str] = []
    try:
        # `git.ls_remote` is typed as returning the full union of git-command return shapes;
        # with plain args it is always the command output.
        ref_tags = cast("str", repo.git.ls_remote("--tags", remote_name)).splitlines()
    except GitCommandError as ex:
        console_print(
            f"[error]Could not fetch tags from `{remote_name}` remote! Make sure to have it configured.\n"
        )
        console_print(f"{ex}\n")
        console_print(
            "[info]Airflow standardises on `upstream` for the apache/airflow remote and `origin` "
            "for your fork. You can add the upstream remote with one of these commands "
            "(depending on which protocol you use):\n"
            " * git remote add upstream https://github.com/apache/airflow.git\n"
            " * git remote add upstream git@github.com:apache/airflow.git\n"
            "If you already have it configured under a different name, rename it with "
            "`git remote rename <old-name> upstream`, or pass `--remote-name <old-name>`.\n"
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
            console_print(f"[error]Error fetching tag date for Airflow {version}")
            sys.exit(1)
        airflow_release_dates[version] = date
    console_print("[info]All Airflow 2/3 versions loaded from GitHub[/]")
    if get_verbose():
        console_print("[info]Found active Airflow versions:[/]")
        console_print(airflow_versions)
    if confirm:
        for version in airflow_versions:
            console_print(f"  {version}: [info]{airflow_release_dates[version]}[/]")
        answer = user_confirm(
            "Should we continue with those versions?", quit_allowed=False, default_answer=Answer.YES
        )
        if answer == Answer.NO:
            console_print("[red]Aborting[/]")
            sys.exit(1)
    return airflow_versions, airflow_release_dates


def get_constraints_github_repository() -> str:
    """
    Returns the repository keeping the constraints branches.

    It is CONSTRAINTS_GITHUB_REPOSITORY when set, the variable the Dockerfiles read for the same purpose,
    and apache/airflow otherwise.
    """
    return os.environ.get("CONSTRAINTS_GITHUB_REPOSITORY") or APACHE_AIRFLOW_GITHUB_REPOSITORY


def download_constraints_file(
    constraints_reference: str,
    python_version: str,
    github_token: str | None,
    airflow_constraints_mode: str,
    output_file: Path,
) -> bool:
    """
    Downloads constraints file from the constraints repository (see get_constraints_github_repository)

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
        github_repository=get_constraints_github_repository(),
    )


def get_branch_commit(branch: str, github_token: str | None = None, timeout: int = 60) -> str | None:
    """
    Returns the commit a branch of the constraints repository (see get_constraints_github_repository)
    points to.

    Unlike the downloads above, it never exits the process: any error is reported as a warning.

    :param branch: branch to resolve
    :param github_token: GitHub token to use for authentication
    :param timeout: timeout in seconds for the request, default is 60 seconds
    :return: the commit SHA, or None if it could not be read
    """
    import requests

    url = f"https://api.github.com/repos/{get_constraints_github_repository()}/commits/{branch}"
    headers = {"Accept": "application/vnd.github.sha"}
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"
        headers["X-GitHub-Api-Version"] = "2022-11-28"
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
    except requests.RequestException as e:
        console_print(f"[warning]Could not read the commit of {branch}: {e}")
        return None
    commit = response.text.strip()
    if response.status_code != 200 or not re.fullmatch(r"[0-9a-f]{40}", commit):
        log_github_rate_limit_error(response)
        console_print(f"[warning]Could not read the commit of {branch}. Status code {response.status_code}")
        return None
    return commit


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
        console_print(f"[warning]Tag {tag} not found in the repository")
        return None
    timestamp: int = (
        tag_object.committed_date if hasattr(tag_object, "committed_date") else tag_object.tagged_date
    )
    return datetime.fromtimestamp(timestamp, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def download_artifact_from_run_id(
    run_id: str, artifact_prefix: str, output_file: Path, github_repository: str, github_token: str
):
    """
    Downloads an image from an artifact of a GitHub Actions run

    :param run_id: run_id of the workflow
    :param artifact_prefix: stash key of the image: the artifact name starts with this key
    :param output_file: Path where the image file should be stored
    :param github_repository: GitHub repository
    :param github_token: GitHub token
    """
    import requests
    from tqdm import tqdm

    url: str | None = (
        f"https://api.github.com/repos/{github_repository}/actions/runs/{run_id}/artifacts?per_page=100"
    )
    headers = {"Accept": "application/vnd.github.v3+json"}

    session = requests.Session()
    headers["Authorization"] = f"Bearer {github_token}"

    artifacts = []
    while url:
        artifact_response = session.get(url, headers=headers)

        if artifact_response.status_code != 200:
            console_print(
                "[error]Describing artifacts failed with status code "
                f"{artifact_response.status_code}: {artifact_response.text}",
            )
            sys.exit(1)

        artifacts.extend(artifact_response.json()["artifacts"])
        url = artifact_response.links.get("next", {}).get("url")

    artifact = next((a for a in artifacts if a["name"].startswith(artifact_prefix)), None)

    if not artifact:
        console_print(
            f"[error]Run {run_id} has no artifact for {artifact_prefix}. The run may still be in progress, "
            "or it may not have needed an image. Images are kept for only 2 days, so a run older than that "
            "no longer has an image."
        )
        other_images = sorted(a["name"] for a in artifacts if "-image-save-" in a["name"])
        if other_images:
            console_print(
                f"[info]Image artifacts of this run: {', '.join(other_images)}. "
                "Set --python and --platform to match one of these artifacts."
            )
        sys.exit(1)

    download_url = artifact["archive_download_url"]
    console_print(f"[info]Downloading artifact {artifact['name']} from {download_url} to {output_file}")

    response = session.get(download_url, stream=True, headers=headers)

    if response.status_code != 200:
        console_print(
            f"[error]Downloading artifacts failed with status code {response.status_code}: {response.text}",
        )
        sys.exit(1)

    total_size = int(response.headers.get("content-length", 0))
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(dir=output_file.parent) as temp_dir:
        temp_file = os.path.join(temp_dir, "file.zip")

        with tqdm(total=total_size, unit="B", unit_scale=True, desc=temp_file, ascii=True) as progress_bar:
            with open(temp_file, "wb") as f:
                for chunk in response.iter_content(chunk_size=1 * 1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        progress_bar.update(len(chunk))

        with zipfile.ZipFile(temp_file, "r") as zip_ref:
            members = zip_ref.namelist()
            image = next((member for member in members if member.endswith(".tar")), None)
            if not image:
                console_print(
                    f"[error]Artifact {artifact['name']} of run {run_id} holds no image. It holds only "
                    f"{', '.join(members)}. A run on RunsOn runners saves its image in an S3 cache, and its "
                    "artifact holds only the digest of the image. So Breeze cannot download that image. "
                    f"Build the image locally from the commit of run {run_id} instead."
                )
                sys.exit(1)
            os.replace(zip_ref.extract(image, temp_dir), output_file)


def download_artifact_from_pr(
    pr: str, artifact_prefix: str, output_file: Path, github_repository: str, github_token: str
):
    import requests

    pr_number = pr.lstrip("#")
    pr_url = f"https://api.github.com/repos/{github_repository}/pulls/{pr_number}"
    workflow_run_url = (
        f"https://api.github.com/repos/{github_repository}/actions/workflows/{PR_IMAGES_WORKFLOW}/runs"
    )

    headers = {"Accept": "application/vnd.github.v3+json"}

    session = requests.Session()
    headers["Authorization"] = f"Bearer {github_token}"

    pull_response = session.get(pr_url, headers=headers)

    if pull_response.status_code != 200:
        console_print(
            f"[error]Fetching PR failed with status code {pull_response.status_code}: {pull_response.text}",
        )
        sys.exit(1)

    # Find the run by commit. PRs from different forks can use the same branch name, such as main.
    head_sha = pull_response.json()["head"]["sha"]

    workflow_runs = session.get(
        workflow_run_url, headers=headers, params={"event": "pull_request", "head_sha": head_sha}
    )

    if workflow_runs.status_code != 200:
        console_print(
            f"[error]Fetching workflow runs failed with status code {workflow_runs.status_code}: "
            f"{workflow_runs.text}. You might need to provide GITHUB_TOKEN, set it as environment variable",
        )
        sys.exit(1)

    runs = workflow_runs.json()["workflow_runs"]
    if not runs:
        console_print(
            f"[error]PR {pr} has no {PR_IMAGES_WORKFLOW} run for its head commit {head_sha}. "
            "Wait for CI to start, or use --from-run with a run of an earlier commit."
        )
        sys.exit(1)

    run_id = max(runs, key=lambda run: datetime.fromisoformat(run["created_at"]))["id"]

    console_print(f"[info]Found run id {run_id} for PR {pr}")

    download_artifact_from_run_id(str(run_id), artifact_prefix, output_file, github_repository, github_token)
