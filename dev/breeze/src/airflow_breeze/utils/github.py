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
    # Filter only workflow with ci.yml, we may get multiple workflows for a PR ex: codeql-analysis.yml, news-fragment.yml

    for run in sorted_data:
        if run.get("path").endswith("ci.yml"):
            run_id = run["id"]
            break

    get_console().print(f"[info]Found run id {run_id} for PR {pr}")

    download_artifact_from_run_id(str(run_id), output_file, github_repository, github_token)


def checkout_target_branch(target_branch: str) -> None:
    """
    Checks out the target branch in the given repository.
    Only use when you are trying to run commands that require being on a specific branch.
    This would require to take an action branch too to make difference between which branch we are on.
    For example, we first need this to generate constraints to run integration tests with multiple versions of Airflow.

    :param target_branch: Target branch name
    """
    from git import GitCommandError, Repo

    repo = Repo(AIRFLOW_ROOT_PATH)
    try:
        get_console().print(f"[info]Fetching branch {target_branch} from remote")
        repo.git.fetch("origin", target_branch)
        repo.git.checkout(f"origin/{target_branch}")
    except GitCommandError as e:
        get_console().print(f"[error]Failed to checkout branch {target_branch}: {e}")
        sys.exit(1)
