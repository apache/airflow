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
import shutil
import sys
from datetime import date
from pathlib import Path

import click

from airflow_breeze.commands.common_options import (
    option_answer,
    option_dry_run,
    option_verbose,
    option_version_suffix,
)
from airflow_breeze.commands.release_management_group import release_management_group
from airflow_breeze.global_constants import (
    TarBallType,
    get_airflow_version,
    get_airflowctl_version,
    get_task_sdk_version,
)
from airflow_breeze.utils.confirm import confirm_action
from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.custom_param_types import BetterChoice
from airflow_breeze.utils.path_utils import (
    AIRFLOW_DIST_PATH,
    AIRFLOW_ROOT_PATH,
    OUT_PATH,
)
from airflow_breeze.utils.reproducible import get_source_date_epoch, repack_deterministically
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.shared_options import get_dry_run


def validate_remote_tracks_apache_airflow(remote_name):
    """Validate that the specified remote tracks the apache/airflow repository."""
    console_print(f"[info]Validating remote '{remote_name}' tracks apache/airflow...")

    result = run_command(
        ["git", "remote", "get-url", remote_name],
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        console_print(f"[error]Remote '{remote_name}' does not exist!")
        console_print("Available remotes:")
        run_command(["git", "remote", "-v"])
        exit(1)

    remote_url = result.stdout.strip()

    # Check if it's the apache/airflow repository
    apache_patterns = [
        "https://github.com/apache/airflow",
        "https://github.com/apache/airflow.git",
        "git@github.com:apache/airflow",
        "git@github.com:apache/airflow.git",
        "ssh://git@github.com/apache/airflow",
        "ssh://git@github.com/apache/airflow.git",
    ]

    is_apache_repo = any(pattern in remote_url for pattern in apache_patterns)

    if not is_apache_repo:
        console_print(f"[error]Remote '{remote_name}' does not track apache/airflow!")
        console_print(f"Remote URL: {remote_url}")
        console_print("Expected patterns: apache/airflow")
        if not confirm_action("Do you want to continue anyway? This is NOT recommended for releases."):
            exit(1)
    console_print(f"[success]Remote '{remote_name}' correctly tracks apache/airflow")


def validate_git_status():
    """Validate that git working directory is clean."""
    console_print("[info]Validating git status...")

    # Check if working directory is clean
    result = run_command(
        ["git", "status", "--porcelain"],
        check=True,
        capture_output=True,
        text=True,
    )

    if result.stdout.strip():
        console_print("[error]Working directory is not clean!")
        run_command(["git", "status"])
        if not confirm_action("Do you want to continue with uncommitted changes? This is NOT recommended."):
            exit(1)
    console_print("[success]Working directory is clean")


def validate_version_branches_exist(version_branch, remote_name):
    """Validate that the required version branches exist."""
    console_print(f"[info]Validating version branches exist for {version_branch}...")

    # Check if test branch exists
    test_branch = f"v{version_branch}-test"
    stable_branch = f"v{version_branch}-stable"

    # Fetch to get latest remote branches
    run_command(["git", "fetch", remote_name], check=True)

    # Check remote branches
    result = run_command(
        ["git", "branch", "-r"],
        check=True,
        capture_output=True,
        text=True,
    )
    remote_branches = result.stdout

    test_branch_exists = f"{remote_name}/{test_branch}" in remote_branches
    stable_branch_exists = f"{remote_name}/{stable_branch}" in remote_branches

    if not test_branch_exists:
        console_print(f"[error]Test branch '{remote_name}/{test_branch}' does not exist!")
        console_print("Available remote branches:")
        run_command(["git", "branch", "-r"])
        exit(1)
    console_print(f"[success]Test branch '{remote_name}/{test_branch}' exists")

    if not stable_branch_exists:
        console_print(f"[error]Stable branch '{remote_name}/{stable_branch}' does not exist!")
        console_print("Available remote branches:")
        run_command(["git", "branch", "-r"])
        exit(1)
    console_print(f"[success]Stable branch '{remote_name}/{stable_branch}' exists")


def validate_tag_does_not_exist(version, remote_name):
    """Validate that the release tag doesn't already exist."""
    console_print(f"[info]Checking if tag '{version}' already exists...")

    # Check if tag exists locally
    local_result = run_command(
        ["git", "tag", "-l", version],
        check=True,
        capture_output=True,
        text=True,
    )

    # Check if tag exists on remote using ls-remote with --exit-code
    remote_result = run_command(
        ["git", "ls-remote", "--exit-code", "--tags", remote_name, f"refs/tags/{version}"],
        check=False,
    )

    tag_exists_locally = bool(local_result.stdout.strip())
    tag_exists_remotely = remote_result.returncode == 0

    if not tag_exists_locally and not tag_exists_remotely:
        console_print(f"[success]Tag '{version}' does not exist yet")
        return

    location = []
    if tag_exists_locally:
        location.append("locally")
    if tag_exists_remotely:
        location.append("remotely")

    console_print(f"[error]Tag '{version}' already exists {' and '.join(location)}!")

    if tag_exists_locally:
        console_print(f"Use 'git tag -d {version}' to delete it locally if needed")
    if tag_exists_remotely:
        console_print(f"Use 'git push {remote_name} --delete {version}' to delete it remotely if needed")

    if not confirm_action("Do you want to continue anyway? This may cause issues."):
        exit(1)


def validate_on_correct_branch_for_tagging(version_branch):
    """Validate that we're on the correct branch for tagging (stable branch)."""
    console_print("[info]Validating we're on the correct branch for tagging...")

    expected_branch = f"v{version_branch}-stable"

    # Check current branch
    result = run_command(
        ["git", "branch", "--show-current"],
        check=True,
        capture_output=True,
        text=True,
    )
    current_branch = result.stdout.strip()

    if current_branch != expected_branch:
        console_print(f"[error]Currently on branch '{current_branch}', expected '{expected_branch}'!")
        console_print("Tags should be created on the stable branch after merging the sync PR.")
        console_print("Make sure the PR merge step completed successfully.")
        exit(1)
    console_print(f"[success]On correct branch '{expected_branch}' for tagging")


def merge_pr(version_branch, remote_name, sync_branch):
    if confirm_action("Do you want to merge the Sync PR?"):
        run_command(
            [
                "git",
                "checkout",
                f"v{version_branch}-stable",
            ],
            check=True,
        )
        run_command(
            ["git", "reset", "--hard", f"{remote_name}/v{version_branch}-stable"],
            check=True,
        )
        run_command(
            ["git", "merge", "--ff-only", f"{sync_branch}"],
            check=True,
        )
        if confirm_action("Do you want to push the changes? Pushing the changes closes the PR"):
            run_command(
                ["git", "push", remote_name, f"v{version_branch}-stable"],
                check=True,
            )


def git_tag(version, message):
    if confirm_action(f"Tag {version}?"):
        run_command(
            ["git", "tag", "-s", f"{version}", "-m", message],
            check=True,
        )
        console_print(f"[success]Tagged {version}!")


def git_clean():
    if confirm_action("Clean git repo?"):
        run_command(["breeze", "ci", "fix-ownership"], check=True)
        run_command(["git", "clean", "-fxd"], check=True)
        console_print("[success]Git repo cleaned")


def tarball_release(
    version: str,
    source_date_epoch: int,
    tarball_type: TarBallType,
    tag: str | None = None,
):
    tag = version if tag is None else tag
    console_print(f"[info]Creating tarball for {tarball_type.value} {version}, tag: {tag}\n")
    shutil.rmtree(OUT_PATH, ignore_errors=True)
    OUT_PATH.mkdir(exist_ok=True)
    AIRFLOW_DIST_PATH.mkdir(exist_ok=True)
    archive_name = f"{tarball_type.value}-{version}-source.tar.gz"
    temporary_archive = OUT_PATH / archive_name
    result = run_command(
        [
            "git",
            "-c",
            "tar.umask=0077",
            "archive",
            "--format=tar.gz",
            tag,
            f"--prefix={tarball_type.value}-{version}-source/",
            "-o",
            temporary_archive.as_posix(),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        if "fatal: not a valid object" in result.stderr:
            console_print()
            console_print(f"[error]Git tag '{tag}' does not exist!")
            console_print()
            sys.exit(1)
        console_print(
            f"[error]Failed to create tarball {temporary_archive} for Apache {tarball_type.value} {version}"
        )
        console_print(f"[error]{result.stderr}")
        exit(result.returncode)
    final_archive = AIRFLOW_DIST_PATH / archive_name
    result = repack_deterministically(
        source_archive=temporary_archive,
        dest_archive=final_archive,
        prepend_path=None,
        timestamp=source_date_epoch,
    )
    if result.returncode != 0:
        console_print(f"[error]Failed to create tarball {temporary_archive} for Airflow {version}")
        exit(result.returncode)
    console_print(f"[success]Tarball created in {final_archive}")


def create_tarball_release(
    tarball_type: TarBallType,
    version: str | None,
    version_suffix: str,
):
    if tarball_type == TarBallType.AIRFLOW:
        tag = version + version_suffix if version else "HEAD"
        if not version:
            version = get_airflow_version()
            console_print(f"\n[info]Using {version} retrieved from airflow-core as tarball version\n")
    elif tarball_type == TarBallType.TASK_SDK:
        tag = f"task-sdk/{version + version_suffix}" if version else "HEAD"
        if not version:
            version = get_task_sdk_version()
            console_print(f"\n[info]Using {version} retrieved from task-sdk as tarball version\n")
    elif tarball_type == TarBallType.AIRFLOW_CTL:
        tag = f"airflow-ctl/{version + version_suffix}" if version else "HEAD"
        if not version:
            version = get_airflowctl_version()
            console_print(f"\n[info]Using {version} retrieved from airflow-ctl as tarball version\n")
    elif tarball_type == TarBallType.PROVIDERS:
        tag = f"providers/{version + version_suffix}" if version else "HEAD"
        if not version:
            version = date.strftime(date.today(), "%Y-%m-%d")
            console_print(f"\n[info]Using current date {version} as tarball version\n")
    elif tarball_type == TarBallType.PYTHON_CLIENT:
        tag = f"python-client/{version + version_suffix}" if version else "HEAD"
        if not version:
            version = date.strftime(date.today(), "%Y-%m-%d")
            console_print(f"\n[info]Using current date {version} as tarball version\n")
    else:  # pragma: no cover
        console_print(f"[error]Unsupported tarball type: {tarball_type}")
        exit(1)
    source_date_epoch = get_source_date_epoch(AIRFLOW_ROOT_PATH)
    # Create the tarball
    tarball_release(
        version=version,
        source_date_epoch=source_date_epoch,
        tarball_type=tarball_type,
        tag=tag,
    )


def create_artifacts_with_hatch(source_date_epoch: int):
    console_print("[info]Creating artifacts with hatch")
    shutil.rmtree(AIRFLOW_DIST_PATH, ignore_errors=True)
    AIRFLOW_DIST_PATH.mkdir(exist_ok=True)
    env_copy = os.environ.copy()
    env_copy["SOURCE_DATE_EPOCH"] = str(source_date_epoch)
    # Build Airflow packages
    run_command(
        ["hatch", "build", "-c", "-t", "custom", "-t", "sdist", "-t", "wheel"], check=True, env=env_copy
    )
    # Build Task SDK packages
    run_command(
        [
            "breeze",
            "release-management",
            "prepare-task-sdk-distributions",
            "--distribution-format",
            "both",
            "--use-local-hatch",
        ],
        check=True,
    )
    console_print("[success]Successfully prepared Airflow and Task SDK packages:")
    for file in sorted(AIRFLOW_DIST_PATH.glob("apache_airflow*")):
        console_print(print(file.name))
    for file in sorted(AIRFLOW_DIST_PATH.glob("*task*")):
        console_print(print(file.name))
    console_print()


def create_artifacts_with_docker():
    console_print("[info]Creating artifacts with docker")
    run_command(
        [
            "breeze",
            "release-management",
            "prepare-airflow-distributions",
            "--distribution-format",
            "both",
            "--version-suffix",
            "",
        ],
        check=True,
    )
    run_command(
        [
            "breeze",
            "release-management",
            "prepare-task-sdk-distributions",
            "--distribution-format",
            "both",
            "--version-suffix",
            "",
        ],
        check=True,
    )
    console_print("[success]Airflow and Task SDK artifacts created")


def sign_the_release(repo_root):
    if confirm_action("Do you want to sign the release?"):
        os.chdir(f"{repo_root}/dist")
        run_command("./../dev/sign.sh *", check=True, shell=True)
        console_print("[success]Release signed")


def tag_and_push_constraints(version, version_branch, remote_name):
    if confirm_action("Do you want to tag and push constraints?"):
        run_command(
            ["git", "checkout", f"{remote_name}/constraints-{version_branch}"],
            check=True,
        )
        run_command(
            [
                "git",
                "tag",
                "-s",
                f"constraints-{version}",
                "-m",
                f"Constraints for Apache Airflow {version}",
            ],
            check=True,
        )
        run_command(
            ["git", "push", remote_name, "tag", f"constraints-{version}"],
            check=True,
        )
        console_print("[success]Constraints tagged and pushed")


def clone_asf_repo(version, repo_root):
    if confirm_action("Do you want to clone asf repo?"):
        os.chdir(repo_root)
        run_command(
            ["svn", "checkout", "--depth=immediates", "https://dist.apache.org/repos/dist", "asf-dist"],
            check=True,
            dry_run_override=False,
        )
        run_command(
            ["svn", "update", "--set-depth=infinity", "asf-dist/dev/airflow"],
            check=True,
            dry_run_override=False,
        )
        console_print("[success]Cloned ASF repo successfully")


def move_artifacts_to_svn(
    version, version_without_rc, task_sdk_version, task_sdk_version_without_rc, repo_root
):
    if confirm_action("Do you want to move artifacts to SVN?"):
        os.chdir(f"{repo_root}/asf-dist/dev/airflow")
        run_command(["svn", "mkdir", f"{version}"], check=True)
        run_command(f"mv {repo_root}/dist/*{version_without_rc}* {version}/", check=True, shell=True)
        run_command(["svn", "mkdir", f"task-sdk/{task_sdk_version}"])
        run_command(
            f"mv {repo_root}/dist/*{task_sdk_version_without_rc}* task-sdk/{task_sdk_version}/",
            check=True,
            shell=True,
        )
        console_print("[success]Moved artifacts to SVN:")
        run_command(["ls"])
        run_command([f"ls {version}"])
        run_command([f"ls task-sdk/{task_sdk_version}"])


def push_artifacts_to_asf_repo(version, task_sdk_version, repo_root):
    if confirm_action("Do you want to push artifacts to ASF repo?"):
        console_print("Airflow Version Files to push to svn:")
        if not get_dry_run():
            os.chdir(f"{repo_root}/asf-dist/dev/airflow/{version}")
        run_command(["ls"])
        confirm_action("Do you want to continue?", abort=True)
        run_command("svn add *", check=True, shell=True)
        console_print("Task SDK Version Files to push to svn:")
        if not get_dry_run():
            os.chdir(f"{repo_root}/asf-dist/dev/airflow/task-sdk/{task_sdk_version}")
        run_command(["ls"])
        confirm_action("Do you want to continue?", abort=True)
        run_command("svn add *", check=True, shell=True)
        run_command(
            ["svn", "commit", "-m", f"Add artifacts for Airflow {version} and Task SDK {task_sdk_version}"],
            check=True,
        )
        console_print("[success]Files pushed to svn")
        console_print(
            "Verify that the files are available here: https://dist.apache.org/repos/dist/dev/airflow/"
        )


def delete_asf_repo(repo_root):
    os.chdir(repo_root)
    if confirm_action("Do you want to remove the cloned asf repo?"):
        run_command(["rm", "-rf", "asf-dist"], check=True)


def prepare_pypi_packages(version, version_suffix, repo_root):
    if confirm_action("Prepare pypi packages?"):
        console_print("[info]Preparing PyPI packages")
        os.chdir(repo_root)
        run_command(["git", "checkout", f"{version}"], check=True)
        run_command(
            [
                "breeze",
                "release-management",
                "prepare-airflow-distributions",
                "--version-suffix",
                f"{version_suffix}",
                "--distribution-format",
                "both",
            ],
            check=True,
        )
        # Task SDK
        run_command(
            [
                "breeze",
                "release-management",
                "prepare-task-sdk-distributions",
                "--version-suffix",
                f"{version_suffix}",
                "--distribution-format",
                "both",
            ],
            check=True,
        )
        files_to_check = []
        for files in Path(AIRFLOW_DIST_PATH).glob("apache_airflow*"):
            if "-sources" not in files.name:
                files_to_check.append(files.as_posix())
        run_command(["twine", "check", *files_to_check], check=True)
        console_print("[success]PyPI packages prepared")


def push_packages_to_pypi(version):
    if confirm_action("Do you want to push packages to production PyPI?"):
        run_command(["twine", "upload", "-r", "pypi", "dist/*"], check=True)
        console_print("[success]Packages pushed to production PyPI")
        console_print(
            "Again, confirm that the package is available here: https://pypi.python.org/pypi/apache-airflow"
        )
        console_print(
            "Verify that the package looks good by downloading it and installing it into a virtual "
            "environment. "
            "Install it with the appropriate constraint file, adapt python version: "
            f"pip install apache-airflow=={version} --constraint "
            f"https://raw.githubusercontent.com/apache/airflow/"
            f"constraints-{version}/constraints-3.9.txt"
        )
        confirm_action(
            "I have tested the package I uploaded to PyPI. "
            "I installed and ran a DAG with it and there's no issue. Do you agree to the above?",
            abort=True,
        )
        console_print(
            """
            It is important to stress that this snapshot should not be named "release", and it
            is not supposed to be used by and advertised to the end-users who do not read the devlist.
            """
        )


def push_release_candidate_tag_to_github(version, remote_name):
    if confirm_action("Do you want to push release candidate tags to GitHub?"):
        console_print(
            """
        This step should only be done now and not before, because it triggers an automated
        build of the production docker image, using the packages that are currently released
        in PyPI (both airflow and latest provider distributions).
        """
        )
        confirm_action(f"Confirm that {version} is pushed to PyPI(not PyPI test). Is it pushed?", abort=True)
        run_command(["git", "push", remote_name, "tag", f"{version}"], check=True)
        console_print("[success]Release candidate tag pushed to GitHub")


def remove_old_releases(version, repo_root):
    if confirm_action("In beta release we do not remove old RCs. Is this a beta release?"):
        return
    if not confirm_action("Do you want to look for old RCs to remove?"):
        return

    os.chdir(f"{repo_root}/asf-dist/dev/airflow")

    old_releases = []
    for entry in os.scandir():
        if entry.name == version:
            # Don't remove the current RC
            continue
        if entry.is_dir() and entry.name.startswith("2."):
            old_releases.append(entry.name)
    old_releases.sort()

    for old_release in old_releases:
        if confirm_action(f"Remove old RC {old_release}?"):
            run_command(["svn", "rm", old_release], check=True)
            run_command(
                ["svn", "commit", "-m", f"Remove old release: {old_release}"],
                check=True,
            )
    console_print("[success]Old releases removed")
    os.chdir(repo_root)


@release_management_group.command(
    name="prepare-tarball",
    help="Prepare source tarball.",
)
@click.option(
    "--tarball-type",
    default=TarBallType.AIRFLOW.value,
    type=BetterChoice(sorted([e.value for e in TarBallType])),
    show_default=True,
    envvar="TARBALL_TYPE",
    help="The type of tarball to build",
)
@click.option(
    "--version",
    type=str,
    help="Version to build the tarball for. Must have a corresponding tag in git. "
    "If not specified, the HEAD of current branch will be used and version will be retrieved from there.",
    envvar="VERSION",
)
@option_version_suffix
@option_dry_run
@option_verbose
def prepare_tarball(
    tarball_type: str,
    version: str | None,
    version_suffix: str,
):
    enum_tarball_type = TarBallType(tarball_type)
    create_tarball_release(
        version=version,
        version_suffix=version_suffix,
        tarball_type=enum_tarball_type,
    )


@release_management_group.command(
    name="start-rc-process",
    short_help="Start RC process",
    help="Start the process for releasing a new RC.",
)
@click.option("--version", required=True, help="The release candidate version e.g. 2.4.3rc1")
@click.option("--previous-version", required=True, help="Previous version released e.g. 2.4.2")
@click.option("--task-sdk-version", required=True, help="The task SDK version e.g. 1.0.6rc1.")
@click.option(
    "--sync-branch", required=True, help="The branch of the sync PR. Can be the test branch. Please specify"
)
@click.option(
    "--github-token", help="GitHub token to use in generating issue for testing of release candidate"
)
@click.option("--remote-name", default="origin", help="Git remote name to push to (default: origin)")
@option_answer
@option_dry_run
@option_verbose
def publish_release_candidate(
    version, previous_version, task_sdk_version, sync_branch, github_token, remote_name
):
    from packaging.version import Version

    airflow_version = Version(version)
    if not airflow_version.is_prerelease:
        exit("--version value must be a pre-release")
    if Version(previous_version).is_prerelease:
        exit("--previous-version value must be a release not a pre-release")
    if not github_token:
        github_token = os.environ.get("GITHUB_TOKEN")
        if not github_token:
            console_print("GITHUB_TOKEN is not set! Issue generation will fail.")
            confirm_action("Do you want to continue?", abort=True)

    version_suffix = airflow_version.pre[0] + str(airflow_version.pre[1])
    version_branch = str(airflow_version.release[0]) + "-" + str(airflow_version.release[1])
    version_without_rc = airflow_version.base_version

    task_sdk_version_obj = Version(task_sdk_version)
    task_sdk_version_without_rc = task_sdk_version_obj.base_version

    os.chdir(AIRFLOW_ROOT_PATH)
    airflow_repo_root = os.getcwd()

    if not get_dry_run():
        console_print("[info]Skipping validations in dry-run mode")
        validate_remote_tracks_apache_airflow(remote_name)
        validate_git_status()
        validate_version_branches_exist(version_branch, remote_name)
        validate_tag_does_not_exist(version, remote_name)
        validate_tag_does_not_exist(f"task-sdk/{task_sdk_version}", remote_name)

    # List the above variables and ask for confirmation
    console_print()
    console_print(f"Previous version: {previous_version}")
    console_print(f"Airflow version: {version}")
    console_print(f"Task SDK version: {task_sdk_version}")
    console_print(f"version_suffix: {version_suffix}")
    console_print(f"version_branch: {version_branch}")
    console_print(f"version_without_rc: {version_without_rc}")
    console_print(f"task_sdk_version_without_rc: {task_sdk_version_without_rc}")
    console_print(f"airflow_repo_root: {airflow_repo_root}")
    console_print(f"remote_name: {remote_name}")
    console_print(f"sync_branch: {sync_branch}")
    console_print()
    console_print(f"Below are your git remotes. We will push to {remote_name}:")
    run_command(["git", "remote", "-v"])
    console_print()
    confirm_action("Verify that the above information is correct. Do you want to continue?", abort=True)
    # Merge the sync PR
    merge_pr(version_branch, remote_name, sync_branch)
    #
    # # Tag & clean the repo
    # Validate we're on the correct branch before tagging
    if not get_dry_run():
        validate_on_correct_branch_for_tagging(version_branch)
    git_tag(version, f"Apache Airflow {version}")
    git_tag(f"task-sdk/{task_sdk_version}", f"Airflow Task SDK {task_sdk_version}")
    git_clean()
    source_date_epoch = get_source_date_epoch(AIRFLOW_ROOT_PATH)
    shutil.rmtree(AIRFLOW_DIST_PATH, ignore_errors=True)
    # Create the artifacts
    if confirm_action("Use docker to create artifacts?"):
        create_artifacts_with_docker()
    elif confirm_action("Use hatch to create artifacts?"):
        create_artifacts_with_hatch(source_date_epoch)
    if confirm_action("Create tarball?"):
        # Create the tarball
        tarball_release(
            version=version_without_rc,
            source_date_epoch=source_date_epoch,
            tarball_type=TarBallType.AIRFLOW,
            tag=version,
        )
    # Sign the release
    sign_the_release(airflow_repo_root)
    # Tag and push constraints
    tag_and_push_constraints(version, version_branch, remote_name)
    # Clone the asf repo
    clone_asf_repo(version, airflow_repo_root)
    # Move artifacts to SVN
    move_artifacts_to_svn(
        version, version_without_rc, task_sdk_version, task_sdk_version_without_rc, airflow_repo_root
    )
    # Push the artifacts to the asf repo
    push_artifacts_to_asf_repo(version, task_sdk_version, airflow_repo_root)

    # Remove old releases
    remove_old_releases(version, airflow_repo_root)

    # Delete asf-dist directory
    delete_asf_repo(airflow_repo_root)

    # Prepare the pypi packages
    prepare_pypi_packages(version, version_suffix, airflow_repo_root)

    # Push the packages to pypi
    push_packages_to_pypi(version)

    # Push the release candidate tag to gitHub
    push_release_candidate_tag_to_github(version, remote_name)
    push_release_candidate_tag_to_github(f"task-sdk/{task_sdk_version}", remote_name)
    # Create issue for testing
    os.chdir(airflow_repo_root)

    console_print()
    console_print("Done!")
