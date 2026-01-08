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
import time
from subprocess import CalledProcessError

import click

from airflow_breeze.commands.common_options import option_answer, option_dry_run, option_verbose
from airflow_breeze.commands.release_management_group import release_management_group
from airflow_breeze.utils.confirm import confirm_action
from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.shared_options import get_dry_run

# Pattern to match Airflow release versions (e.g., "3.0.5")
RELEASE_PATTERN = re.compile(r"^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)$")


SVN_NUM_TRIES = 3


def clone_asf_repo(working_dir):
    if confirm_action("Clone ASF repo?"):
        run_command(["rm", "-rf", f"{working_dir}/asf-dist"], check=True)

        # SVN checkout with retries
        for attempt in range(SVN_NUM_TRIES):
            try:
                run_command(
                    [
                        "svn",
                        "checkout",
                        "--depth=immediates",
                        "https://dist.apache.org/repos/dist",
                        "asf-dist",
                    ],
                    check=True,
                    dry_run_override=False,
                )
                break
            except CalledProcessError:
                if attempt == SVN_NUM_TRIES - 1:
                    raise
                console_print(
                    f"[warning]SVN checkout failed. Retrying! {SVN_NUM_TRIES - attempt - 1} tries left."
                )
                time.sleep(5)

        dev_dir = f"{working_dir}/asf-dist/dev/airflow"
        release_dir = f"{working_dir}/asf-dist/release/airflow"
        run_command(["svn", "update", "--set-depth", "infinity", dev_dir], dry_run_override=False, check=True)
        run_command(
            ["svn", "update", "--set-depth", "infinity", release_dir], dry_run_override=False, check=True
        )


def find_latest_release_candidate(version, svn_dev_repo, component="airflow"):
    """
    Find the latest release candidate for a given version from SVN dev directory.

    :param version: The base version (e.g., "3.0.5")
    :param svn_dev_repo: Path to the SVN dev repository
    :param component: Component name ("airflow" or "task-sdk")
    :return: The latest release candidate string (e.g., "3.0.5rc3") or None if not found
    """
    if component == "task-sdk":
        search_dir = f"{svn_dev_repo}/task-sdk"
    else:
        search_dir = svn_dev_repo

    if not os.path.exists(search_dir):
        return None

    # Pattern to match release candidates for this version (e.g., "3.0.5rc1", "3.0.5rc2")
    pattern = re.compile(rf"^{re.escape(version)}rc(\d+)$")
    matching_rcs = []

    try:
        entries = os.listdir(search_dir)
        for entry in entries:
            match = pattern.match(entry)
            if match:
                rc_number = int(match.group(1))
                matching_rcs.append((rc_number, entry))

        if not matching_rcs:
            return None

        # Sort by RC number and return the latest
        matching_rcs.sort(key=lambda x: x[0], reverse=True)
        latest_rc = matching_rcs[0][1]
        console_print(f"Found latest {component} release candidate: {latest_rc}")
        return latest_rc
    except OSError:
        return None


def create_version_dir(version, task_sdk_version=None):
    if confirm_action(f"Create SVN version directory for Airflow {version}?"):
        run_command(["svn", "mkdir", f"{version}"], check=True)
        console_print(f"Airflow {version} directory created")

    if task_sdk_version and confirm_action(f"Create SVN version directory for Task SDK {task_sdk_version}?"):
        run_command(["svn", "mkdir", f"task-sdk/{task_sdk_version}"], check=True)
        console_print(f"Task SDK {task_sdk_version} directory created")


def copy_artifacts_to_svn(rc, task_sdk_rc, svn_dev_repo, svn_release_repo):
    if confirm_action(f"Copy Airflow artifacts to SVN for {rc}?"):
        bash_command = f"""
        for f in {svn_dev_repo}/{rc}/*; do
            svn cp "$f" "$(basename "$f")/"
        done
        """

        run_command(
            [
                "bash",
                "-c",
                bash_command,
            ],
            check=True,
        )
        console_print("Airflow artifacts copied to SVN:")
        run_command(["ls"])

    if task_sdk_rc and confirm_action(f"Copy Task SDK artifacts to SVN for {task_sdk_rc}?"):
        # Save current directory
        current_dir = os.getcwd()
        # Change to task-sdk release directory
        task_sdk_version = task_sdk_rc[:-3]
        os.chdir(f"{svn_release_repo}/task-sdk/{task_sdk_version}")

        bash_command = f"""
        for f in {svn_dev_repo}/task-sdk/{task_sdk_rc}/*; do
            svn cp "$f" "$(basename "$f")/"
        done
        """

        run_command(
            [
                "bash",
                "-c",
                bash_command,
            ],
            check=True,
        )
        console_print("Task SDK artifacts copied to SVN:")
        run_command(["ls"])

        # Go back to previous directory
        os.chdir(current_dir)


def commit_release(version, task_sdk_version, rc, task_sdk_rc, svn_release_repo):
    commit_message = f"Release Airflow {version} from {rc}"
    if task_sdk_version and task_sdk_rc:
        commit_message += f" & Task SDK {task_sdk_version} from {task_sdk_rc}"

    if confirm_action("Commit release to SVN?"):
        # Need to commit from parent directory to include both airflow and task-sdk if applicable
        current_dir = os.getcwd()
        os.chdir(svn_release_repo)
        run_command(
            ["svn", "commit", "-m", commit_message],
            check=True,
        )
        os.chdir(current_dir)


def remove_old_release(version, task_sdk_version, svn_release_repo):
    """
    Remove all old Airflow and Task SDK releases from SVN except the current versions.

    :param version: Current Airflow release version to keep
    :param task_sdk_version: Current Task SDK release version to keep (if any)
    :param svn_release_repo: Path to the SVN release repository
    """
    if not confirm_action("Do you want to look for old releases to remove?"):
        return

    # Save current directory
    current_dir = os.getcwd()
    os.chdir(svn_release_repo)

    # Initialize lists for old releases
    old_airflow_releases = []
    old_task_sdk_releases = []

    # Remove old Airflow releases
    for entry in os.scandir():
        if entry.name == version:
            # Don't remove the current release
            continue
        if entry.is_dir() and RELEASE_PATTERN.match(entry.name):
            old_airflow_releases.append(entry.name)
    old_airflow_releases.sort()

    if old_airflow_releases:
        console_print(f"The following old Airflow releases should be removed: {old_airflow_releases}")
        for old_release in old_airflow_releases:
            console_print(f"Removing old release {old_release}")
            if confirm_action(f"Remove old release {old_release}?"):
                run_command(["svn", "rm", old_release], check=True)
                run_command(
                    ["svn", "commit", "-m", f"Remove old release: {old_release}"],
                    check=True,
                )

    # Remove old Task SDK releases
    if task_sdk_version:
        task_sdk_dir = os.path.join(svn_release_repo, "task-sdk")
        if os.path.exists(task_sdk_dir):
            for entry in os.scandir(task_sdk_dir):
                if entry.name == task_sdk_version:
                    # Don't remove the current Task SDK release
                    continue
                if entry.is_dir() and RELEASE_PATTERN.match(entry.name):
                    old_task_sdk_releases.append(f"task-sdk/{entry.name}")
            old_task_sdk_releases.sort()

            if old_task_sdk_releases:
                console_print(
                    f"The following old Task SDK releases should be removed: {old_task_sdk_releases}"
                )
                for old_release in old_task_sdk_releases:
                    console_print(f"Removing old release {old_release}")
                    if confirm_action(f"Remove old release {old_release}?"):
                        run_command(["svn", "rm", old_release], check=True)
                        run_command(
                            ["svn", "commit", "-m", f"Remove old release: {old_release}"],
                            check=True,
                        )

    if not old_airflow_releases and not old_task_sdk_releases:
        console_print("No old releases to remove.")
    if old_airflow_releases or old_task_sdk_releases:
        console_print("[success]Old releases removed")
    os.chdir(current_dir)


def verify_pypi_package(version):
    if confirm_action("Verify PyPI package?"):
        run_command(["twine", "check", "*.whl", f"*{version}.tar.gz"], check=True)


def upload_to_pypi(version, task_sdk_version=None):
    if confirm_action("Upload Airflow packages to PyPI?"):
        run_command(
            [
                "twine",
                "upload",
                "-r",
                "pypi",
                "apache_airflow-*.whl",
                f"apache_airflow-{version}.tar.gz",
                f"apache_airflow_core-{version}.tar.gz",
                "apache_airflow_core-*.whl",
            ],
            check=True,
        )
        console_print("Airflow packages pushed to production PyPI")
        console_print(
            "Verify that the package looks good by downloading it and installing it into a virtual "
            "environment. The package download link is available at: "
            "https://pypi.python.org/pypi/apache-airflow"
        )

    if task_sdk_version and confirm_action("Upload Task SDK packages to PyPI?"):
        os.chdir(f"../task-sdk/{task_sdk_version}")
        run_command(
            [
                "twine",
                "upload",
                "-r",
                "pypi",
                "apache_airflow_task_sdk-*.whl",
                f"apache_airflow_task_sdk-{task_sdk_version}.tar.gz",
            ],
            check=True,
        )
        console_print("Task SDK packages pushed to production PyPI")
        console_print(
            "Verify that the Task SDK package is available at: "
            "https://pypi.python.org/pypi/apache-airflow-task-sdk"
        )


def retag_constraints(release_candidate, version):
    if confirm_action(f"Retag constraints for {release_candidate} as {version}?"):
        run_command(
            ["git", "checkout", f"constraints-{release_candidate}"],
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
    if confirm_action(f"Push constraints-{version} tag to GitHub?"):
        run_command(
            ["git", "push", "origin", "tag", f"constraints-{version}"],
            check=True,
        )


def tag_and_push_latest_constraint(version):
    console_print("In case you release 'latest stable' version, also update 'latest' constraints")
    if confirm_action("Tag latest constraint?"):
        run_command(
            [
                "git",
                "tag",
                "-f",
                "-s",
                "constraints-latest",
                "-m",
                f"Latest constraints set to Apache Airflow {version}",
            ],
            check=True,
        )
    if confirm_action("Push latest constraints tag to GitHub?"):
        run_command(
            ["git", "push", "origin", "tag", "-f", "constraints-latest"],
            check=True,
        )


def push_tag_for_final_version(version, release_candidate, task_sdk_version=None, task_sdk_rc=None):
    if confirm_action(f"Push Airflow tag for final version {version}?"):
        console_print(
            """
        This step should only be done now and not before, because it triggers an automated
        build of the production docker image, using the packages that are currently released
        in PyPI (both airflow and latest provider distributions).
        """
        )
        confirm_action(f"Confirm that {version} is pushed to PyPI(not PyPI test). Is it pushed?", abort=True)

        run_command(["git", "checkout", f"{release_candidate}"], check=True)
        run_command(
            ["git", "tag", "-s", f"{version}", "-m", f"Apache Airflow {version}"],
            check=True,
        )
        run_command(["git", "push", "origin", "tag", f"{version}"], check=True)

    if (
        task_sdk_version
        and task_sdk_rc
        and confirm_action(f"Push Task SDK tag for final version {task_sdk_version}?")
    ):
        confirm_action(
            f"Confirm that Task SDK {task_sdk_version} is pushed to PyPI. Is it pushed?", abort=True
        )
        run_command(["git", "checkout", f"task-sdk/{task_sdk_rc}"], check=True)
        run_command(
            [
                "git",
                "tag",
                "-s",
                f"task-sdk/{task_sdk_version}",
                "-m",
                f"Airflow Task SDK {task_sdk_version}",
            ],
            check=True,
        )
        run_command(["git", "push", "origin", "tag", f"task-sdk/{task_sdk_version}"], check=True)


@release_management_group.command(
    name="start-release",
    short_help="Start Airflow release process",
    help="Start the process of releasing an Airflow version. "
    "This command will guide you through the release process. "
    "The latest release candidate for the given version will be automatically found from SVN dev directory.",
)
@click.option("--version", required=True, help="Airflow release version e.g. 3.0.5")
@click.option("--task-sdk-version", required=False, help="Task SDK release version e.g. 1.0.5")
@option_answer
@option_dry_run
@option_verbose
def airflow_release(version, task_sdk_version):
    if "rc" in version:
        exit("Version must not contain 'rc' - use the final version (e.g., 3.0.5)")

    os.chdir(AIRFLOW_ROOT_PATH)
    airflow_repo_root = os.getcwd()
    console_print()
    console_print("Airflow Release Version:", version)
    if task_sdk_version:
        console_print("Task SDK Release Version:", task_sdk_version)
    console_print("Airflow repo root:", airflow_repo_root)
    console_print()
    console_print("Below are your git remotes. We will push to origin:")
    run_command(["git", "remote", "-v"], check=True)
    console_print()
    confirm_action("Verify that the above information is correct. Do you want to continue?", abort=True)
    # Final confirmation
    confirm_action("Pushes will be made to origin. Do you want to continue?", abort=True)

    # Clone the asf repo
    os.chdir("..")
    working_dir = os.getcwd()
    svn_dev_repo = f"{working_dir}/asf-dist/dev/airflow"
    svn_release_repo = f"{working_dir}/asf-dist/release/airflow"

    if get_dry_run():
        # Skip SVN clone in dry-run mode - use placeholder RCs for testing the workflow
        console_print("[info]Skipping SVN operations in dry-run mode")
        release_candidate = f"{version}rc1"
        task_sdk_release_candidate = f"{task_sdk_version}rc1" if task_sdk_version else None
    else:
        clone_asf_repo(working_dir)
        console_print("SVN dev repo root:", svn_dev_repo)
        console_print("SVN release repo root:", svn_release_repo)

        console_print()
        console_print("Finding latest release candidate from SVN dev directory...")
        release_candidate = find_latest_release_candidate(version, svn_dev_repo, component="airflow")
        if not release_candidate:
            exit(f"No release candidate found for version {version} in SVN dev directory")

        task_sdk_release_candidate = None
        if task_sdk_version:
            task_sdk_release_candidate = find_latest_release_candidate(
                task_sdk_version, svn_dev_repo, component="task-sdk"
            )
            if not task_sdk_release_candidate:
                exit(
                    f"No Task SDK release candidate found for version {task_sdk_version} in SVN dev directory"
                )

    console_print()
    console_print("Airflow Release candidate:", release_candidate)
    console_print("Airflow Release Version:", version)
    if task_sdk_release_candidate:
        console_print("Task SDK Release candidate:", task_sdk_release_candidate)
        console_print("Task SDK Release Version:", task_sdk_version)
    console_print()

    # Create the version directory
    confirm_action("Confirm that the above repo exists. Continue?", abort=True)

    # Change to the svn release repo
    os.chdir(svn_release_repo)

    # Create the version directory
    create_version_dir(version, task_sdk_version)
    svn_release_version_dir = f"{svn_release_repo}/{version}"
    svn_release_task_sdk_version_dir = f"{svn_release_repo}/task-sdk/{task_sdk_version}"
    console_print("SVN Release version dir:", svn_release_version_dir)

    # Change directory to the version directory
    if os.path.exists(svn_release_version_dir):
        os.chdir(svn_release_version_dir)
    else:
        confirm_action("Version directory does not exist. Do you want to Continue?", abort=True)

    # Copy artifacts to the version directory
    copy_artifacts_to_svn(release_candidate, task_sdk_release_candidate, svn_dev_repo, svn_release_repo)

    # Commit the release to svn
    commit_release(version, task_sdk_version, release_candidate, task_sdk_release_candidate, svn_release_repo)

    confirm_action(
        "Verify that the artifacts appear in https://dist.apache.org/repos/dist/release/airflow/", abort=True
    )

    # Remove old releases
    if os.path.exists(svn_release_version_dir):
        os.chdir("..")
    remove_old_release(version, task_sdk_version, svn_release_repo)
    confirm_action(
        "Verify that the packages appear in "
        "[airflow](https://dist.apache.org/repos/dist/release/airflow/)"
        "and [airflow](https://dist.apache.org/repos/dist/release/airflow/task-sdk/). Continue?",
        abort=True,
    )

    # Verify pypi package
    if os.path.exists(svn_release_version_dir):
        os.chdir(svn_release_version_dir)
    verify_pypi_package(version)
    if os.path.exists(svn_release_task_sdk_version_dir):
        os.chdir(svn_release_task_sdk_version_dir)
        console_print("Task SDK release dir:", svn_release_task_sdk_version_dir)
        verify_pypi_package(task_sdk_version)
        os.chdir(svn_release_version_dir)

    # Upload to pypi
    upload_to_pypi(version, task_sdk_version)

    # Change Directory to airflow
    os.chdir(airflow_repo_root)

    # Retag and push the constraint file
    retag_constraints(release_candidate, version)
    tag_and_push_latest_constraint(version)

    # Push tag for final version
    push_tag_for_final_version(version, release_candidate, task_sdk_version, task_sdk_release_candidate)

    console_print("Done!")
