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

import click

from airflow_breeze.commands.release_management_group import release_management
from airflow_breeze.utils.common_options import option_answer
from airflow_breeze.utils.confirm import confirm_action
from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT
from airflow_breeze.utils.run_utils import run_command

CI = os.environ.get("CI")
DRY_RUN = True if CI else False


def clone_asf_repo(working_dir):
    if confirm_action("Clone ASF repo?"):
        run_command(["rm", "-rf", f"{working_dir}/asf-dist"], check=True)
        run_command(
            ["svn", "checkout", "--depth=immediates", "https://dist.apache.org/repos/dist", "asf-dist"],
            check=True,
        )
        dev_dir = f"{working_dir}/asf-dist/dev/airflow"
        release_dir = f"{working_dir}/asf-dist/release/airflow"
        run_command(["svn", "update", "--set-depth", "infinity", dev_dir], check=True)
        run_command(["svn", "update", "--set-depth", "infinity", release_dir], check=True)


def create_version_dir(version):
    if confirm_action(f"Create SVN version directory for {version}?"):
        run_command(["svn", "mkdir", f"{version}"], dry_run_override=DRY_RUN, check=True)
        console_print(f"{version} directory created")


def copy_artifacts_to_svn(rc, svn_dev_repo):
    if confirm_action(f"Copy artifacts to SVN for {rc}?"):
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
            dry_run_override=DRY_RUN,
            check=True,
        )
        console_print("Artifacts copied to SVN:")
        run_command(["ls"], dry_run_override=DRY_RUN)


def commit_release(version, rc, svn_release_version_dir):
    if confirm_action(f"Commit release {version} to SVN?"):
        run_command(
            ["svn", "commit", "-m", f"Release Airflow {version} from {rc}"],
            dry_run_override=DRY_RUN,
            check=True,
        )


def remove_old_release(previous_release):
    if confirm_action(f"Remove old release {previous_release}?"):
        run_command(["svn", "rm", f"{previous_release}"], dry_run_override=DRY_RUN, check=True)
        run_command(
            ["svn", "commit", "-m", f"Remove old release: {previous_release}"],
            dry_run_override=DRY_RUN,
            check=True,
        )
        confirm_action(
            "Verify that the packages appear in "
            "[airflow](https://dist.apache.org/repos/dist/release/airflow/). Continue?",
            abort=True,
        )


def verify_pypi_package(version):
    if confirm_action("Verify PyPI package?"):
        run_command(["twine", "check", "*.whl", f"*{version}.tar.gz"], check=True)


def upload_to_pypi(version):
    if confirm_action("Upload to PyPI?"):
        run_command(
            ["twine", "upload", "-r", "pypi", "*.whl", f"*{version}.tar.gz"],
            dry_run_override=DRY_RUN,
            check=True,
        )
        console_print("Packages pushed to production PyPI")
        console_print(
            "Verify that the package looks good by downloading it and installing it into a virtual "
            "environment. The package download link is available at: "
            "https://pypi.python.org/pypi/apache-airflow"
        )


def retag_constraints(release_candidate, version):
    if confirm_action(f"Retag constraints for {release_candidate} as {version}?"):
        run_command(
            ["git", "checkout", f"constraints-{release_candidate}"], dry_run_override=DRY_RUN, check=True
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
            dry_run_override=DRY_RUN,
            check=True,
        )
    if confirm_action(f"Push contraints-{version} tag to GitHub?"):
        run_command(
            ["git", "push", "origin", "tag", f"constraints-{version}"], dry_run_override=DRY_RUN, check=True
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
            dry_run_override=DRY_RUN,
            check=True,
        )
    if confirm_action("Push latest constraints tag to GitHub?"):
        run_command(
            ["git", "push", "origin", "tag", "-f", "constraints-latest"],
            dry_run_override=DRY_RUN,
            check=True,
        )


def push_tag_for_final_version(version, release_candidate):
    if confirm_action(f"Push tag for final version {version}?"):
        console_print(
            """
        This step should only be done now and not before, because it triggers an automated
        build of the production docker image, using the packages that are currently released
        in PyPI (both airflow and latest provider packages).
        """
        )
        confirm_action(f"Confirm that {version} is pushed to PyPI(not PyPI test). Is it pushed?", abort=True)

        run_command(["git", "checkout", f"{release_candidate}"], dry_run_override=DRY_RUN, check=True)
        run_command(
            ["git", "tag", "-s", f"{version}", "-m", f"Apache Airflow {version}"],
            dry_run_override=DRY_RUN,
            check=True,
        )
        run_command(["git", "push", "origin", "tag", f"{version}"], dry_run_override=DRY_RUN, check=True)


@release_management.command(
    name="start-release",
    short_help="Start Airflow release process",
    help="Start the process of releasing an Airflow version. "
    "This command will guide you through the release process. ",
)
@click.option("--release-candidate", required=True)
@click.option("--previous-release", required=True)
@option_answer
def airflow_release(release_candidate, previous_release):
    if "rc" not in release_candidate:
        exit("Release candidate must contain 'rc'")
    if "rc" in previous_release:
        exit("Previous release must not contain 'rc'")
    version = release_candidate[:-3]
    os.chdir(AIRFLOW_SOURCES_ROOT)
    airflow_repo_root = os.getcwd()
    console_print()
    console_print("Release candidate:", release_candidate)
    console_print("Release Version:", version)
    console_print("Previous release:", previous_release)
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
    clone_asf_repo(working_dir)
    svn_dev_repo = f"{working_dir}/asf-dist/dev/airflow"
    svn_release_repo = f"{working_dir}/asf-dist/release/airflow"
    console_print("SVN dev repo root:", svn_dev_repo)
    console_print("SVN release repo root:", svn_release_repo)

    # Create the version directory
    confirm_action("Confirm that the above repo exists. Continue?", abort=True)

    # Change to the svn release repo
    os.chdir(svn_release_repo)

    # Create the version directory
    create_version_dir(version)
    svn_release_version_dir = f"{svn_release_repo}/{version}"
    console_print("SVN Release version dir:", svn_release_version_dir)

    # Change directory to the version directory
    if os.path.exists(svn_release_version_dir):
        os.chdir(svn_release_version_dir)
    else:
        confirm_action("Version directory does not exist. Do you want to Continue?", abort=True)

    # Copy artifacts to the version directory
    copy_artifacts_to_svn(release_candidate, svn_dev_repo)

    # Commit the release to svn
    commit_release(version, release_candidate, svn_release_version_dir)

    confirm_action(
        "Verify that the artifacts appear in https://dist.apache.org/repos/dist/release/airflow/", abort=True
    )

    # Remove old release
    if os.path.exists(svn_release_version_dir):
        os.chdir("..")
    remove_old_release(previous_release)

    # Verify pypi package
    if os.path.exists(svn_release_version_dir):
        os.chdir(svn_release_version_dir)
    verify_pypi_package(version)

    # Upload to pypi
    upload_to_pypi(version)

    # Change Directory to airflow
    os.chdir(airflow_repo_root)

    # Retag and push the constraint file
    retag_constraints(release_candidate, version)
    tag_and_push_latest_constraint(version)

    # Push tag for final version
    push_tag_for_final_version(version, release_candidate)

    console_print("Done!")
