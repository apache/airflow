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
from pathlib import Path

import click

from airflow_breeze.commands.common_options import option_answer
from airflow_breeze.commands.release_management_group import release_management
from airflow_breeze.utils.confirm import confirm_action
from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT, DIST_DIR, OUT_DIR
from airflow_breeze.utils.python_versions import check_python_version
from airflow_breeze.utils.reproducible import (
    get_source_date_epoch,
    repack_deterministically,
)
from airflow_breeze.utils.run_utils import run_command

CI = os.environ.get("CI")
RUNNING_IN_CI = True if CI else False


def merge_pr(version_branch):
    if confirm_action("Do you want to merge the Sync PR?"):
        run_command(
            [
                "git",
                "checkout",
                f"v{version_branch}-stable",
            ],
            dry_run_override=RUNNING_IN_CI,
            check=True,
        )
        run_command(
            ["git", "reset", "--hard", f"origin/v{version_branch}-stable"],
            dry_run_override=RUNNING_IN_CI,
            check=True,
        )
        run_command(
            ["git", "merge", "--ff-only", f"v{version_branch}-test"],
            dry_run_override=RUNNING_IN_CI,
            check=True,
        )
        if confirm_action(
            "Do you want to push the changes? Pushing the changes closes the PR"
        ):
            run_command(
                ["git", "push", "origin", f"v{version_branch}-stable"],
                dry_run_override=RUNNING_IN_CI,
                check=True,
            )


def git_tag(version):
    if confirm_action(f"Tag {version}?"):
        if RUNNING_IN_CI:
            # override tags when running in CI
            run_command(
                ["git", "tag", "-f", f"{version}", "-m", f"Apache Airflow {version}"],
                check=True,
            )
        else:
            run_command(
                ["git", "tag", "-s", f"{version}", "-m", f"Apache Airflow {version}"],
                check=True,
            )
        console_print("[success]Tagged")


def git_clean():
    if confirm_action("Clean git repo?"):
        run_command(
            ["breeze", "ci", "fix-ownership"], dry_run_override=RUNNING_IN_CI, check=True
        )
        run_command(["git", "clean", "-fxd"], dry_run_override=RUNNING_IN_CI, check=True)
        console_print("[success]Git repo cleaned")


def tarball_release(version: str, version_without_rc: str, source_date_epoch: int):
    console_print(f"[info]Creating tarball for Airflow {version}")
    shutil.rmtree(OUT_DIR, ignore_errors=True)
    DIST_DIR.mkdir(exist_ok=True)
    OUT_DIR.mkdir(exist_ok=True)
    archive_name = f"apache-airflow-{version_without_rc}-source.tar.gz"
    temporary_archive = OUT_DIR / archive_name
    result = run_command(
        [
            "git",
            "-c",
            "tar.umask=0077",
            "archive",
            "--format=tar.gz",
            f"{version}",
            f"--prefix=apache-airflow-{version_without_rc}/",
            "-o",
            temporary_archive.as_posix(),
        ],
        check=False,
    )
    if result.returncode != 0:
        console_print(
            f"[error]Failed to create tarball {temporary_archive} for Airflow {version}"
        )
        exit(result.returncode)
    final_archive = DIST_DIR / archive_name
    result = repack_deterministically(
        source_archive=temporary_archive,
        dest_archive=final_archive,
        prepend_path=None,
        timestamp=source_date_epoch,
    )
    if result.returncode != 0:
        console_print(
            f"[error]Failed to create tarball {temporary_archive} for Airflow {version}"
        )
        exit(result.returncode)
    console_print(f"[success]Tarball created in {final_archive}")


def create_artifacts_with_hatch(source_date_epoch: int):
    console_print("[info]Creating artifacts with hatch")
    shutil.rmtree(DIST_DIR, ignore_errors=True)
    DIST_DIR.mkdir(exist_ok=True)
    env_copy = os.environ.copy()
    env_copy["SOURCE_DATE_EPOCH"] = str(source_date_epoch)
    run_command(
        ["hatch", "build", "-c", "-t", "custom", "-t", "sdist", "-t", "wheel"],
        check=True,
        env=env_copy,
    )
    console_print("[success]Successfully prepared Airflow packages:")
    for file in sorted(DIST_DIR.glob("apache_airflow*")):
        console_print(print(file.name))
    console_print()


def create_artifacts_with_docker():
    console_print("[info]Creating artifacts with docker")
    run_command(
        [
            "breeze",
            "release-management",
            "prepare-airflow-package",
            "--package-format",
            "both",
        ],
        check=True,
    )
    console_print("[success]Artifacts created")


def sign_the_release(repo_root):
    if confirm_action("Do you want to sign the release?"):
        os.chdir(f"{repo_root}/dist")
        run_command(
            "./../dev/sign.sh *", dry_run_override=RUNNING_IN_CI, check=True, shell=True
        )
        console_print("[success]Release signed")


def tag_and_push_constraints(version, version_branch):
    if confirm_action("Do you want to tag and push constraints?"):
        run_command(
            ["git", "checkout", f"origin/constraints-{version_branch}"],
            dry_run_override=RUNNING_IN_CI,
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
            dry_run_override=RUNNING_IN_CI,
            check=True,
        )
        run_command(
            ["git", "push", "origin", "tag", f"constraints-{version}"],
            dry_run_override=RUNNING_IN_CI,
            check=True,
        )
        console_print("[success]Constraints tagged and pushed")


def clone_asf_repo(version, repo_root):
    if confirm_action("Do you want to clone asf repo?"):
        os.chdir(repo_root)
        run_command(
            [
                "svn",
                "checkout",
                "--depth=immediates",
                "https://dist.apache.org/repos/dist",
                "asf-dist",
            ],
            check=True,
        )
        run_command(
            ["svn", "update", "--set-depth=infinity", "asf-dist/dev/airflow"], check=True
        )
        console_print("[success]Cloned ASF repo successfully")


def move_artifacts_to_svn(version, repo_root):
    if confirm_action("Do you want to move artifacts to SVN?"):
        os.chdir(f"{repo_root}/asf-dist/dev/airflow")
        run_command(
            ["svn", "mkdir", f"{version}"], dry_run_override=RUNNING_IN_CI, check=True
        )
        run_command(
            f"mv {repo_root}/dist/* {version}/",
            dry_run_override=RUNNING_IN_CI,
            check=True,
            shell=True,
        )
        console_print("[success]Moved artifacts to SVN:")
        run_command(["ls"], dry_run_override=RUNNING_IN_CI)


def push_artifacts_to_asf_repo(version, repo_root):
    if confirm_action("Do you want to push artifacts to ASF repo?"):
        console_print("Files to push to svn:")
        if not RUNNING_IN_CI:
            os.chdir(f"{repo_root}/asf-dist/dev/airflow/{version}")
        run_command(["ls"], dry_run_override=RUNNING_IN_CI)
        confirm_action("Do you want to continue?", abort=True)
        run_command("svn add *", dry_run_override=RUNNING_IN_CI, check=True, shell=True)
        run_command(
            ["svn", "commit", "-m", f"Add artifacts for Airflow {version}"],
            dry_run_override=RUNNING_IN_CI,
            check=True,
        )
        console_print("[success]Files pushed to svn")
        console_print(
            "Verify that the files are available here: https://dist.apache.org/repos/dist/dev/airflow/"
        )


def delete_asf_repo(repo_root):
    os.chdir(repo_root)
    if confirm_action("Do you want to remove the cloned asf repo?"):
        run_command(["rm", "-rf", "asf-dist"], dry_run_override=RUNNING_IN_CI, check=True)


def prepare_pypi_packages(version, version_suffix, repo_root):
    if confirm_action("Prepare pypi packages?"):
        console_print("[info]Preparing PyPI packages")
        os.chdir(repo_root)
        run_command(
            ["git", "checkout", f"{version}"], dry_run_override=RUNNING_IN_CI, check=True
        )
        run_command(
            [
                "breeze",
                "release-management",
                "prepare-airflow-package",
                "--version-suffix-for-pypi",
                f"{version_suffix}",
                "--package-format",
                "both",
            ],
            check=True,
        )
        files_to_check = []
        for files in Path(DIST_DIR).glob("apache_airflow*"):
            if "-sources" not in files.name:
                files_to_check.append(files.as_posix())
        run_command(["twine", "check", *files_to_check], check=True)
        console_print("[success]PyPI packages prepared")


def push_packages_to_pypi(version):
    if confirm_action("Do you want to push packages to production PyPI?"):
        run_command(
            ["twine", "upload", "-r", "pypi", "dist/*"],
            dry_run_override=RUNNING_IN_CI,
            check=True,
        )
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


def push_release_candidate_tag_to_github(version):
    if confirm_action("Do you want to push release candidate tag to GitHub?"):
        console_print(
            """
        This step should only be done now and not before, because it triggers an automated
        build of the production docker image, using the packages that are currently released
        in PyPI (both airflow and latest provider packages).
        """
        )
        confirm_action(
            f"Confirm that {version} is pushed to PyPI(not PyPI test). Is it pushed?",
            abort=True,
        )
        run_command(
            ["git", "push", "origin", "tag", f"{version}"],
            dry_run_override=RUNNING_IN_CI,
            check=True,
        )
        console_print("[success]Release candidate tag pushed to GitHub")


def remove_old_releases(version, repo_root):
    if confirm_action(
        "In beta release we do not remove old RCs. Is this a beta release?"
    ):
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
            run_command(
                ["svn", "rm", old_release], dry_run_override=RUNNING_IN_CI, check=True
            )
            run_command(
                ["svn", "commit", "-m", f"Remove old release: {old_release}"],
                dry_run_override=RUNNING_IN_CI,
                check=True,
            )
    console_print("[success]Old releases removed")
    os.chdir(repo_root)


@release_management.command(
    name="prepare-airflow-tarball",
    help="Prepare airflow's source tarball.",
)
@click.option(
    "--version",
    required=True,
    help="The release candidate version e.g. 2.4.3rc1",
    envvar="VERSION",
)
def prepare_airflow_tarball(version: str):
    check_python_version()
    from packaging.version import Version

    airflow_version = Version(version)
    if not airflow_version.is_prerelease:
        exit("--version value must be a pre-release")
    source_date_epoch = get_source_date_epoch(AIRFLOW_SOURCES_ROOT / "airflow")
    version_without_rc = airflow_version.base_version
    # Create the tarball
    tarball_release(
        version=version,
        version_without_rc=version_without_rc,
        source_date_epoch=source_date_epoch,
    )


@release_management.command(
    name="start-rc-process",
    short_help="Start RC process",
    help="Start the process for releasing a new RC.",
)
@click.option(
    "--version", required=True, help="The release candidate version e.g. 2.4.3rc1"
)
@click.option(
    "--previous-version", required=True, help="Previous version released e.g. 2.4.2"
)
@click.option(
    "--github-token",
    help="GitHub token to use in generating issue for testing of release candidate",
)
@option_answer
def publish_release_candidate(version, previous_version, github_token):
    check_python_version()
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
    version_branch = (
        str(airflow_version.release[0]) + "-" + str(airflow_version.release[1])
    )
    version_without_rc = airflow_version.base_version
    os.chdir(AIRFLOW_SOURCES_ROOT)
    airflow_repo_root = os.getcwd()

    # List the above variables and ask for confirmation
    console_print()
    console_print(f"Previous version: {previous_version}")
    console_print(f"version: {version}")
    console_print(f"version_suffix: {version_suffix}")
    console_print(f"version_branch: {version_branch}")
    console_print(f"version_without_rc: {version_without_rc}")
    console_print(f"airflow_repo_root: {airflow_repo_root}")
    console_print()
    console_print("Below are your git remotes. We will push to origin:")
    run_command(["git", "remote", "-v"], dry_run_override=RUNNING_IN_CI)
    console_print()
    confirm_action(
        "Verify that the above information is correct. Do you want to continue?",
        abort=True,
    )
    # Final confirmation
    confirm_action("Pushes will be made to origin. Do you want to continue?", abort=True)
    # Merge the sync PR
    merge_pr(version_branch)
    #
    # # Tag & clean the repo
    git_tag(version)
    git_clean()
    source_date_epoch = get_source_date_epoch(AIRFLOW_SOURCES_ROOT / "airflow")
    shutil.rmtree(DIST_DIR, ignore_errors=True)
    # Create the artifacts
    if confirm_action("Use docker to create artifacts?"):
        create_artifacts_with_docker()
    elif confirm_action("Use hatch to create artifacts?"):
        create_artifacts_with_hatch(source_date_epoch)
    if confirm_action("Create tarball?"):
        # Create the tarball
        tarball_release(
            version=version,
            version_without_rc=version_without_rc,
            source_date_epoch=source_date_epoch,
        )
    # Sign the release
    sign_the_release(airflow_repo_root)
    # Tag and push constraints
    tag_and_push_constraints(version, version_branch)
    # Clone the asf repo
    clone_asf_repo(version, airflow_repo_root)
    # Move artifacts to SVN
    move_artifacts_to_svn(version, airflow_repo_root)
    # Push the artifacts to the asf repo
    push_artifacts_to_asf_repo(version, airflow_repo_root)

    # Remove old releases
    remove_old_releases(version, airflow_repo_root)

    # Delete asf-dist directory
    delete_asf_repo(airflow_repo_root)

    # Prepare the pypi packages
    prepare_pypi_packages(version, version_suffix, airflow_repo_root)

    # Push the packages to pypi
    push_packages_to_pypi(version)

    # Push the release candidate tag to gitHub
    push_release_candidate_tag_to_github(version)
    # Create issue for testing
    os.chdir(airflow_repo_root)

    console_print()
    console_print("Done!")
