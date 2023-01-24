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

from airflow_breeze.utils.common_options import option_answer
from airflow_breeze.utils.confirm import confirm_action
from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT
from airflow_breeze.utils.run_utils import run_command

CI = os.environ.get("CI")
DRY_RUN = True if CI else False


def merge_pr(version_branch):
    if confirm_action("Do you want to merge the Sync PR?"):
        run_command(
            [
                "git",
                "checkout",
                f"v{version_branch}-stable",
            ],
            dry_run_override=DRY_RUN,
            check=True,
        )
        run_command(
            ["git", "reset", "--hard", f"origin/v{version_branch}-stable"],
            dry_run_override=DRY_RUN,
            check=True,
        )
        run_command(
            ["git", "merge", "--ff-only", f"v{version_branch}-test"], dry_run_override=DRY_RUN, check=True
        )
        if confirm_action("Do you want to push the changes? Pushing the changes closes the PR"):
            run_command(
                ["git", "push", "origin", f"v{version_branch}-stable"], dry_run_override=DRY_RUN, check=True
            )


def git_tag(version):
    if confirm_action(f"Tag {version}?"):
        run_command(["git", "tag", "-s", f"{version}", "-m", f"Apache Airflow {version}"], check=True)
        console_print("Tagged")


def git_clean():
    if confirm_action("Clean git repo?"):
        run_command(["git", "clean", "-fxd"], dry_run_override=DRY_RUN, check=True)
        console_print("Git repo cleaned")


def tarball_release(version, version_without_rc):
    if confirm_action("Create tarball?"):
        run_command(["rm", "-rf", "dist"], check=True)

        run_command(["mkdir", "dist"], check=True)
        run_command(
            [
                "git",
                "archive",
                "--format=tar.gz",
                f"{version}",
                f"--prefix=apache-airflow-{version_without_rc}/",
                "-o",
                f"dist/apache-airflow-{version_without_rc}-source.tar.gz",
            ],
            check=True,
        )
        console_print("Tarball created")


def create_artifacts_with_sdist():
    run_command(["python3", "setup.py", "compile_assets", "sdist", "bdist_wheel"], check=True)
    console_print("Artifacts created")


def create_artifacts_with_breeze():
    run_command(
        ["breeze", "release-management", "prepare-airflow-package", "--package-format", "both"], check=True
    )
    console_print("Artifacts created")


def sign_the_release(repo_root):
    if confirm_action("Do you want to sign the release?"):
        os.chdir(repo_root)
        run_command(["pushd", "dist"], dry_run_override=DRY_RUN, check=True)
        run_command(["./dev/sign.sh", "*"], dry_run_override=DRY_RUN, check=True)
        run_command(["popd"], dry_run_override=DRY_RUN, check=True)
        console_print("Release signed")


def tag_and_push_constraints(version, version_branch):
    if confirm_action("Do you want to tag and push constraints?"):
        run_command(
            ["git", "checkout", f"origin/constraints-{version_branch}"], dry_run_override=DRY_RUN, check=True
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
        run_command(
            ["git", "push", "origin", "tag", f"constraints-{version}"], dry_run_override=DRY_RUN, check=True
        )
        console_print("Constraints tagged and pushed")


def clone_asf_repo(version, repo_root):
    if confirm_action("Do you want to clone asf repo?"):
        os.chdir(repo_root)
        run_command(
            ["svn", "checkout", "--depth=immediates", "https://dist.apache.org/repos/dist", "asf-dist"],
            check=True,
        )
        run_command(["svn", "update", "--set-depth=infinity", "asf-dist/dev/airflow"], check=True)
        console_print("Cloned ASF repo successfully")


def move_artifacts_to_svn(version, repo_root):
    if confirm_action("Do you want to move artifacts to SVN?"):
        os.chdir(f"{repo_root}/asf-dist/dev/airflow")
        run_command(["svn", "mkdir", f"{version}"], dry_run_override=DRY_RUN, check=True)
        run_command(["mv", f"{repo_root}/dist/*", f"{version}/"], dry_run_override=DRY_RUN, check=True)
        console_print("Moved artifacts to SVN:")
        run_command(["ls"], dry_run_override=DRY_RUN)


def push_artifacts_to_asf_repo(version, repo_root):
    if confirm_action("Do you want to push artifacts to ASF repo?"):
        console_print("Files to push to svn:")
        if not DRY_RUN:
            os.chdir(f"{repo_root}/asf-dist/dev/airflow/{version}")
        run_command(["ls"], dry_run_override=DRY_RUN)
        confirm_action("Do you want to continue?", abort=True)
        run_command(["svn", "add", "*"], dry_run_override=DRY_RUN, check=True)
        run_command(
            ["svn", "commit", "-m", f"Add artifacts for Airflow {version}"],
            dry_run_override=DRY_RUN,
            check=True,
        )
        console_print("Files pushed to svn")
        os.chdir(repo_root)
        run_command(["rm", "-rf", "asf-dist"], dry_run_override=DRY_RUN, check=True)


def prepare_pypi_packages(version, version_suffix, repo_root):
    if confirm_action("Prepare pypi packages?"):
        console_print("Preparing PyPI packages")
        os.chdir(repo_root)
        run_command(["git", "checkout", f"{version}"], dry_run_override=DRY_RUN, check=True)
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
        run_command(["twine", "check", "dist/*"], check=True)
        console_print("PyPI packages prepared")


def push_packages_to_test_pypi():
    if confirm_action("Do you want to push packages to test PyPI?"):
        run_command(["twine", "upload", "-r", "pypitest", "dist/*"], dry_run_override=DRY_RUN, check=True)
        console_print("Packages pushed to test PyPI")
        console_print(
            "Verify that the test package looks good by downloading it and installing it into a virtual "
            "environment. The package download link is available at: "
            "https://test.pypi.org/project/apache-airflow/#files"
        )


def push_packages_to_pypi():
    if confirm_action("Do you want to push packages to production PyPI?"):
        confirm_action(
            "I have tested the package I uploaded to test PyPI. "
            "I installed and ran a DAG with it and there's no issue. Do you agree to the above?",
            abort=True,
        )
        run_command(["twine", "upload", "-r", "pypi", "dist/*"], dry_run_override=DRY_RUN, check=True)
        console_print("Packages pushed to production PyPI")
        console_print(
            "Again, confirm that the package is available here: https://pypi.python.org/pypi/apache-airflow"
        )
        console_print(
            """
            It is important to stress that this snapshot should not be named "release", and it
            is not supposed to be used by and advertised to the end-users who do not read the devlist.
            """
        )


def push_release_candidate_to_github(version):
    if confirm_action("Do you want to push release candidate to GitHub?"):
        console_print(
            """
        This step should only be done now and not before, because it triggers an automated
        build of the production docker image, using the packages that are currently released
        in PyPI (both airflow and latest provider packages).
        """
        )
        confirm_action(f"Confirm that {version} is pushed to PyPI(not PyPI test). Is it pushed?", abort=True)
        run_command(["git", "push", "origin", "tag", f"{version}"], dry_run_override=DRY_RUN, check=True)
        console_print("Release candidate pushed to GitHub")


def create_issue_for_testing(version, previous_version, github_token):
    if confirm_action("Do you want to create issue for testing? Only applicable for patch release"):
        console_print()
        console_print("Create issue in github for testing the release using this subject:")
        console_print()
        console_print(f"Status of testing of Apache Airflow {version}")
        console_print()
        if CI:
            run_command(["git", "fetch"], check=True)
        if confirm_action("Print the issue body?"):
            run_command(
                [
                    "./dev/prepare_release_issue.py",
                    "generate-issue-content",
                    "--previous-release",
                    f"{previous_version}",
                    "--current-release",
                    f"{version}",
                    "--github-token",
                    f"{github_token}",
                ],
                check=True,
            )


@click.command(
    name="start-rc-process",
    short_help="Start RC process",
    help="Start the process for releasing a new RC.",
    hidden=True,
)
@click.option("--version", required=True, help="The release candidate version e.g. 2.4.3rc1")
@click.option("--previous-version", required=True, help="Previous version released e.g. 2.4.2")
@click.option(
    "--github-token", help="GitHub token to use in generating issue for testing of release candidate"
)
@option_answer
def publish_release_candidate(version, previous_version, github_token):
    if "rc" not in version:
        exit("Version must contain 'rc'")
    if "rc" in previous_version:
        exit("Previous version must not contain 'rc'")
    if not github_token:
        github_token = os.environ.get("GITHUB_TOKEN")
        if not github_token:
            console_print("GITHUB_TOKEN is not set! Issue generation will fail.")
            confirm_action("Do you want to continue?", abort=True)
    version_suffix = version[-3:]
    version_branch = version[:3].replace(".", "-")
    version_without_rc = version[:-3]
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
    run_command(["git", "remote", "-v"], dry_run_override=DRY_RUN)
    console_print()
    confirm_action("Verify that the above information is correct. Do you want to continue?", abort=True)
    # Final confirmation
    confirm_action("Pushes will be made to origin. Do you want to continue?", abort=True)
    # Merge the sync PR
    merge_pr(version_branch)

    # Tag & clean the repo
    git_tag(version)
    git_clean()
    # Build the latest image
    if confirm_action("Build latest breeze image?"):
        run_command(["breeze", "ci-image", "build", "--python", "3.7"], dry_run_override=DRY_RUN, check=True)
    # Create the tarball
    tarball_release(version, version_without_rc)
    # Create the artifacts
    if confirm_action("Use breeze to create artifacts?"):
        create_artifacts_with_breeze()
    elif confirm_action("Use setup.py to create artifacts?"):
        create_artifacts_with_sdist()
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
    # Prepare the pypi packages
    prepare_pypi_packages(version, version_suffix, airflow_repo_root)
    # Push the packages to test pypi
    push_packages_to_test_pypi()

    # Push the packages to pypi
    push_packages_to_pypi()
    # Push the release candidate to gitHub

    push_release_candidate_to_github(version)
    # Create issue for testing
    os.chdir(airflow_repo_root)
    create_issue_for_testing(version, previous_version, github_token)
    console_print()
    console_print("Done!")
