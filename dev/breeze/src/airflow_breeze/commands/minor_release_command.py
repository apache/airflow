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
import sys

import click

from airflow_breeze.commands.common_options import option_answer
from airflow_breeze.commands.release_management_group import release_management
from airflow_breeze.utils.confirm import confirm_action
from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT
from airflow_breeze.utils.run_utils import run_command

CI = os.environ.get("CI")
DRY_RUN = True if CI else False


def create_branch(version_branch):
    if confirm_action(f"Create version branch: {version_branch}?"):
        if DRY_RUN:
            console_print("Skipping below command on CI")

        run_command(["git", "checkout", "main"], dry_run_override=DRY_RUN, check=True)
        if DRY_RUN:
            console_print("Skipping below command on CI")
        run_command(
            ["git", "checkout", "-b", f"v{version_branch}-test"], dry_run_override=DRY_RUN, check=True
        )
        console_print(f"Created branch: v{version_branch}-test")


def update_default_branch(version_branch):
    if confirm_action("Update default branches?"):
        console_print()
        console_print("You need to update the default branch at:")
        console_print("./dev/breeze/src/airflow_breeze/branch_defaults.py")
        console_print("Change the following:")
        console_print("AIRFLOW_BRANCH = 'main'")
        console_print("DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH = 'constraints-main'")
        console_print()
        console_print("To:")
        console_print()
        console_print(f"AIRFLOW_BRANCH = 'v{version_branch}-test'")
        console_print(f"DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH = 'constraints-{version_branch}'")


def commit_changes(version_branch):
    if confirm_action("Commit the above changes?"):
        if DRY_RUN:
            console_print("Skipping below command on CI")
        run_command(["git", "add", "-p", "."], dry_run_override=DRY_RUN, check=True)
        if DRY_RUN:
            console_print("Skipping below command on CI")
        run_command(
            ["git", "commit", "-m", f"Update default branches for {version_branch}", "--no-verify"],
            dry_run_override=DRY_RUN,
            check=True,
        )


def create_stable_branch(version_branch):
    if confirm_action(f"Create stable branch: v{version_branch}-stable?"):
        if DRY_RUN:
            console_print("Skipping below command on CI")
        run_command(
            ["git", "checkout", "-b", f"v{version_branch}-stable"], dry_run_override=DRY_RUN, check=True
        )
        console_print(f"Created branch: v{version_branch}-stable")
    else:
        run_command(["git", "checkout", f"v{version_branch}-stable"], check=True)


def push_test_and_stable_branch(version_branch):
    if confirm_action("Push test and stable branches?"):
        if DRY_RUN:
            console_print("Skipping below command on CI")

        run_command(["git", "checkout", f"v{version_branch}-test"], dry_run_override=DRY_RUN, check=True)
        if DRY_RUN:
            console_print("Skipping below command on CI")
        run_command(
            ["git", "push", "--set-upstream", "origin", f"v{version_branch}-test"],
            dry_run_override=DRY_RUN,
            check=True,
        )
        if DRY_RUN:
            console_print("Skipping below command on CI")

        run_command(["git", "checkout", f"v{version_branch}-stable"], dry_run_override=DRY_RUN, check=True)
        if DRY_RUN:
            console_print("Skipping below command on CI")
        run_command(
            ["git", "push", "--set-upstream", "origin", f"v{version_branch}-stable"],
            dry_run_override=DRY_RUN,
            check=True,
        )


def checkout_main():
    if confirm_action("We now need to checkout main. Continue?"):
        if DRY_RUN:
            console_print("Skipping below command on CI")
        result = run_command(
            ["git", "checkout", "main"], dry_run_override=DRY_RUN, check=False, capture_output=True
        )
        if result.returncode != 0:
            console_print("[error]Failed to checkout main.[/]")
            console_print(result.stdout)
            console_print(result.stderr)
            sys.exit(1)
        if DRY_RUN:
            console_print("Skipping below command on CI")
        result = run_command(["git", "pull"], dry_run_override=DRY_RUN, capture_output=True, check=False)
        if result.returncode != 0:
            console_print("[error]Failed to pull repo.[/]")
            console_print(result.stdout)
            console_print(result.stderr)
            sys.exit(1)


def instruction_update_version_branch(version_branch):
    if confirm_action("Now, we need to manually update the version branches in main. Continue?"):
        console_print()
        console_print(
            f"Add v{version_branch}-stable and v{version_branch}-test branches "
            "in codecov.yml (there are 2 places in the file!)"
        )
        console_print("Areas to add the branches will look like this:")
        console_print(
            """
            branches:
                - main
                - v2-0-stable
                - v2-0-test
                - v2-1-stable
                - v2-1-test
                - v2-2-stable
                - v2-2-test
            """
        )
        console_print()
        console_print(f"Add v{version_branch}-stable to .asf.yaml ({version_branch} is your new branch)")
        console_print(
            f"""
            protected_branches:
            main:
                required_pull_request_reviews:
                required_approving_review_count: 1
            ...
            v{version_branch}-stable:
                required_pull_request_reviews:
                required_approving_review_count: 1
            """
        )
        console_print("Once you finish with the above. Commit the changes and make a PR against main")
        confirm_action("I'm done with the changes. Continue?", abort=True)


def create_constraints(version_branch):
    if confirm_action("Do you want to create branches from the constraints main?"):
        if DRY_RUN:
            console_print("Skipping below 4 commands on CI")
        run_command(["git", "checkout", "constraints-main"], dry_run_override=DRY_RUN, check=True)
        run_command(["git", "pull", "origin", "constraints-main"], dry_run_override=DRY_RUN, check=True)
        run_command(
            ["git", "checkout", "-b", f"constraints-{version_branch}"], dry_run_override=DRY_RUN, check=True
        )
        if confirm_action("Push the new branch?"):
            run_command(
                ["git", "push", "--set-upstream", "origin", f"constraints-{version_branch}"],
                dry_run_override=DRY_RUN,
                check=True,
            )


@release_management.command(
    name="create-minor-branch",
    help="Create a new version branch and update the default branches in main",
)
@click.option("--version-branch", help="The version branch you want to create e.g 2-4", required=True)
@option_answer
def create_minor_version_branch(version_branch):
    for obj in version_branch.split("-"):
        if not obj.isdigit():
            console_print(f"[error]Failed `version_branch` part {obj!r} not a digit.")
            sys.exit(1)
        elif len(obj) > 1 and obj.startswith("0"):
            # `01` is a valid digit string, as well as it could be converted to the integer,
            # however, it might be considered as typo (e.g. 10) so better stop here
            console_print(
                f"[error]Found leading zero into the `version_branch` part {obj!r} ",
                f"if it is not a typo consider to use {int(obj)} instead.",
            )
            sys.exit(1)

    os.chdir(AIRFLOW_SOURCES_ROOT)
    repo_root = os.getcwd()
    console_print()
    console_print(f"Repo root: {repo_root}")
    console_print(f"Version branch: {version_branch}")
    console_print("Below are your git remotes. We will push to origin:")
    run_command(["git", "remote", "-v"])
    console_print()
    confirm_action("Verify that the above information is correct. Do you want to continue?", abort=True)
    # Final confirmation
    confirm_action("Pushes will be made to origin. Do you want to continue?", abort=True)
    # Create a new branch from main
    create_branch(version_branch)
    # Build ci image
    if confirm_action("Build latest breeze image?"):
        if DRY_RUN:
            console_print("Skipping below command on CI")
        run_command(["breeze", "ci-image", "build", "--python", "3.9"], dry_run_override=DRY_RUN, check=True)
    # Update default branches
    update_default_branch(version_branch)
    # Commit changes
    commit_changes(version_branch)
    # Create stable branch
    create_stable_branch(version_branch)
    # Push test and stable branches
    push_test_and_stable_branch(version_branch)
    # Checkout main
    checkout_main()
    # Update version branches in main
    instruction_update_version_branch(version_branch)
    # Create constraints branch
    create_constraints(version_branch)
    console_print("Done!")
