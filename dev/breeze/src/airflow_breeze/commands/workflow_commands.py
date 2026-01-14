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

import json
import os
import re
import sys

import click

from airflow_breeze.commands.common_options import argument_doc_packages
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.custom_param_types import BetterChoice
from airflow_breeze.utils.gh_workflow_utils import trigger_workflow_and_monitor
from airflow_breeze.utils.run_utils import run_command

WORKFLOW_NAME_MAPS = {
    "publish-docs": "publish-docs-to-s3.yml",
    "airflow-refresh-site": "build.yml",
    "sync-s3-to-github": "s3-to-github.yml",
}

APACHE_AIRFLOW_REPO = "apache/airflow"
APACHE_AIRFLOW_SITE_REPO = "apache/airflow-site"
APACHE_AIRFLOW_SITE_ARCHIVE_REPO = "apache/airflow-site-archive"


@click.group(cls=BreezeGroup, name="workflow-run", help="Tools to manage Airflow repository workflows ")
def workflow_run_group():
    pass


@workflow_run_group.command(name="publish-docs", help="Trigger publish docs to S3 workflow")
@click.option(
    "--ref",
    help="Git reference tag to checkout to build documentation.",
    required=True,
)
@click.option(
    "--skip-tag-validation",
    help="Skip validation of the tag. Allows to use `main` or commit hash. Use with caution.",
    is_flag=True,
)
@click.option(
    "--exclude-docs",
    help="Comma separated short name list of docs packages to exclude from the publish. (example: apache.druid,google)",
    default="no-docs-excluded",
)
@click.option(
    "--site-env",
    help="S3 bucket to which the documentation will be published.",
    default="auto",
    type=BetterChoice(["auto", "live", "staging"]),
)
@click.option(
    "--skip-write-to-stable-folder",
    help="Skip writing to stable folder.",
    is_flag=True,
)
@click.option(
    "--airflow-version",
    help="Override Airflow Version to use for the docs build. "
    "If not provided, it will be extracted from the ref. If only base version is provided, it will be "
    "set to the same as the base version.",
    default=None,
    type=str,
)
@click.option(
    "--airflow-base-version",
    help="Override Airflow Base Version to use for the docs build. "
    "If not provided, it will be extracted from the ref. If airflow-version is provided, the "
    "base version of the version provided (i.e. stripped pre-/post-/dev- suffixes).",
    default=None,
    type=str,
)
@click.option(
    "--apply-commits",
    help="Apply commits before building the docs - for example to patch fixes "
    "to the docs (comma separated list of commits). ",
    default=None,
    type=str,
)
@click.option(
    "--workflow-branch",
    help="Branch to run the workflow on. Defaults to 'main'.",
    default="main",
    type=str,
)
@argument_doc_packages
def workflow_run_publish(
    ref: str,
    exclude_docs: str,
    site_env: str,
    skip_tag_validation: bool,
    doc_packages: tuple[str, ...],
    skip_write_to_stable_folder: bool = False,
    airflow_version: str | None = None,
    airflow_base_version: str | None = None,
    apply_commits: str | None = None,
    workflow_branch: str = "main",
):
    if len(doc_packages) == 0:
        get_console().print(
            "[red]Error: No doc packages provided. Please provide at least one doc package.[/red]",
        )
        sys.exit(1)
    if os.environ.get("GITHUB_TOKEN", ""):
        get_console().print("\n[warning]Your authentication will use GITHUB_TOKEN environment variable.")
        get_console().print(
            "\nThis might not be what you want unless your token has "
            "sufficient permissions to trigger workflows."
        )
        get_console().print(
            "If you remove GITHUB_TOKEN, workflow_run will use the authentication you already "
            "set-up with `gh auth login`.\n"
        )
    get_console().print(
        f"[blue]Validating ref: {ref}[/blue]",
    )

    if not skip_tag_validation:
        tag_result = run_command(
            ["gh", "api", f"repos/apache/airflow/git/refs/tags/{ref}"],
            capture_output=True,
            check=False,
        )

        stdout = tag_result.stdout.decode("utf-8")
        tag_respo = json.loads(stdout)

        if not tag_respo.get("ref"):
            get_console().print(
                f"[red]Error: Ref {ref} does not exists in repo apache/airflow .[/red]",
            )
            get_console().print("\nYou can add --skip-tag-validation to skip this validation.")
            sys.exit(1)

    get_console().print(
        f"[blue]Triggering workflow {WORKFLOW_NAME_MAPS['publish-docs']}: at {APACHE_AIRFLOW_REPO}[/blue]",
    )
    from packaging.version import InvalidVersion, Version

    if airflow_version:
        try:
            Version(airflow_version)
        except InvalidVersion as e:
            f"[red]Invalid version passed as --airflow-version:  {airflow_version}[/red]: {e}"
            sys.exit(1)
        get_console().print(
            f"[blue]Using provided Airflow version: {airflow_version}[/blue]",
        )
    if airflow_base_version:
        try:
            Version(airflow_base_version)
        except InvalidVersion as e:
            f"[red]Invalid base version passed as --airflow-base-version:  {airflow_version}[/red]: {e}"
            sys.exit(1)
        get_console().print(
            f"[blue]Using provided Airflow base version: {airflow_base_version}[/blue]",
        )
    if not airflow_version and airflow_base_version:
        airflow_version = airflow_base_version
    if airflow_version and not airflow_base_version:
        airflow_base_version = Version(airflow_version).base_version

    joined_packages = " ".join(doc_packages)
    if "providers" in joined_packages and "apache-airflow-providers" not in joined_packages:
        joined_packages = joined_packages + " apache-airflow-providers"

    workflow_fields = {
        "ref": ref,
        "destination": site_env,
        "include-docs": joined_packages,
        "exclude-docs": exclude_docs,
        "skip-write-to-stable-folder": skip_write_to_stable_folder,
        "build-sboms": "true" if "apache-airflow" in doc_packages else "false",
        "apply-commits": apply_commits if apply_commits else "",
    }

    if airflow_version:
        workflow_fields["airflow-version"] = airflow_version
    if airflow_base_version:
        workflow_fields["airflow-base-version"] = airflow_base_version

    trigger_workflow_and_monitor(
        workflow_name=WORKFLOW_NAME_MAPS["publish-docs"],
        repo=APACHE_AIRFLOW_REPO,
        branch=workflow_branch,
        **workflow_fields,
    )

    if site_env == "auto":
        pattern = re.compile(r"^.*[0-9]+\.[0-9]+\.[0-9]+$")
        if pattern.match(ref):
            site_env = "live"
        else:
            site_env = "staging"

    branch = "main" if site_env == "live" else "staging"

    get_console().print(
        f"[blue]Refreshing site at {APACHE_AIRFLOW_SITE_REPO}[/blue]",
    )
    wf_name = WORKFLOW_NAME_MAPS["airflow-refresh-site"]

    get_console().print(
        f"[blue]Triggering workflow {wf_name}: at {APACHE_AIRFLOW_SITE_REPO}[/blue]",
    )

    trigger_workflow_and_monitor(
        workflow_name=wf_name,
        repo=APACHE_AIRFLOW_SITE_REPO,
        branch=branch,
    )

    get_console().print(
        f"[blue]Refreshing completed workflow {wf_name}: at {APACHE_AIRFLOW_SITE_REPO}[/blue]",
    )

    workflow_fields = {"source": site_env}

    get_console().print(
        f"[blue]Syncing S3 docs to GitHub repository at {APACHE_AIRFLOW_SITE_ARCHIVE_REPO}[/blue]",
    )
    trigger_workflow_and_monitor(
        workflow_name=WORKFLOW_NAME_MAPS["sync-s3-to-github"],
        repo=APACHE_AIRFLOW_SITE_ARCHIVE_REPO,
        branch=branch,
        **workflow_fields,
        monitor=False,
    )
