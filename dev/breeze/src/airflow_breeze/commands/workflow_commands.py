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
def workflow_run():
    pass


@workflow_run.command(name="publish-docs", help="Trigger publish docs to S3 workflow")
@click.option(
    "--ref",
    help="Git reference tag to checkout to build documentation.",
    required=True,
)
@click.option(
    "--exclude-docs",
    help="Comma separated list of docs packages to exclude from the publish.",
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
@argument_doc_packages
def workflow_run_publish(
    ref: str,
    exclude_docs: str,
    site_env: str,
    doc_packages: tuple[str, ...],
    skip_write_to_stable_folder: bool = False,
):
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

    tag_result = run_command(
        ["gh", "api", f"repos/apache/airflow/git/refs/tags/{ref}"],
        capture_output=True,
        check=False,
    )

    stdout = tag_result.stdout.decode("utf-8")
    tag_respo = json.loads(stdout)

    if not tag_respo.get("ref"):
        get_console().print(
            f"[red]Error: Ref {ref} is not exists in repo apache/airflow .[/red]",
        )
        sys.exit(1)

    get_console().print(
        f"[blue]Triggering workflow {WORKFLOW_NAME_MAPS['publish-docs']}: at {APACHE_AIRFLOW_REPO}[/blue]",
    )

    workflow_fields = {
        "ref": ref,
        "destination": site_env,
        "include-docs": " ".join(doc_packages),
        "exclude-docs": exclude_docs,
        "skip-write-to-stable-folder": skip_write_to_stable_folder,
    }

    trigger_workflow_and_monitor(
        workflow_name=WORKFLOW_NAME_MAPS["publish-docs"],
        repo=APACHE_AIRFLOW_REPO,
        branch="main",
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
