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

import sys
import uuid

import click

from airflow_breeze.commands.ci_image_commands import rebuild_or_pull_ci_image_if_needed
from airflow_breeze.commands.common_options import option_dry_run, option_python, option_verbose
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.docker_command_utils import execute_command_in_shell, fix_ownership_using_docker


@click.group(cls=BreezeGroup, name="registry", help="Tools for the Airflow Provider Registry")
def registry_group():
    pass


@registry_group.command(
    name="extract-data",
    help="Extract provider metadata, parameters, and connection types for the registry.",
)
@option_python
@click.option(
    "--provider",
    default=None,
    help="Extract only this provider ID (e.g. 'amazon'). Omit for full build.",
)
@option_verbose
@option_dry_run
def extract_data(python: str, provider: str | None):
    unique_project_name = f"breeze-registry-{uuid.uuid4().hex[:8]}"

    shell_params = ShellParams(
        python=python,
        project_name=unique_project_name,
        quiet=True,
        skip_environment_initialization=True,
        extra_args=(),
    )

    rebuild_or_pull_ci_image_if_needed(command_params=shell_params)

    provider_flag = f" --provider '{provider}'" if provider else ""
    command = (
        f"python dev/registry/extract_metadata.py{provider_flag} && "
        "python dev/registry/extract_parameters.py && "
        "python dev/registry/extract_connections.py"
    )

    with ci_group("Extracting registry data"):
        result = execute_command_in_shell(
            shell_params=shell_params,
            project_name=unique_project_name,
            command=command,
            preserve_backend=True,
        )

    fix_ownership_using_docker()
    sys.exit(result.returncode)


@registry_group.command(
    name="publish-versions",
    help="Publish per-provider versions.json to S3 from deployed directories. "
    "Same pattern as 'breeze release-management publish-docs-to-s3'.",
)
@click.option(
    "--s3-bucket",
    required=True,
    envvar="S3_BUCKET",
    help="S3 bucket URL, e.g. s3://staging-docs-airflow-apache-org/registry/",
)
@click.option(
    "--providers-json",
    default=None,
    type=click.Path(exists=True),
    help="Path to providers.json. Auto-detected if not provided.",
)
def publish_versions(s3_bucket: str, providers_json: str | None):
    from pathlib import Path

    from airflow_breeze.utils.publish_registry_versions import publish_versions as _publish_versions

    providers_path = Path(providers_json) if providers_json else None
    _publish_versions(s3_bucket, providers_json_path=providers_path)
