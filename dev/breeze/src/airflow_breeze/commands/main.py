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

from airflow_breeze.commands.common_options import (
    option_airflow_extras,
    option_answer,
    option_backend,
    option_db_reset,
    option_debian_version,
    option_dry_run,
    option_force_build,
    option_forward_credentials,
    option_github_repository,
    option_installation_package_format,
    option_integration,
    option_mount_sources,
    option_mssql_version,
    option_mysql_version,
    option_postgres_version,
    option_python,
    option_use_airflow_version,
    option_use_packages_from_dist,
    option_verbose,
)
from airflow_breeze.commands.configure_rich_click import click
from airflow_breeze.utils.path_utils import create_directories


@click.group(invoke_without_command=True, context_settings={'help_option_names': ['-h', '--help']})
@option_verbose
@option_dry_run
@option_python
@option_github_repository
@option_backend
@option_postgres_version
@option_mysql_version
@option_mssql_version
@option_debian_version
@option_forward_credentials
@option_force_build
@option_use_airflow_version
@option_airflow_extras
@option_use_packages_from_dist
@option_installation_package_format
@option_mount_sources
@option_integration
@option_db_reset
@option_answer
@click.pass_context
def main(ctx: click.Context, **kwargs):
    from airflow_breeze.commands.developer_commands import shell

    create_directories()
    if not ctx.invoked_subcommand:
        ctx.forward(shell, extra_args={})
