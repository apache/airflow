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

import multiprocessing as mp

import click

from airflow_breeze.global_constants import (
    ALL_HISTORICAL_PYTHON_VERSIONS,
    ALLOWED_BACKENDS,
    ALLOWED_DOCKER_COMPOSE_PROJECTS,
    ALLOWED_INSTALLATION_PACKAGE_FORMATS,
    ALLOWED_MOUNT_OPTIONS,
    ALLOWED_MYSQL_VERSIONS,
    ALLOWED_POSTGRES_VERSIONS,
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    ALLOWED_USE_AIRFLOW_VERSIONS,
    APACHE_AIRFLOW_GITHUB_REPOSITORY,
    AUTOCOMPLETE_INTEGRATIONS,
    DEFAULT_UV_HTTP_TIMEOUT,
)
from airflow_breeze.utils.custom_param_types import (
    AnswerChoice,
    BackendVersionChoice,
    BetterChoice,
    CacheableChoice,
    CacheableDefault,
    DryRunOption,
    MySQLBackendVersionChoice,
    NotVerifiedBetterChoice,
    UseAirflowVersionType,
    VerboseOption,
)
from airflow_breeze.utils.packages import get_available_packages
from airflow_breeze.utils.recording import generating_command_images


def _set_default_from_parent(ctx: click.core.Context, option: click.core.Option, value):
    from click.core import ParameterSource

    if (
        ctx.parent
        and option.name in ctx.parent.params
        and ctx.get_parameter_source(option.name)
        in (
            ParameterSource.DEFAULT,
            ParameterSource.DEFAULT_MAP,
        )
    ):
        # Current value is the default, use the parent's value (i.e. for `breeze
        # # -v static-checks` respect the "global" option)
        value = ctx.parent.params[option.name]
    return value


argument_doc_packages = click.argument(
    "doc_packages",
    nargs=-1,
    required=False,
    type=NotVerifiedBetterChoice(
        get_available_packages(
            include_non_provider_doc_packages=True,
            include_all_providers=True,
            include_removed=True,
            include_not_ready=True,
        )
    ),
)
option_airflow_extras = click.option(
    "--airflow-extras",
    help="Airflow extras to install when --use-airflow-version is used",
    default="",
    show_default=True,
    envvar="AIRFLOW_EXTRAS",
)
option_answer = click.option(
    "-a",
    "--answer",
    type=AnswerChoice(["y", "n", "q", "yes", "no", "quit"]),
    help="Force answer to questions.",
    envvar="ANSWER",
    expose_value=False,
    callback=_set_default_from_parent,
)
option_backend = click.option(
    "-b",
    "--backend",
    type=CacheableChoice(ALLOWED_BACKENDS),
    default=CacheableDefault(value=ALLOWED_BACKENDS[0]),
    show_default=True,
    help="Database backend to use. If 'none' is chosen, "
    "Breeze will start with an invalid database configuration, meaning there will be no database "
    "available, and any attempts to connect to the Airflow database will fail.",
    envvar="BACKEND",
)
option_builder = click.option(
    "--builder",
    help="Buildx builder used to perform `docker buildx build` commands.",
    envvar="BUILDER",
    show_default=True,
    default="autodetect",
)
option_clean_airflow_installation = click.option(
    "--clean-airflow-installation",
    help="Clean the airflow installation before installing version specified by --use-airflow-version.",
    is_flag=True,
    envvar="CLEAN_AIRFLOW_INSTALLATION",
)
option_commit_sha = click.option(
    "--commit-sha",
    show_default=True,
    envvar="COMMIT_SHA",
    help="Commit SHA that is used to build the images.",
)
option_db_reset = click.option(
    "-d",
    "--db-reset",
    help="Reset DB when entering the container.",
    is_flag=True,
    envvar="DB_RESET",
)
option_debug_resources = click.option(
    "--debug-resources",
    is_flag=True,
    help="Whether to show resource information while running in parallel.",
    envvar="DEBUG_RESOURCES",
)
option_database_isolation = click.option(
    "--database-isolation",
    help="Run airflow in database isolation mode.",
    is_flag=True,
    envvar="DATABASE_ISOLATION",
)
option_docker_host = click.option(
    "--docker-host",
    help="Optional - docker host to use when running docker commands. "
    "When set, the `--builder` option is ignored when building images.",
    envvar="DOCKER_HOST",
)
option_downgrade_sqlalchemy = click.option(
    "--downgrade-sqlalchemy",
    help="Downgrade SQLAlchemy to minimum supported version.",
    is_flag=True,
    envvar="DOWNGRADE_SQLALCHEMY",
)
option_downgrade_pendulum = click.option(
    "--downgrade-pendulum",
    help="Downgrade Pendulum to minimum supported version.",
    is_flag=True,
    envvar="DOWNGRADE_PENDULUM",
)
option_dry_run = click.option(
    "-D",
    "--dry-run",
    is_flag=True,
    help="If dry-run is set, commands are only printed, not executed.",
    envvar="DRY_RUN",
    metavar="BOOLEAN",
    expose_value=False,
    type=DryRunOption(),
    callback=_set_default_from_parent,
)
option_forward_credentials = click.option(
    "-f",
    "--forward-credentials",
    help="Forward local credentials to container when running.",
    is_flag=True,
)
option_excluded_providers = click.option(
    "--excluded-providers",
    help="JSON-string of dictionary containing excluded providers per python version ({'3.12': ['provider']})",
    envvar="EXCLUDED_PROVIDERS",
)
option_force_lowest_dependencies = click.option(
    "--force-lowest-dependencies",
    help="Run tests for the lowest direct dependencies of Airflow or selected provider if "
    "`Provider[PROVIDER_ID]` is used as test type.",
    is_flag=True,
    envvar="FORCE_LOWEST_DEPENDENCIES",
)
option_github_token = click.option(
    "--github-token",
    help="The token used to authenticate to GitHub.",
    envvar="GITHUB_TOKEN",
)
option_github_repository = click.option(
    "-g",
    "--github-repository",
    help="GitHub repository used to pull, push run images.",
    default=APACHE_AIRFLOW_GITHUB_REPOSITORY,
    show_default=True,
    envvar="GITHUB_REPOSITORY",
    callback=_set_default_from_parent,
)
option_historical_python_version = click.option(
    "--python",
    type=BetterChoice(ALL_HISTORICAL_PYTHON_VERSIONS),
    required=False,
    envvar="PYTHON_VERSION",
    help="Python version to update sbom from. (defaults to all historical python versions)",
)
option_include_removed_providers = click.option(
    "--include-removed-providers",
    help="Whether to include providers that are removed.",
    is_flag=True,
    envvar="INCLUDE_REMOVED_PROVIDERS",
)
option_include_not_ready_providers = click.option(
    "--include-not-ready-providers",
    help="Whether to include providers that are not yet ready to be released.",
    is_flag=True,
    envvar="INCLUDE_NOT_READY_PROVIDERS",
)
option_include_success_outputs = click.option(
    "--include-success-outputs",
    help="Whether to include outputs of successful parallel runs (skipped by default).",
    is_flag=True,
    envvar="INCLUDE_SUCCESS_OUTPUTS",
)
option_integration = click.option(
    "--integration",
    help="Integration(s) to enable when running (can be more than one).",
    type=BetterChoice(AUTOCOMPLETE_INTEGRATIONS),
    multiple=True,
)
option_image_name = click.option(
    "-n",
    "--image-name",
    help="Name of the image to verify (overrides --python and --image-tag).",
)
option_image_tag_for_running = click.option(
    "--image-tag",
    help="Tag of the image which is used to run the image (implies --mount-sources=skip).",
    show_default=True,
    default="latest",
    envvar="IMAGE_TAG",
)
option_keep_env_variables = click.option(
    "--keep-env-variables",
    help="Do not clear environment variables that might have side effect while running tests",
    envvar="KEEP_ENV_VARIABLES",
    is_flag=True,
)
option_max_time = click.option(
    "--max-time",
    help="Maximum time that the command should take - if it takes longer, the command will fail.",
    type=click.IntRange(min=1),
    envvar="MAX_TIME",
    callback=_set_default_from_parent,
)
option_mount_sources = click.option(
    "--mount-sources",
    type=BetterChoice(ALLOWED_MOUNT_OPTIONS),
    default=ALLOWED_MOUNT_OPTIONS[0],
    show_default=True,
    envvar="MOUNT_SOURCES",
    help="Choose scope of local sources that should be mounted, skipped, or removed (default = selected).",
)
option_mysql_version = click.option(
    "-M",
    "--mysql-version",
    help="Version of MySQL used.",
    type=MySQLBackendVersionChoice(ALLOWED_MYSQL_VERSIONS),
    default=CacheableDefault(ALLOWED_MYSQL_VERSIONS[0]),
    envvar="MYSQL_VERSION",
    show_default=True,
)
option_no_db_cleanup = click.option(
    "--no-db-cleanup",
    help="Do not clear the database before each test module",
    is_flag=True,
)
option_installation_package_format = click.option(
    "--package-format",
    type=BetterChoice(ALLOWED_INSTALLATION_PACKAGE_FORMATS),
    help="Format of packages that should be installed from dist.",
    default=ALLOWED_INSTALLATION_PACKAGE_FORMATS[0],
    show_default=True,
    envvar="PACKAGE_FORMAT",
)
option_parallelism = click.option(
    "--parallelism",
    help="Maximum number of processes to use while running the operation in parallel.",
    type=click.IntRange(1, mp.cpu_count() * 3 if not generating_command_images() else 8),
    default=mp.cpu_count() if not generating_command_images() else 4,
    envvar="PARALLELISM",
    show_default=True,
)
option_postgres_version = click.option(
    "-P",
    "--postgres-version",
    type=BackendVersionChoice(ALLOWED_POSTGRES_VERSIONS),
    default=CacheableDefault(ALLOWED_POSTGRES_VERSIONS[0]),
    envvar="POSTGRES_VERSION",
    show_default=True,
    help="Version of Postgres used.",
)
option_project_name = click.option(
    "--project-name",
    help="Name of the docker-compose project to bring down. "
    "The `docker-compose` is for legacy breeze project name and you can use "
    "`breeze down --project-name docker-compose` to stop all containers belonging to it.",
    show_default=True,
    type=NotVerifiedBetterChoice(ALLOWED_DOCKER_COMPOSE_PROJECTS),
    default=ALLOWED_DOCKER_COMPOSE_PROJECTS[0],
    envvar="PROJECT_NAME",
)
option_python = click.option(
    "-p",
    "--python",
    type=CacheableChoice(ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS),
    default=CacheableDefault(value=ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS[0]),
    show_default=True,
    help="Python major/minor version used in Airflow image for images.",
    envvar="PYTHON_MAJOR_MINOR_VERSION",
)
option_python_versions = click.option(
    "--python-versions",
    help="Space separated list of python versions used for build with multiple versions.",
    default=" ".join(ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS),
    show_default=True,
    envvar="PYTHON_VERSIONS",
)
option_run_db_tests_only = click.option(
    "--run-db-tests-only",
    help="Only runs tests that require a database",
    is_flag=True,
    envvar="run_db_tests_only",
)
option_run_in_parallel = click.option(
    "--run-in-parallel",
    help="Run the operation in parallel on all or selected subset of parameters.",
    is_flag=True,
    envvar="RUN_IN_PARALLEL",
)
option_skip_cleanup = click.option(
    "--skip-cleanup",
    help="Skip cleanup of temporary files created during parallel run.",
    is_flag=True,
    envvar="SKIP_CLEANUP",
)
option_skip_db_tests = click.option(
    "--skip-db-tests",
    help="Skip tests that require a database",
    is_flag=True,
    envvar="SKIP_DB_TESTS",
)
option_standalone_dag_processor = click.option(
    "--standalone-dag-processor",
    help="Run standalone dag processor for start-airflow.",
    is_flag=True,
    envvar="STANDALONE_DAG_PROCESSOR",
)
option_upgrade_boto = click.option(
    "--upgrade-boto",
    help="Remove aiobotocore and upgrade botocore and boto to the latest version.",
    is_flag=True,
    envvar="UPGRADE_BOTO",
)
option_use_uv = click.option(
    "--use-uv/--no-use-uv",
    is_flag=True,
    default=True,
    show_default=True,
    help="Use uv instead of pip as packaging tool to build the image.",
    envvar="USE_UV",
)
option_use_uv_default_disabled = click.option(
    "--use-uv/--no-use-uv",
    is_flag=True,
    default=False,
    show_default=True,
    help="Use uv instead of pip as packaging tool to build the image.",
    envvar="USE_UV",
)
option_uv_http_timeout = click.option(
    "--uv-http-timeout",
    help="Timeout for requests that UV makes (only used in case of UV builds).",
    type=click.IntRange(min=1),
    default=DEFAULT_UV_HTTP_TIMEOUT,
    show_default=True,
    envvar="UV_HTTP_TIMEOUT",
)
option_use_airflow_version = click.option(
    "--use-airflow-version",
    help="Use (reinstall at entry) Airflow version from PyPI. It can also be version (to install from PyPI), "
    "`none`, `wheel`, or `sdist` to install from `dist` folder, or VCS URL to install from "
    "(https://pip.pypa.io/en/stable/topics/vcs-support/). Implies --mount-sources `remove`.",
    type=UseAirflowVersionType(ALLOWED_USE_AIRFLOW_VERSIONS),
    envvar="USE_AIRFLOW_VERSION",
)
option_airflow_version = click.option(
    "-A",
    "--airflow-version",
    help="Airflow version to use for the command.",
    type=str,
    envvar="AIRFLOW_VERSION",
    required=True,
)
option_verbose = click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print verbose information about performed steps.",
    envvar="VERBOSE",
    metavar="BOOLEAN",
    expose_value=False,
    type=VerboseOption(),
    callback=_set_default_from_parent,
)
option_version_suffix_for_pypi = click.option(
    "--version-suffix-for-pypi",
    help="Version suffix used for PyPI packages (alpha, beta, rc1, etc.).",
    envvar="VERSION_SUFFIX_FOR_PYPI",
    default="",
)
