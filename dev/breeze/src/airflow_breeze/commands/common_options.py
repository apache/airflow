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
    ALLOWED_AUTH_MANAGERS,
    ALLOWED_BACKENDS,
    ALLOWED_DOCKER_COMPOSE_PROJECTS,
    ALLOWED_INSTALLATION_DISTRIBUTION_FORMATS,
    ALLOWED_MOUNT_OPTIONS,
    ALLOWED_MYSQL_VERSIONS,
    ALLOWED_POSTGRES_VERSIONS,
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    ALLOWED_TTY,
    ALLOWED_USE_AIRFLOW_VERSIONS,
    APACHE_AIRFLOW_GITHUB_REPOSITORY,
    AUTOCOMPLETE_ALL_INTEGRATIONS,
    AUTOCOMPLETE_CORE_INTEGRATIONS,
    AUTOCOMPLETE_PROVIDERS_INTEGRATIONS,
    DEFAULT_POSTGRES_VERSION,
    DEFAULT_UV_HTTP_TIMEOUT,
    DOCKER_DEFAULT_PLATFORM,
    SINGLE_PLATFORMS,
    normalize_platform_machine,
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
from airflow_breeze.utils.packages import get_available_distributions
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
        get_available_distributions(
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
    help="Database backend to use. Default is 'sqlite'. "
    "If 'none' is chosen, Breeze will start with an invalid database configuration — "
    "no database will be available, and any attempt to run Airflow will fail. "
    "Use 'none' only for specific non-DB test cases.",
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
    "--db-reset/--no-db-reset",
    help="Reset DB when entering the container.",
    is_flag=True,
    default=False,
    show_default=True,
    envvar="DB_RESET",
)
option_debug_resources = click.option(
    "--debug-resources",
    is_flag=True,
    help="Whether to show resource information while running in parallel.",
    envvar="DEBUG_RESOURCES",
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
    "-f", "--forward-credentials", help="Forward local credentials to container when running.", is_flag=True
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
option_historical_python_versions = click.option(
    "--python-versions",
    type=BetterChoice(ALL_HISTORICAL_PYTHON_VERSIONS),
    required=False,
    envvar="PYTHON_VERSIONS",
    help="Comma separate list of Python versions to update sbom from "
    "(defaults to all historical python versions)",
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
    help="Whether to include outputs of successful runs (not shown by default).",
    is_flag=True,
    envvar="INCLUDE_SUCCESS_OUTPUTS",
)
option_all_integration = click.option(
    "--integration",
    help="Core Integrations to enable when running (can be more than one).",
    type=BetterChoice(AUTOCOMPLETE_ALL_INTEGRATIONS),
    multiple=True,
)

option_core_integration = click.option(
    "--integration",
    help="Core Integrations to enable when running (can be more than one).",
    type=BetterChoice(AUTOCOMPLETE_CORE_INTEGRATIONS),
    multiple=True,
)
option_providers_integration = click.option(
    "--integration",
    help="Providers Integration(s) to enable when running (can be more than one).",
    type=BetterChoice(AUTOCOMPLETE_PROVIDERS_INTEGRATIONS),
    multiple=True,
)
option_image_name = click.option(
    "-n", "--image-name", help="Name of the image to verify (overrides --python)."
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
option_installation_distribution_format = click.option(
    "--distribution-format",
    type=BetterChoice(ALLOWED_INSTALLATION_DISTRIBUTION_FORMATS),
    help="Format of packages that should be installed from dist.",
    default=ALLOWED_INSTALLATION_DISTRIBUTION_FORMATS[0],
    show_default=True,
    envvar="DISTRIBUTION_FORMAT",
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
    default=CacheableDefault(DEFAULT_POSTGRES_VERSION),
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
option_python_no_default = click.option(
    "-p",
    "--python",
    type=BetterChoice(ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS),
    help="Python major/minor version used in Airflow image for images "
    "(if not specified - all python versions are used).",
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
    "--standalone-dag-processor/--no-standalone-dag-processor",
    is_flag=True,
    default=True,
    show_default=True,
    help="Run standalone dag processor for start-airflow (required for Airflow 3).",
    envvar="STANDALONE_DAG_PROCESSOR",
)
option_use_mprocs = click.option(
    "--use-mprocs/--use-tmux",
    is_flag=True,
    default=False,
    show_default=True,
    help="Use mprocs instead of tmux for start-airflow.",
    envvar="USE_MPROCS",
)
option_tty = click.option(
    "--tty",
    envvar="TTY",
    type=BetterChoice(ALLOWED_TTY),
    default=ALLOWED_TTY[0],
    show_default=True,
    help="Whether to allocate pseudo-tty when running docker command"
    " (useful for prek and CI to force-enable it).",
)
option_upgrade_boto = click.option(
    "--upgrade-boto",
    help="Remove aiobotocore and upgrade botocore and boto to the latest version.",
    is_flag=True,
    envvar="UPGRADE_BOTO",
)
option_upgrade_sqlalchemy = click.option(
    "--upgrade-sqlalchemy",
    help="Upgrade SQLAlchemy to the latest version.",
    is_flag=True,
    envvar="UPGRADE_SQLALCHEMY",
)
option_use_uv = click.option(
    "--use-uv/--no-use-uv",
    is_flag=True,
    default=True,
    show_default=True,
    help="Use uv instead of pip as packaging tool to build the image.",
    envvar="USE_UV",
)
option_use_uv_default_depends_on_installation_method = click.option(
    "--use-uv/--no-use-uv",
    is_flag=True,
    default=None,
    help="Use uv instead of pip as packaging tool to build the image (default is True for installing "
    "from sources and False for installing from packages).",
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
    "`none`, `wheel`, or `sdist` to install from `dist` folder, `owner/repo:branch` to "
    "install from GitHub repo, or a PR number (e.g., `57219`) to install from a pull request. "
    "Uses --mount-sources `remove` if not specified, but `providers-and-tests` "
    "or `tests` can be specified for `--mount-sources` when `--use-airflow-version` is used.",
    type=UseAirflowVersionType(ALLOWED_USE_AIRFLOW_VERSIONS),
    envvar="USE_AIRFLOW_VERSION",
)
option_allow_pre_releases = click.option(
    "--allow-pre-releases",
    help="Allow pre-releases of Airflow, task-sdk, providers and airflowctl to be installed. "
    "Set to true automatically for pre-release --use-airflow-version)",
    is_flag=True,
    envvar="ALLOW_PRE_RELEASES",
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
option_install_airflow_with_constraints = click.option(
    "--install-airflow-with-constraints/--no-install-airflow-with-constraints",
    is_flag=True,
    default=False,
    show_default=True,
    envvar="INSTALL_AIRFLOW_WITH_CONSTRAINTS",
    help="Install airflow in a separate step, with constraints determined from package or airflow version.",
)
option_install_airflow_with_constraints_default_true = click.option(
    "--install-airflow-with-constraints/--no-install-airflow-with-constraints",
    is_flag=True,
    default=True,
    show_default=True,
    envvar="INSTALL_AIRFLOW_WITH_CONSTRAINTS",
    help="Install airflow in a separate step, with constraints determined from package or airflow version.",
)
option_debug_components = click.option(
    "--debug",
    "debug_components",
    help="Enable debugging for specific Airflow components. Can be one or more of: "
    "scheduler, triggerer, api-server, dag-processor, edge-worker, celery-worker.",
    type=BetterChoice(
        ["scheduler", "triggerer", "api-server", "dag-processor", "edge-worker", "celery-worker"]
    ),
    multiple=True,
    envvar="DEBUG_COMPONENTS",
)
option_debugger = click.option(
    "--debugger",
    help="Debugger to use for debugging Airflow components.",
    type=BetterChoice(["debugpy", "pydevd-pycharm"]),
    default="debugpy",
    show_default=True,
    envvar="DEBUGGER",
)


def _is_number_greater_than_expected(value: str) -> bool:
    digits = [c for c in value.split("+")[0] if c.isdigit()]
    if not digits:
        return False
    if len(digits) == 1 and digits[0] == "0" and not value.startswith(".dev"):
        return False
    return True


def _validate_version_suffix(ctx: click.core.Context, param: click.core.Option, value: str):
    if not value:
        return value
    if any(
        value.startswith(s) for s in ("a", "b", "rc", "+", ".dev", ".post")
    ) and _is_number_greater_than_expected(value):
        return value
    raise click.BadParameter(
        "Version suffix for PyPI packages should be empty or or start with a/b/rc/+/.dev/.post and number "
        "should be greater than 0 for non-dev version."
    )


option_version_suffix = click.option(
    "--version-suffix",
    help="Version suffix used for PyPI packages (a1, a2, b1, rc1, rc2, .dev0, .dev1, .post1, .post2 etc.)."
    " Note the `.` is need in `.dev0` and `.post`. Might be followed with +local_version",
    envvar="VERSION_SUFFIX",
    callback=_validate_version_suffix,
    default="",
)


option_auth_manager = click.option(
    "--auth-manager",
    type=CacheableChoice(ALLOWED_AUTH_MANAGERS, case_sensitive=False),
    help="Specify the auth manager to set",
    default=CacheableDefault(ALLOWED_AUTH_MANAGERS[0]),
    show_default=True,
)


def _normalize_platform(ctx: click.core.Context, param: click.core.Option, value: str):
    if not value:
        return value
    return normalize_platform_machine(value)


option_platform_single = click.option(
    "--platform",
    help="Platform for Airflow image.",
    default=DOCKER_DEFAULT_PLATFORM if not generating_command_images() else SINGLE_PLATFORMS[0],
    envvar="PLATFORM",
    callback=_normalize_platform,
    type=BetterChoice(SINGLE_PLATFORMS),
)


# UI E2E Testing Options

option_airflow_ui_base_url = click.option(
    "--airflow-ui-base-url",
    help="Base URL for Airflow UI during e2e tests",
    default="http://localhost:28080",
    show_default=True,
    envvar="AIRFLOW_UI_BASE_URL",
)

option_browser = click.option(
    "--browser",
    help="Browser to use for e2e tests",
    type=BetterChoice(["chromium", "firefox", "webkit", "all"]),
    default="all",
    show_default=True,
)

option_headed = click.option(
    "--headed",
    help="Run e2e tests in headed mode (show browser window)",
    is_flag=True,
)

option_debug_e2e = click.option(
    "--debug-e2e",
    help="Run e2e tests in debug mode",
    is_flag=True,
)

option_ui_mode = click.option(
    "--ui-mode",
    help="Run e2e tests in Playwright UI mode",
    is_flag=True,
)

option_update_snapshots = click.option(
    "--update-snapshots",
    help="Update visual regression snapshots",
    is_flag=True,
)

option_test_pattern = click.option(
    "--test-pattern",
    help="Glob pattern to filter test files",
    type=str,
)

option_e2e_workers = click.option(
    "--workers",
    help="Number of parallel workers for e2e tests",
    type=int,
    default=1,
    show_default=True,
)

option_e2e_timeout = click.option(
    "--timeout",
    help="Test timeout in milliseconds",
    type=int,
    default=60000,
    show_default=True,
)

option_e2e_reporter = click.option(
    "--reporter",
    help="Test reporter for e2e tests",
    type=BetterChoice(["list", "dot", "line", "json", "junit", "html", "github"]),
    default="html",
    show_default=True,
)

option_test_admin_username = click.option(
    "--test-admin-username",
    help="Admin username for e2e tests",
    default="admin",
    show_default=True,
    envvar="TEST_ADMIN_USERNAME",
)

option_test_admin_password = click.option(
    "--test-admin-password",
    help="Admin password for e2e tests",
    default="admin",
    show_default=True,
    envvar="TEST_ADMIN_PASSWORD",
)

option_skip_airflow_start = click.option(
    "--skip-airflow-start",
    help="Skip starting Airflow services (assume already running)",
    is_flag=True,
)

option_keep_airflow_running = click.option(
    "--keep-airflow-running",
    help="Keep Airflow services running after tests",
    is_flag=True,
)

option_force_reinstall_deps = click.option(
    "--force-reinstall-deps",
    help="Force reinstall UI dependencies",
    is_flag=True,
)
