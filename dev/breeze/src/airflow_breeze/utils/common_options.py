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
from click import IntRange

from airflow_breeze.branch_defaults import DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import (
    ALL_HISTORICAL_PYTHON_VERSIONS,
    ALLOWED_BACKENDS,
    ALLOWED_BUILD_CACHE,
    ALLOWED_BUILD_PROGRESS,
    ALLOWED_CELERY_BROKERS,
    ALLOWED_CONSTRAINTS_MODES_CI,
    ALLOWED_CONSTRAINTS_MODES_PROD,
    ALLOWED_DEBIAN_VERSIONS,
    ALLOWED_INSTALLATION_PACKAGE_FORMATS,
    ALLOWED_MOUNT_OPTIONS,
    ALLOWED_MSSQL_VERSIONS,
    ALLOWED_MYSQL_VERSIONS,
    ALLOWED_PACKAGE_FORMATS,
    ALLOWED_PARALLEL_TEST_TYPE_CHOICES,
    ALLOWED_PLATFORMS,
    ALLOWED_POSTGRES_VERSIONS,
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    ALLOWED_TEST_TYPE_CHOICES,
    ALLOWED_USE_AIRFLOW_VERSIONS,
    APACHE_AIRFLOW_GITHUB_REPOSITORY,
    AUTOCOMPLETE_INTEGRATIONS,
    DEFAULT_CELERY_BROKER,
    SINGLE_PLATFORMS,
    START_AIRFLOW_ALLOWED_EXECUTORS,
    START_AIRFLOW_DEFAULT_ALLOWED_EXECUTORS,
)
from airflow_breeze.utils.custom_param_types import (
    AnswerChoice,
    BetterChoice,
    CacheableChoice,
    CacheableDefault,
    DryRunOption,
    MySQLBackendVersionType,
    NotVerifiedBetterChoice,
    UseAirflowVersionType,
    VerboseOption,
)
from airflow_breeze.utils.packages import get_available_packages
from airflow_breeze.utils.recording import generating_command_images
from airflow_breeze.utils.selective_checks import ALL_CI_SELECTIVE_TEST_TYPES


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
option_answer = click.option(
    "-a",
    "--answer",
    type=AnswerChoice(["y", "n", "q", "yes", "no", "quit"]),
    help="Force answer to questions.",
    envvar="ANSWER",
    expose_value=False,
    callback=_set_default_from_parent,
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
option_python = click.option(
    "-p",
    "--python",
    type=CacheableChoice(ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS),
    default=CacheableDefault(value=ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS[0]),
    show_default=True,
    help="Python major/minor version used in Airflow image for images.",
    envvar="PYTHON_MAJOR_MINOR_VERSION",
)
option_debian_version = click.option(
    "--debian-version",
    type=BetterChoice(ALLOWED_DEBIAN_VERSIONS),
    default=ALLOWED_DEBIAN_VERSIONS[0],
    show_default=True,
    help="Debian version used in Airflow image as base for building images.",
    envvar="DEBIAN_VERSION",
)
option_backend = click.option(
    "-b",
    "--backend",
    type=CacheableChoice(ALLOWED_BACKENDS),
    default=CacheableDefault(value=ALLOWED_BACKENDS[0]),
    show_default=True,
    help="Database backend to use. If 'none' is selected, breeze starts with invalid DB configuration "
    "and no database and any attempts to connect to Airflow DB will fail.",
    envvar="BACKEND",
)
option_integration = click.option(
    "--integration",
    help="Integration(s) to enable when running (can be more than one).",
    type=BetterChoice(AUTOCOMPLETE_INTEGRATIONS),
    multiple=True,
)
option_postgres_version = click.option(
    "-P",
    "--postgres-version",
    type=CacheableChoice(ALLOWED_POSTGRES_VERSIONS),
    default=CacheableDefault(ALLOWED_POSTGRES_VERSIONS[0]),
    show_default=True,
    help="Version of Postgres used.",
)
option_mysql_version = click.option(
    "-M",
    "--mysql-version",
    help="Version of MySQL used.",
    type=MySQLBackendVersionType(ALLOWED_MYSQL_VERSIONS),
    default=CacheableDefault(ALLOWED_MYSQL_VERSIONS[0]),
    show_default=True,
)
option_mssql_version = click.option(
    "-S",
    "--mssql-version",
    help="Version of MsSQL used.",
    type=CacheableChoice(ALLOWED_MSSQL_VERSIONS),
    default=CacheableDefault(ALLOWED_MSSQL_VERSIONS[0]),
    show_default=True,
)
option_forward_credentials = click.option(
    "-f", "--forward-credentials", help="Forward local credentials to container when running.", is_flag=True
)
option_use_airflow_version = click.option(
    "--use-airflow-version",
    help="Use (reinstall at entry) Airflow version from PyPI. It can also be `none`, `wheel`, or `sdist`"
    " if Airflow should be removed, installed from wheel packages or sdist packages available in dist "
    "folder respectively. Implies --mount-sources `remove`.",
    type=UseAirflowVersionType(ALLOWED_USE_AIRFLOW_VERSIONS),
    envvar="USE_AIRFLOW_VERSION",
)
option_airflow_extras = click.option(
    "--airflow-extras",
    help="Airflow extras to install when --use-airflow-version is used",
    default="",
    show_default=True,
    envvar="AIRFLOW_EXTRAS",
)
option_mount_sources = click.option(
    "--mount-sources",
    type=BetterChoice(ALLOWED_MOUNT_OPTIONS),
    default=ALLOWED_MOUNT_OPTIONS[0],
    show_default=True,
    help="Choose scope of local sources that should be mounted, skipped, or removed (default = selected).",
)
option_force_build = click.option(
    "--force-build", help="Force image build no matter if it is determined as needed.", is_flag=True
)
option_db_reset = click.option(
    "-d",
    "--db-reset",
    help="Reset DB when entering the container.",
    is_flag=True,
    envvar="DB_RESET",
)
option_use_packages_from_dist = click.option(
    "--use-packages-from-dist",
    is_flag=True,
    help="Install all found packages (--package-format determines type) from 'dist' folder "
    "when entering breeze.",
    envvar="USE_PACKAGES_FROM_DIST",
)
option_docker_cache = click.option(
    "-c",
    "--docker-cache",
    help="Cache option for image used during the build.",
    default=ALLOWED_BUILD_CACHE[0],
    show_default=True,
    type=BetterChoice(ALLOWED_BUILD_CACHE),
)
option_github_token = click.option(
    "--github-token",
    help="The token used to authenticate to GitHub.",
    envvar="GITHUB_TOKEN",
)
option_image_tag_for_pulling = click.option(
    "-t",
    "--image-tag",
    help="Tag of the image which is used to pull the image.",
    show_default=True,
    default="latest",
    envvar="IMAGE_TAG",
)
option_image_tag_for_building = click.option(
    "--image-tag",
    help="Tag the image after building it.",
    show_default=True,
    default="latest",
    envvar="IMAGE_TAG",
)
option_image_tag_for_running = click.option(
    "--image-tag",
    help="Tag of the image which is used to run the image (implies --mount-sources=skip).",
    show_default=True,
    default="latest",
    envvar="IMAGE_TAG",
)
option_image_tag_for_verifying = click.option(
    "-t",
    "--image-tag",
    help="Tag of the image when verifying it.",
    show_default=True,
    default="latest",
    envvar="IMAGE_TAG",
)
option_image_name = click.option(
    "-n", "--image-name", help="Name of the image to verify (overrides --python and --image-tag)."
)
option_platform_multiple = click.option(
    "--platform",
    help="Platform for Airflow image.",
    envvar="PLATFORM",
    type=BetterChoice(ALLOWED_PLATFORMS),
)
option_platform_single = click.option(
    "--platform",
    help="Platform for Airflow image.",
    envvar="PLATFORM",
    type=BetterChoice(SINGLE_PLATFORMS),
)
option_upgrade_to_newer_dependencies = click.option(
    "-u",
    "--upgrade-to-newer-dependencies",
    is_flag=True,
    help="When set, upgrade all PIP packages to latest.",
    envvar="UPGRADE_TO_NEWER_DEPENDENCIES",
)
option_upgrade_on_failure = click.option(
    "--upgrade-on-failure",
    is_flag=True,
    help="When set, attempt to run upgrade to newer dependencies when regular build fails.",
    envvar="UPGRADE_ON_FAILURE",
)
option_additional_extras = click.option(
    "--additional-extras",
    help="Additional extra package while installing Airflow in the image.",
    envvar="ADDITIONAL_AIRFLOW_EXTRAS",
)
option_additional_dev_apt_deps = click.option(
    "--additional-dev-apt-deps",
    help="Additional apt dev dependencies to use when building the images.",
    envvar="ADDITIONAL_DEV_APT_DEPS",
)
option_additional_runtime_apt_deps = click.option(
    "--additional-runtime-apt-deps",
    help="Additional apt runtime dependencies to use when building the images.",
    envvar="ADDITIONAL_RUNTIME_APT_DEPS",
)
option_additional_python_deps = click.option(
    "--additional-python-deps",
    help="Additional python dependencies to use when building the images.",
    envvar="ADDITIONAL_PYTHON_DEPS",
)
option_additional_dev_apt_command = click.option(
    "--additional-dev-apt-command",
    help="Additional command executed before dev apt deps are installed.",
    envvar="ADDITIONAL_DEV_APT_COMMAND",
)
option_additional_runtime_apt_command = click.option(
    "--additional-runtime-apt-command",
    help="Additional command executed before runtime apt deps are installed.",
    envvar="ADDITIONAL_RUNTIME_APT_COMMAND",
)
option_additional_dev_apt_env = click.option(
    "--additional-dev-apt-env",
    help="Additional environment variables set when adding dev dependencies.",
    envvar="ADDITIONAL_DEV_APT_ENV",
)
option_additional_runtime_apt_env = click.option(
    "--additional-runtime-apt-env",
    help="Additional environment variables set when adding runtime dependencies.",
    envvar="ADDITIONAL_RUNTIME_APT_ENV",
)
option_dev_apt_command = click.option(
    "--dev-apt-command",
    help="Command executed before dev apt deps are installed.",
    envvar="DEV_APT_COMMAND",
)
option_dev_apt_deps = click.option(
    "--dev-apt-deps",
    help="Apt dev dependencies to use when building the images.",
    envvar="DEV_APT_DEPS",
)
option_runtime_apt_command = click.option(
    "--runtime-apt-command",
    help="Command executed before runtime apt deps are installed.",
    envvar="RUNTIME_APT_COMMAND",
)
option_runtime_apt_deps = click.option(
    "--runtime-apt-deps",
    help="Apt runtime dependencies to use when building the images.",
    envvar="RUNTIME_APT_DEPS",
)
option_prepare_buildx_cache = click.option(
    "--prepare-buildx-cache",
    help="Prepares build cache (this is done as separate per-platform steps instead of building the image).",
    is_flag=True,
    envvar="PREPARE_BUILDX_CACHE",
)
option_push = click.option(
    "--push",
    help="Push image after building it.",
    is_flag=True,
    envvar="PUSH",
)
option_wait_for_image = click.option(
    "--wait-for-image",
    help="Wait until image is available.",
    is_flag=True,
    envvar="WAIT_FOR_IMAGE",
)
option_tag_as_latest = click.option(
    "--tag-as-latest",
    help="Tags the image as latest and update checksum of all files after pulling. "
    "Useful when you build or pull image with --image-tag.",
    is_flag=True,
    envvar="TAG_AS_LATEST",
)
option_verify = click.option(
    "--verify",
    help="Verify image.",
    is_flag=True,
    envvar="VERIFY",
)
option_additional_pip_install_flags = click.option(
    "--additional-pip-install-flags",
    help="Additional flags added to `pip install` commands (except reinstalling `pip` itself).",
    envvar="ADDITIONAL_PIP_INSTALL_FLAGS",
)

option_install_providers_from_sources = click.option(
    "--install-providers-from-sources",
    help="Install providers from sources when installing.",
    is_flag=True,
    envvar="INSTALL_PROVIDERS_FROM_SOURCES",
)
option_load_example_dags = click.option(
    "-e",
    "--load-example-dags",
    help="Enable configuration to load example DAGs when starting Airflow.",
    is_flag=True,
    envvar="LOAD_EXAMPLES",
)
option_load_default_connection = click.option(
    "-c",
    "--load-default-connections",
    help="Enable configuration to load default connections when starting Airflow.",
    is_flag=True,
    envvar="LOAD_DEFAULT_CONNECTIONS",
)
option_version_suffix_for_pypi = click.option(
    "--version-suffix-for-pypi",
    help="Version suffix used for PyPI packages (alpha, beta, rc1, etc.).",
    default="",
    envvar="VERSION_SUFFIX_FOR_PYPI",
)
option_package_format = click.option(
    "--package-format",
    type=BetterChoice(ALLOWED_PACKAGE_FORMATS),
    help="Format of packages.",
    default=ALLOWED_PACKAGE_FORMATS[0],
    show_default=True,
    envvar="PACKAGE_FORMAT",
)
option_installation_package_format = click.option(
    "--package-format",
    type=BetterChoice(ALLOWED_INSTALLATION_PACKAGE_FORMATS),
    help="Format of packages that should be installed from dist.",
    default=ALLOWED_INSTALLATION_PACKAGE_FORMATS[0],
    show_default=True,
    envvar="PACKAGE_FORMAT",
)
option_python_versions = click.option(
    "--python-versions",
    help="Space separated list of python versions used for build with multiple versions.",
    default=" ".join(ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS),
    show_default=True,
    envvar="PYTHON_VERSIONS",
)
option_run_in_parallel = click.option(
    "--run-in-parallel",
    help="Run the operation in parallel on all or selected subset of parameters.",
    is_flag=True,
    envvar="RUN_IN_PARALLEL",
)
option_parallelism = click.option(
    "--parallelism",
    help="Maximum number of processes to use while running the operation in parallel.",
    type=click.IntRange(1, mp.cpu_count() * 3 if not generating_command_images() else 8),
    default=mp.cpu_count() if not generating_command_images() else 4,
    envvar="PARALLELISM",
    show_default=True,
)
argument_provider_packages = click.argument(
    "provider_packages",
    nargs=-1,
    required=False,
    type=NotVerifiedBetterChoice(get_available_packages()),
)
argument_doc_packages = click.argument(
    "doc_packages",
    nargs=-1,
    required=False,
    type=NotVerifiedBetterChoice(
        get_available_packages(include_non_provider_doc_packages=True, include_all_providers=True)
    ),
)

option_airflow_constraints_reference = click.option(
    "--airflow-constraints-reference",
    help="Constraint reference to use. Useful with --use-airflow-version parameter to specify "
    "constraints for the installed version and to find newer dependencies",
    default=DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH,
    envvar="AIRFLOW_CONSTRAINTS_REFERENCE",
)
option_airflow_constraints_location = click.option(
    "--airflow-constraints-location",
    type=str,
    default="",
    help="If specified, it is used instead of calculating reference to the constraint file. "
    "It could be full remote URL to the location file, or local file placed in `docker-context-files` "
    "(in this case it has to start with /opt/airflow/docker-context-files).",
)
option_airflow_constraints_reference_build = click.option(
    "--airflow-constraints-reference",
    default=DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH,
    help="Constraint reference to use when building the image.",
    envvar="AIRFLOW_CONSTRAINTS_REFERENCE",
)
option_airflow_constraints_mode_update = click.option(
    "--airflow-constraints-mode",
    type=BetterChoice(ALLOWED_CONSTRAINTS_MODES_CI),
    required=False,
    help="Limit constraint update to only selected constraint mode - if selected.",
)
option_airflow_constraints_mode_ci = click.option(
    "--airflow-constraints-mode",
    type=BetterChoice(ALLOWED_CONSTRAINTS_MODES_CI),
    default=ALLOWED_CONSTRAINTS_MODES_CI[0],
    show_default=True,
    help="Mode of constraints for CI image building.",
)
option_airflow_constraints_mode_prod = click.option(
    "--airflow-constraints-mode",
    type=BetterChoice(ALLOWED_CONSTRAINTS_MODES_PROD),
    default=ALLOWED_CONSTRAINTS_MODES_PROD[0],
    show_default=True,
    help="Mode of constraints for PROD image building.",
)
option_pull = click.option(
    "--pull",
    help="Pull image is missing before attempting to verify it.",
    is_flag=True,
    envvar="PULL",
)
option_python_image = click.option(
    "--python-image",
    help="If specified this is the base python image used to build the image. "
    "Should be something like: python:VERSION-slim-bookworm.",
    envvar="PYTHON_IMAGE",
)
option_builder = click.option(
    "--builder",
    help="Buildx builder used to perform `docker buildx build` commands.",
    envvar="BUILDER",
    show_default=True,
    default="autodetect",
)
option_build_progress = click.option(
    "--build-progress",
    help="Build progress.",
    type=BetterChoice(ALLOWED_BUILD_PROGRESS),
    envvar="BUILD_PROGRESS",
    show_default=True,
    default=ALLOWED_BUILD_PROGRESS[0],
)
option_include_success_outputs = click.option(
    "--include-success-outputs",
    help="Whether to include outputs of successful parallel runs (skipped by default).",
    is_flag=True,
    envvar="INCLUDE_SUCCESS_OUTPUTS",
)
option_skip_cleanup = click.option(
    "--skip-cleanup",
    help="Skip cleanup of temporary files created during parallel run.",
    is_flag=True,
    envvar="SKIP_CLEANUP",
)
option_include_mypy_volume = click.option(
    "--include-mypy-volume",
    help="Whether to include mounting of the mypy volume (useful for debugging mypy).",
    is_flag=True,
    envvar="INCLUDE_MYPY_VOLUME",
)
option_max_time = click.option(
    "--max-time",
    help="Maximum time that the command should take - if it takes longer, the command will fail.",
    type=click.IntRange(min=1),
    envvar="MAX_TIME",
    callback=_set_default_from_parent,
)
option_debug_resources = click.option(
    "--debug-resources",
    is_flag=True,
    help="Whether to show resource information while running in parallel.",
    envvar="DEBUG_RESOURCES",
)
option_executor = click.option(
    "--executor",
    type=click.Choice(START_AIRFLOW_ALLOWED_EXECUTORS, case_sensitive=False),
    help="Specify the executor to use with airflow.",
    default=START_AIRFLOW_DEFAULT_ALLOWED_EXECUTORS,
    show_default=True,
)
option_celery_broker = click.option(
    "--celery-broker",
    type=click.Choice(ALLOWED_CELERY_BROKERS, case_sensitive=False),
    help="Specify the celery message broker",
    default=DEFAULT_CELERY_BROKER,
    show_default=True,
)
option_celery_flower = click.option("--celery-flower", help="Start celery flower", is_flag=True)
option_standalone_dag_processor = click.option(
    "--standalone-dag-processor",
    help="Run standalone dag processor for start-airflow.",
    is_flag=True,
    envvar="STANDALONE_DAG_PROCESSOR",
)
option_database_isolation = click.option(
    "--database-isolation",
    help="Run airflow in database isolation mode.",
    is_flag=True,
    envvar="DATABASE_ISOLATION",
)
option_install_selected_providers = click.option(
    "--install-selected-providers",
    help="Comma-separated list of providers selected to be installed (implies --use-packages-from-dist).",
    envvar="INSTALL_SELECTED_PROVIDERS",
    default="",
)
option_skip_constraints = click.option(
    "--skip-constraints",
    is_flag=True,
    help="Do not use constraints when installing providers.",
    envvar="SKIP_CONSTRAINTS",
)
option_historical_python_version = click.option(
    "--python",
    type=BetterChoice(ALL_HISTORICAL_PYTHON_VERSIONS),
    required=False,
    envvar="PYTHON_VERSION",
    help="Python version to update sbom from. (defaults to all historical python versions)",
)
option_commit_sha = click.option(
    "--commit-sha",
    default=None,
    show_default=True,
    envvar="COMMIT_SHA",
    help="Commit SHA that is used to build the images.",
)
option_build_timeout_minutes = click.option(
    "--build-timeout-minutes",
    required=False,
    type=int,
    envvar="BUILD_TIMEOUT_MINUTES",
    help="Optional timeout for the build in minutes. Useful to detect `pip` backtracking problems.",
)
option_eager_upgrade_additional_requirements = click.option(
    "--eager-upgrade-additional-requirements",
    required=False,
    type=str,
    envvar="EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS",
    help="Optional additional requirements to upgrade eagerly to avoid backtracking "
    "(see `breeze ci find-backtracking-candidates`).",
)
option_airflow_site_directory = click.option(
    "-a",
    "--airflow-site-directory",
    envvar="AIRFLOW_SITE_DIRECTORY",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, resolve_path=True),
    help="Local directory path of cloned airflow-site repo.",
    required=True,
)
option_upgrade_boto = click.option(
    "--upgrade-boto",
    help="Remove aiobotocore and upgrade botocore and boto to the latest version.",
    is_flag=True,
    envvar="UPGRADE_BOTO",
)
option_downgrade_sqlalchemy = click.option(
    "--downgrade-sqlalchemy",
    help="Downgrade SQLAlchemy to minimum supported version.",
    is_flag=True,
    envvar="DOWNGRADE_SQLALCHEMY",
)
option_run_db_tests_only = click.option(
    "--run-db-tests-only",
    help="Only runs tests that require a database",
    is_flag=True,
    envvar="run_db_tests_only",
)
option_skip_db_tests = click.option(
    "--skip-db-tests",
    help="Skip tests that require a database",
    is_flag=True,
    envvar="SKIP_DB_TESTS",
)
option_test_timeout = click.option(
    "--test-timeout",
    help="Test timeout in seconds. Set the pytest setup, execution and teardown timeouts to this value",
    default=60,
    envvar="TEST_TIMEOUT",
    type=IntRange(min=0),
    show_default=True,
)
option_enable_coverage = click.option(
    "--enable-coverage",
    help="Enable coverage capturing for tests in the form of XML files",
    is_flag=True,
    envvar="ENABLE_COVERAGE",
)
option_skip_provider_tests = click.option(
    "--skip-provider-tests",
    help="Skip provider tests",
    is_flag=True,
    envvar="SKIP_PROVIDER_TESTS",
)
option_use_xdist = click.option(
    "--use-xdist",
    help="Use xdist plugin for pytest",
    is_flag=True,
    envvar="USE_XDIST",
)
option_test_type = click.option(
    "--test-type",
    help="Type of test to run. With Providers, you can specify tests of which providers "
    "should be run: `Providers[airbyte,http]` or "
    "excluded from the full test suite: `Providers[-amazon,google]`",
    default="Default",
    envvar="TEST_TYPE",
    show_default=True,
    type=NotVerifiedBetterChoice(ALLOWED_TEST_TYPE_CHOICES),
)
option_parallel_test_types = click.option(
    "--parallel-test-types",
    help="Space separated list of test types used for testing in parallel",
    default=ALL_CI_SELECTIVE_TEST_TYPES,
    show_default=True,
    envvar="PARALLEL_TEST_TYPES",
    type=NotVerifiedBetterChoice(ALLOWED_PARALLEL_TEST_TYPE_CHOICES),
)
option_excluded_parallel_test_types = click.option(
    "--excluded-parallel-test-types",
    help="Space separated list of test types that will be excluded from parallel tes runs.",
    default="",
    show_default=True,
    envvar="EXCLUDED_PARALLEL_TEST_TYPES",
    type=NotVerifiedBetterChoice(ALLOWED_PARALLEL_TEST_TYPE_CHOICES),
)
option_collect_only = click.option(
    "--collect-only",
    help="Collect tests only, do not run them.",
    is_flag=True,
    envvar="COLLECT_ONLY",
)
option_remove_arm_packages = click.option(
    "--remove-arm-packages",
    help="Removes arm packages from the image to test if ARM collection works",
    is_flag=True,
    envvar="REMOVE_ARM_PACKAGES",
)
option_skip_docker_compose_down = click.option(
    "--skip-docker-compose-down",
    help="Skips running docker-compose down after tests",
    is_flag=True,
    envvar="SKIP_DOCKER_COMPOSE_DOWN",
)
