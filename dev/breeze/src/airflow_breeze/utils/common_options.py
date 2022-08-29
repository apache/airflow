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

import multiprocessing as mp

import click

from airflow_breeze.branch_defaults import DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import (
    ALLOWED_BACKENDS,
    ALLOWED_BUILD_CACHE,
    ALLOWED_CONSTRAINTS_MODES_CI,
    ALLOWED_CONSTRAINTS_MODES_PROD,
    ALLOWED_INSTALLATION_PACKAGE_FORMATS,
    ALLOWED_INTEGRATIONS,
    ALLOWED_MOUNT_OPTIONS,
    ALLOWED_MSSQL_VERSIONS,
    ALLOWED_MYSQL_VERSIONS,
    ALLOWED_PACKAGE_FORMATS,
    ALLOWED_PLATFORMS,
    ALLOWED_POSTGRES_VERSIONS,
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    ALLOWED_USE_AIRFLOW_VERSIONS,
    SINGLE_PLATFORMS,
    get_available_packages,
)
from airflow_breeze.utils.custom_param_types import (
    AnswerChoice,
    BetterChoice,
    CacheableChoice,
    CacheableDefault,
    UseAirflowVersionType,
)
from airflow_breeze.utils.recording import output_file_for_recording

option_verbose = click.option(
    "-v", "--verbose", is_flag=True, help="Print verbose information about performed steps.", envvar='VERBOSE'
)
option_dry_run = click.option(
    "-D",
    "--dry-run",
    is_flag=True,
    help="If dry-run is set, commands are only printed, not executed.",
    envvar='DRY_RUN',
)
option_answer = click.option(
    "-a",
    "--answer",
    type=AnswerChoice(['y', 'n', 'q', 'yes', 'no', 'quit']),
    help="Force answer to questions.",
    envvar='ANSWER',
)
option_github_repository = click.option(
    '-g',
    '--github-repository',
    help='GitHub repository used to pull, push run images.',
    default="apache/airflow",
    show_default=True,
    envvar='GITHUB_REPOSITORY',
)
option_python = click.option(
    '-p',
    '--python',
    type=CacheableChoice(ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS),
    default=CacheableDefault(value=ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS[0]),
    show_default=True,
    help='Python major/minor version used in Airflow image for images.',
    envvar='PYTHON_MAJOR_MINOR_VERSION',
)
option_backend = click.option(
    '-b',
    '--backend',
    type=CacheableChoice(ALLOWED_BACKENDS),
    default=CacheableDefault(value=ALLOWED_BACKENDS[0]),
    show_default=True,
    help="Database backend to use.",
    envvar='BACKEND',
)
option_integration = click.option(
    '--integration',
    help="Integration(s) to enable when running (can be more than one).",
    type=BetterChoice(ALLOWED_INTEGRATIONS),
    multiple=True,
)
option_postgres_version = click.option(
    '-P',
    '--postgres-version',
    type=CacheableChoice(ALLOWED_POSTGRES_VERSIONS),
    default=CacheableDefault(ALLOWED_POSTGRES_VERSIONS[0]),
    show_default=True,
    help="Version of Postgres used.",
)
option_mysql_version = click.option(
    '-M',
    '--mysql-version',
    help="Version of MySQL used.",
    type=CacheableChoice(ALLOWED_MYSQL_VERSIONS),
    default=CacheableDefault(ALLOWED_MYSQL_VERSIONS[0]),
    show_default=True,
)
option_mssql_version = click.option(
    '-S',
    '--mssql-version',
    help="Version of MsSQL used.",
    type=CacheableChoice(ALLOWED_MSSQL_VERSIONS),
    default=CacheableDefault(ALLOWED_MSSQL_VERSIONS[0]),
    show_default=True,
)
option_forward_credentials = click.option(
    '-f', '--forward-credentials', help="Forward local credentials to container when running.", is_flag=True
)
option_use_airflow_version = click.option(
    '--use-airflow-version',
    help="Use (reinstall at entry) Airflow version from PyPI. It can also be `none`, `wheel`, or `sdist`"
    " if Airflow should be removed, installed from wheel packages or sdist packages available in dist "
    "folder respectively. Implies --mount-sources `remove`.",
    type=UseAirflowVersionType(ALLOWED_USE_AIRFLOW_VERSIONS),
    envvar='USE_AIRFLOW_VERSION',
)
option_airflow_extras = click.option(
    '--airflow-extras',
    help="Airflow extras to install when --use-airflow-version is used",
    default="",
    show_default=True,
    envvar='AIRFLOW_EXTRAS',
)
option_mount_sources = click.option(
    '--mount-sources',
    type=BetterChoice(ALLOWED_MOUNT_OPTIONS),
    default=ALLOWED_MOUNT_OPTIONS[0],
    show_default=True,
    help="Choose scope of local sources that should be mounted, skipped, or removed (default = selected).",
)
option_force_build = click.option(
    '--force-build', help="Force image build no matter if it is determined as needed.", is_flag=True
)
option_db_reset = click.option(
    '-d',
    '--db-reset',
    help="Reset DB when entering the container.",
    is_flag=True,
    envvar='DB_RESET',
)
option_use_packages_from_dist = click.option(
    '--use-packages-from-dist',
    is_flag=True,
    help="Install all found packages (--package-format determines type) from 'dist' folder "
    "when entering breeze.",
    envvar='USE_PACKAGES_FROM_DIST',
)
option_docker_cache = click.option(
    '-c',
    '--docker-cache',
    help='Cache option for image used during the build.',
    default=ALLOWED_BUILD_CACHE[0],
    show_default=True,
    type=BetterChoice(ALLOWED_BUILD_CACHE),
)
option_github_token = click.option(
    '--github-token',
    help='The token used to authenticate to GitHub.',
    envvar='GITHUB_TOKEN',
)
option_github_username = click.option(
    '--github-username',
    help='The user name used to authenticate to GitHub.',
    envvar='GITHUB_USERNAME',
)
option_image_tag_for_pulling = click.option(
    '-t',
    '--image-tag',
    help='Tag of the image which is used to pull the image',
    envvar='IMAGE_TAG',
    required=True,
)
option_image_tag_for_building = click.option(
    '-t',
    '--image-tag',
    help='Tag the image after building it',
    envvar='IMAGE_TAG',
)
option_image_tag_for_running = click.option(
    '-t',
    '--image-tag',
    help='Tag of the image which is used to run the image (implies --mount-sources=skip)',
    envvar='IMAGE_TAG',
)
option_image_tag_for_verifying = click.option(
    '-t',
    '--image-tag',
    help='Tag of the image when verifying it',
    envvar='IMAGE_TAG',
)
option_image_name = click.option(
    '-n', '--image-name', help='Name of the image to verify (overrides --python and --image-tag).'
)
option_platform_multiple = click.option(
    '--platform',
    help='Platform for Airflow image.',
    envvar='PLATFORM',
    type=BetterChoice(ALLOWED_PLATFORMS),
)
option_platform_single = click.option(
    '--platform',
    help='Platform for Airflow image.',
    envvar='PLATFORM',
    type=BetterChoice(SINGLE_PLATFORMS),
)
option_upgrade_to_newer_dependencies = click.option(
    "-u",
    '--upgrade-to-newer-dependencies',
    is_flag=True,
    help='When set, upgrade all PIP packages to latest.',
    envvar='UPGRADE_TO_NEWER_DEPENDENCIES',
)
option_additional_extras = click.option(
    '--additional-extras',
    help='Additional extra package while installing Airflow in the image.',
    envvar='ADDITIONAL_AIRFLOW_EXTRAS',
)
option_additional_dev_apt_deps = click.option(
    '--additional-dev-apt-deps',
    help='Additional apt dev dependencies to use when building the images.',
    envvar='ADDITIONAL_DEV_APT_DEPS',
)
option_additional_runtime_apt_deps = click.option(
    '--additional-runtime-apt-deps',
    help='Additional apt runtime dependencies to use when building the images.',
    envvar='ADDITIONAL_RUNTIME_APT_DEPS',
)
option_additional_python_deps = click.option(
    '--additional-python-deps',
    help='Additional python dependencies to use when building the images.',
    envvar='ADDITIONAL_PYTHON_DEPS',
)
option_additional_dev_apt_command = click.option(
    '--additional-dev-apt-command',
    help='Additional command executed before dev apt deps are installed.',
    envvar='ADDITIONAL_DEV_APT_COMMAND',
)
option_additional_runtime_apt_command = click.option(
    '--additional-runtime-apt-command',
    help='Additional command executed before runtime apt deps are installed.',
    envvar='ADDITIONAL_RUNTIME_APT_COMMAND',
)
option_additional_dev_apt_env = click.option(
    '--additional-dev-apt-env',
    help='Additional environment variables set when adding dev dependencies.',
    envvar='ADDITIONAL_DEV_APT_ENV',
)
option_additional_runtime_apt_env = click.option(
    '--additional-runtime-apt-env',
    help='Additional environment variables set when adding runtime dependencies.',
    envvar='ADDITIONAL_RUNTIME_APT_ENV',
)
option_dev_apt_command = click.option(
    '--dev-apt-command',
    help='Command executed before dev apt deps are installed.',
    envvar='DEV_APT_COMMAND',
)
option_dev_apt_deps = click.option(
    '--dev-apt-deps',
    help='Apt dev dependencies to use when building the images.',
    envvar='DEV_APT_DEPS',
)
option_runtime_apt_command = click.option(
    '--runtime-apt-command',
    help='Command executed before runtime apt deps are installed.',
    envvar='RUNTIME_APT_COMMAND',
)
option_runtime_apt_deps = click.option(
    '--runtime-apt-deps',
    help='Apt runtime dependencies to use when building the images.',
    envvar='RUNTIME_APT_DEPS',
)
option_prepare_buildx_cache = click.option(
    '--prepare-buildx-cache',
    help='Prepares build cache (this is done as separate per-platform steps instead of building the image).',
    is_flag=True,
    envvar='PREPARE_BUILDX_CACHE',
)
option_push = click.option(
    '--push',
    help='Push image after building it.',
    is_flag=True,
    envvar='PUSH',
)
option_empty_image = click.option(
    '--empty-image',
    help='Prepare empty image tagged with the same name as the Airflow image.',
    is_flag=True,
    envvar='EMPTY_IMAGE',
)
option_wait_for_image = click.option(
    '--wait-for-image',
    help='Wait until image is available.',
    is_flag=True,
    envvar='WAIT_FOR_IMAGE',
)
option_tag_as_latest = click.option(
    '--tag-as-latest',
    help='Tags the image as latest and update checksum of all files after pulling. '
    'Useful when you build or pull image with --image-tag.',
    is_flag=True,
    envvar='TAG_AS_LATEST',
)
option_verify = click.option(
    '--verify',
    help='Verify image.',
    is_flag=True,
    envvar='VERIFY',
)
option_additional_pip_install_flags = click.option(
    '--additional-pip-install-flags',
    help='Additional flags added to `pip install` commands (except reinstalling `pip` itself).',
    envvar='ADDITIONAL_PIP_INSTALL_FLAGS',
)

option_install_providers_from_sources = click.option(
    '--install-providers-from-sources',
    help="Install providers from sources when installing.",
    is_flag=True,
    envvar='INSTALL_PROVIDERS_FROM_SOURCES',
)
option_load_example_dags = click.option(
    '-e',
    '--load-example-dags',
    help="Enable configuration to load example DAGs when starting Airflow.",
    is_flag=True,
    envvar='LOAD_EXAMPLES',
)
option_load_default_connection = click.option(
    '-c',
    '--load-default-connections',
    help="Enable configuration to load default connections when starting Airflow.",
    is_flag=True,
    envvar='LOAD_DEFAULT_CONNECTIONS',
)
option_version_suffix_for_pypi = click.option(
    '--version-suffix-for-pypi',
    help='Version suffix used for PyPI packages (alpha, beta, rc1, etc.).',
    default="",
    envvar='VERSION_SUFFIX_FOR_PYPI',
)
option_package_format = click.option(
    '--package-format',
    type=BetterChoice(ALLOWED_PACKAGE_FORMATS),
    help='Format of packages.',
    default=ALLOWED_PACKAGE_FORMATS[0],
    show_default=True,
    envvar='PACKAGE_FORMAT',
)
option_installation_package_format = click.option(
    '--package-format',
    type=BetterChoice(ALLOWED_INSTALLATION_PACKAGE_FORMATS),
    help='Format of packages that should be installed from dist.',
    default=ALLOWED_INSTALLATION_PACKAGE_FORMATS[0],
    show_default=True,
    envvar='PACKAGE_FORMAT',
)
option_python_versions = click.option(
    '--python-versions',
    help="Space separated list of python versions used for build with multiple versions.",
    default=" ".join(ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS),
    show_default=True,
    envvar="PYTHON_VERSIONS",
)
option_run_in_parallel = click.option(
    '--run-in-parallel',
    help="Run the operation in parallel on all or selected subset of Python versions.",
    is_flag=True,
    envvar='RUN_IN_PARALLEL',
)
option_parallelism = click.option(
    '--parallelism',
    help="Maximum number of processes to use while running the operation in parallel.",
    type=click.IntRange(1, mp.cpu_count() * 2 if not output_file_for_recording else 8),
    default=mp.cpu_count() if not output_file_for_recording else 4,
    envvar='PARALLELISM',
    show_default=True,
)
argument_packages = click.argument(
    "packages",
    nargs=-1,
    required=False,
    type=BetterChoice(get_available_packages(short_version=True)),
)
option_timezone = click.option(
    "--timezone",
    default="UTC",
    type=str,
    help="Timezone to use during the check",
)
option_updated_on_or_after = click.option(
    "--updated-on-or-after",
    type=str,
    help="Date when the release was updated after",
)
option_max_age = click.option(
    "--max-age",
    type=int,
    default=3,
    help="Max age of the last release (used if no updated-on-or-after if specified)",
)
option_airflow_constraints_reference = click.option(
    "--airflow-constraints-reference",
    help="Constraint reference to use. Useful with --use-airflow-version parameter to specify "
    "constraints for the installed version and to find newer dependencies",
    default=DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH,
    envvar='AIRFLOW_CONSTRAINTS_REFERENCE',
)
option_airflow_constraints_reference_build = click.option(
    "--airflow-constraints-reference",
    default=DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH,
    help="Constraint reference to use when building the image.",
    envvar='AIRFLOW_CONSTRAINTS_REFERENCE',
)

option_airflow_constraints_mode_ci = click.option(
    '--airflow-constraints-mode',
    type=BetterChoice(ALLOWED_CONSTRAINTS_MODES_CI),
    default=ALLOWED_CONSTRAINTS_MODES_CI[0],
    show_default=True,
    help='Mode of constraints for CI image building',
)
option_airflow_constraints_mode_prod = click.option(
    '--airflow-constraints-mode',
    type=BetterChoice(ALLOWED_CONSTRAINTS_MODES_PROD),
    default=ALLOWED_CONSTRAINTS_MODES_PROD[0],
    show_default=True,
    help='Mode of constraints for PROD image building',
)
option_pull = click.option(
    '--pull',
    help="Pull image is missing before attempting to verify it.",
    is_flag=True,
    envvar='PULL',
)
option_python_image = click.option(
    '--python-image',
    help="If specified this is the base python image used to build the image. "
    "Should be something like: python:VERSION-slim-bullseye",
    envvar='PYTHON_IMAGE',
)
option_builder = click.option(
    '--builder',
    help="Buildx builder used to perform `docker buildx build` commands",
    envvar='BUILDER',
    default='default',
)
option_include_success_outputs = click.option(
    '--include-success-outputs',
    help="Whether to include outputs of successful parallel runs (by default they are not printed).",
    is_flag=True,
    envvar='INCLUDE_SUCCESS_OUTPUTS',
)
option_skip_cleanup = click.option(
    '--skip-cleanup',
    help="Skip cleanup of temporary files created during parallel run",
    is_flag=True,
    envvar='SKIP_CLEANUP',
)
option_include_mypy_volume = click.option(
    '--include-mypy-volume',
    help="Whether to include mounting of the mypy volume (useful for debugging mypy).",
    is_flag=True,
    envvar='INCLUDE_MYPY_VOLUME',
)
