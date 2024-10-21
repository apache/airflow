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

import click

from airflow_breeze.branch_defaults import DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import (
    ALLOWED_BUILD_CACHE,
    ALLOWED_BUILD_PROGRESS,
    ALLOWED_DEBIAN_VERSIONS,
    ALLOWED_INSTALL_MYSQL_CLIENT_TYPES,
    ALLOWED_PLATFORMS,
    DOCKER_DEFAULT_PLATFORM,
)
from airflow_breeze.utils.custom_param_types import BetterChoice
from airflow_breeze.utils.recording import generating_command_images

option_additional_dev_apt_command = click.option(
    "--additional-dev-apt-command",
    help="Additional command executed before dev apt deps are installed.",
    envvar="ADDITIONAL_DEV_APT_COMMAND",
)
option_additional_dev_apt_deps = click.option(
    "--additional-dev-apt-deps",
    help="Additional apt dev dependencies to use when building the images.",
    envvar="ADDITIONAL_DEV_APT_DEPS",
)
option_additional_dev_apt_env = click.option(
    "--additional-dev-apt-env",
    help="Additional environment variables set when adding dev dependencies.",
    envvar="ADDITIONAL_DEV_APT_ENV",
)
option_additional_airflow_extras = click.option(
    "--additional-airflow-extras",
    help="Additional extra package while installing Airflow in the image.",
    envvar="ADDITIONAL_AIRFLOW_EXTRAS",
)
option_additional_python_deps = click.option(
    "--additional-python-deps",
    help="Additional python dependencies to use when building the images.",
    envvar="ADDITIONAL_PYTHON_DEPS",
)
option_additional_pip_install_flags = click.option(
    "--additional-pip-install-flags",
    help="Additional flags added to `pip install` commands (except reinstalling `pip` itself).",
    envvar="ADDITIONAL_PIP_INSTALL_FLAGS",
)
option_additional_runtime_apt_command = click.option(
    "--additional-runtime-apt-command",
    help="Additional command executed before runtime apt deps are installed.",
    envvar="ADDITIONAL_RUNTIME_APT_COMMAND",
)
option_additional_runtime_apt_deps = click.option(
    "--additional-runtime-apt-deps",
    help="Additional apt runtime dependencies to use when building the images.",
    envvar="ADDITIONAL_RUNTIME_APT_DEPS",
)
option_additional_runtime_apt_env = click.option(
    "--additional-runtime-apt-env",
    help="Additional environment variables set when adding runtime dependencies.",
    envvar="ADDITIONAL_RUNTIME_APT_ENV",
)
option_airflow_constraints_reference_build = click.option(
    "--airflow-constraints-reference",
    default=DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH,
    help="Constraint reference to use when building the image.",
    envvar="AIRFLOW_CONSTRAINTS_REFERENCE",
)
option_build_progress = click.option(
    "--build-progress",
    help="Build progress.",
    type=BetterChoice(ALLOWED_BUILD_PROGRESS),
    envvar="BUILD_PROGRESS",
    show_default=True,
    default=ALLOWED_BUILD_PROGRESS[0],
)
option_debian_version = click.option(
    "--debian-version",
    type=BetterChoice(ALLOWED_DEBIAN_VERSIONS),
    default=ALLOWED_DEBIAN_VERSIONS[0],
    show_default=True,
    help="Debian version used in Airflow image as base for building images.",
    envvar="DEBIAN_VERSION",
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
option_disable_airflow_repo_cache = click.option(
    "--disable-airflow-repo-cache",
    help="Disable cache from Airflow repository during building.",
    is_flag=True,
    envvar="DISABLE_AIRFLOW_REPO_CACHE",
)
option_docker_cache = click.option(
    "-c",
    "--docker-cache",
    help="Cache option for image used during the build.",
    default=ALLOWED_BUILD_CACHE[0],
    show_default=True,
    type=BetterChoice(ALLOWED_BUILD_CACHE),
    envvar="DOCKER_CACHE",
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
option_image_tag_for_verifying = click.option(
    "-t",
    "--image-tag",
    help="Tag of the image when verifying it.",
    show_default=True,
    default="latest",
    envvar="IMAGE_TAG",
)
option_install_mysql_client_type = click.option(
    "--install-mysql-client-type",
    help="Which client to choose when installing.",
    type=BetterChoice(ALLOWED_INSTALL_MYSQL_CLIENT_TYPES),
    default=ALLOWED_INSTALL_MYSQL_CLIENT_TYPES[0],
    envvar="INSTALL_MYSQL_CLIENT_TYPE",
)
option_platform_multiple = click.option(
    "--platform",
    help="Platform for Airflow image.",
    default=DOCKER_DEFAULT_PLATFORM if not generating_command_images() else ALLOWED_PLATFORMS[0],
    envvar="PLATFORM",
    type=BetterChoice(ALLOWED_PLATFORMS),
)
option_prepare_buildx_cache = click.option(
    "--prepare-buildx-cache",
    help="Prepares build cache (this is done as separate per-platform steps instead of building the image).",
    is_flag=True,
    envvar="PREPARE_BUILDX_CACHE",
)
option_pull = click.option(
    "--pull",
    help="Pull image is missing before attempting to verify it.",
    is_flag=True,
    envvar="PULL",
)
option_push = click.option(
    "--push",
    help="Push image after building it.",
    is_flag=True,
    envvar="PUSH",
)
option_python_image = click.option(
    "--python-image",
    help="If specified this is the base python image used to build the image. "
    "Should be something like: python:VERSION-slim-bookworm.",
    envvar="PYTHON_IMAGE",
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
option_wait_for_image = click.option(
    "--wait-for-image",
    help="Wait until image is available.",
    is_flag=True,
    envvar="WAIT_FOR_IMAGE",
)
