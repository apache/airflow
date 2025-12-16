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
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH, DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import (
    ALLOWED_BUILD_PROGRESS,
    ALLOWED_INSTALL_MYSQL_CLIENT_TYPES,
    APACHE_AIRFLOW_GITHUB_REPOSITORY,
    DEFAULT_UV_HTTP_TIMEOUT,
    DOCKER_DEFAULT_PLATFORM,
    get_airflow_version,
)
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.platforms import get_normalized_platform


@dataclass
class CommonBuildParams:
    """
    Common build parameters. Those parameters are common parameters for CI And PROD build.
    """

    additional_airflow_extras: str | None = None
    additional_dev_apt_command: str | None = None
    additional_dev_apt_deps: str | None = None
    additional_dev_apt_env: str | None = None
    additional_python_deps: str | None = None
    additional_pip_install_flags: str | None = None
    airflow_branch: str = AIRFLOW_BRANCH
    default_constraints_branch: str = DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    airflow_constraints_location: str | None = None
    builder: str = "autodetect"
    build_progress: str = ALLOWED_BUILD_PROGRESS[0]
    constraints_github_repository: str = APACHE_AIRFLOW_GITHUB_REPOSITORY
    commit_sha: str | None = None
    dev_apt_command: str | None = None
    dev_apt_deps: str | None = None
    disable_airflow_repo_cache: bool = False
    docker_cache: str = "registry"
    docker_host: str | None = os.environ.get("DOCKER_HOST")
    github_actions: str = os.environ.get("GITHUB_ACTIONS", "false")
    github_repository: str = APACHE_AIRFLOW_GITHUB_REPOSITORY
    github_token: str = os.environ.get("GITHUB_TOKEN", "")
    install_mysql_client_type: str = ALLOWED_INSTALL_MYSQL_CLIENT_TYPES[0]
    platform: str = DOCKER_DEFAULT_PLATFORM
    prepare_buildx_cache: bool = False
    python_image: str | None = None
    push: bool = False
    python: str = "3.10"
    uv_http_timeout: int = DEFAULT_UV_HTTP_TIMEOUT
    dry_run: bool = False
    version_suffix: str | None = None
    verbose: bool = False
    debian_version: str = "bookworm"
    build_arg_values: list[str] = field(default_factory=list)

    @property
    def airflow_version(self):
        raise NotImplementedError()

    @property
    def build_id(self) -> str:
        return os.environ.get("CI_BUILD_ID", "0")

    @property
    def image_type(self) -> str:
        raise NotImplementedError()

    @property
    def airflow_base_image_name(self):
        image = f"ghcr.io/{self.github_repository.lower()}"
        return image

    @property
    def airflow_image_name(self):
        """Construct image link"""
        image = (
            f"{self.airflow_base_image_name}/{self.airflow_branch}/"
            f"{self.image_type.lower()}/python{self.python}"
        )
        return image

    @property
    def common_docker_build_flags(self) -> list[str]:
        from airflow_breeze.utils.docker_command_utils import check_container_engine_is_docker

        extra_flags = []
        extra_flags.append(f"--progress={self.build_progress}")
        # Check if we're using Docker or Podman
        is_docker_engine = check_container_engine_is_docker(quiet=True)

        if not is_docker_engine:
            # For Podman, always disable cache to avoid compatibility issues
            extra_flags.append("--no-cache")
        elif self.docker_cache == "registry":
            extra_flags.append(f"--cache-from={self.get_cache(self.platform)}")
        elif self.docker_cache == "disabled":
            extra_flags.append("--no-cache")

        return extra_flags

    @property
    def python_base_image(self):
        """Construct Python Base Image"""
        if self.python_image is not None:
            return self.python_image
        return f"debian:{self.debian_version}-slim"

    @property
    def airflow_image_repository(self):
        return f"https://github.com/{self.github_repository}"

    @property
    def airflow_image_date_created(self):
        now = datetime.now()
        return now.strftime("%Y-%m-%dT%H:%M:%SZ")

    @property
    def airflow_image_readme_url(self):
        return "https://raw.githubusercontent.com/apache/airflow/refs/heads/main/docker-stack-docs/README.md"

    def get_cache(self, single_platform: str) -> str:
        if "," in single_platform:
            get_console().print(
                "[error]Cache can only be retrieved for single platform and you "
                f"tried for {single_platform}[/]"
            )
            sys.exit(1)
        platform_tag = get_normalized_platform(single_platform).replace("/", "-")
        return f"{self.airflow_image_name}:cache-{platform_tag}"

    def is_multi_platform(self) -> bool:
        return "," in self.platform

    @property
    def platforms(self) -> list[str]:
        return [get_normalized_platform(single_platform) for single_platform in self.platform.split(",")]

    def _build_arg(self, name: str, value: Any, optional: bool):
        if value is None or "":
            if optional:
                return
            raise ValueError(f"Value for {name} cannot be empty or None")
        if value is True:
            str_value = "true"
        elif value is False:
            str_value = "false"
        else:
            str_value = str(value) if value is not None else ""
        self.build_arg_values.append(f"{name}={str_value}")

    def _req_arg(self, name: str, value: Any):
        self._build_arg(name, value, False)

    def _opt_arg(self, name: str, value: Any):
        self._build_arg(name, value, True)

    def prepare_arguments_for_docker_build_command(self) -> list[str]:
        raise NotImplementedError()

    def _to_build_args(self):
        build_args = []
        for arg in self.build_arg_values:
            build_args.extend(["--build-arg", arg])
        return build_args

    def _get_version_with_suffix(self) -> str:
        from packaging.version import Version

        airflow_version = get_airflow_version()
        try:
            if self.version_suffix and self.version_suffix not in airflow_version:
                version = Version(airflow_version)
                return version.base_version + f".{self.version_suffix}"
        except Exception:
            # in case of any failure just fall back to the original version set
            pass
        return airflow_version

    def _set_common_opt_args(self):
        self._opt_arg("AIRFLOW_CONSTRAINTS_LOCATION", self.airflow_constraints_location)
        self._opt_arg("ADDITIONAL_AIRFLOW_EXTRAS", self.additional_airflow_extras)
        self._opt_arg("ADDITIONAL_DEV_APT_COMMAND", self.additional_dev_apt_command)
        self._opt_arg("ADDITIONAL_DEV_APT_DEPS", self.additional_dev_apt_deps)
        self._opt_arg("ADDITIONAL_DEV_APT_ENV", self.additional_dev_apt_env)
        self._opt_arg("ADDITIONAL_PIP_INSTALL_FLAGS", self.additional_pip_install_flags)
        self._opt_arg("ADDITIONAL_PYTHON_DEPS", self.additional_python_deps)
        self._opt_arg("COMMIT_SHA", self.commit_sha)
        self._opt_arg("DEV_APT_COMMAND", self.dev_apt_command)
        self._opt_arg("DEV_APT_DEPS", self.dev_apt_deps)
        self._opt_arg("DOCKER_HOST", self.docker_host)
        self._opt_arg("VERSION_SUFFIX", self.version_suffix)

    def _set_common_req_args(self):
        self._req_arg("AIRFLOW_BRANCH", self.airflow_branch)
        self._req_arg("AIRFLOW_IMAGE_DATE_CREATED", self.airflow_image_date_created)
        self._req_arg("AIRFLOW_IMAGE_REPOSITORY", self.airflow_image_repository)
        self._req_arg("BUILD_ID", self.build_id)
        self._req_arg("CONSTRAINTS_GITHUB_REPOSITORY", self.constraints_github_repository)
