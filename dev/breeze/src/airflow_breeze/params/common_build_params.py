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
from airflow_breeze.utils.platforms import get_real_platform


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
    image_tag: str | None = None
    install_mysql_client_type: str = ALLOWED_INSTALL_MYSQL_CLIENT_TYPES[0]
    platform: str = DOCKER_DEFAULT_PLATFORM
    prepare_buildx_cache: bool = False
    python_image: str | None = None
    push: bool = False
    python: str = "3.9"
    tag_as_latest: bool = False
    uv_http_timeout: int = DEFAULT_UV_HTTP_TIMEOUT
    dry_run: bool = False
    version_suffix_for_pypi: str | None = None
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
    def airflow_pre_cached_pip_packages(self) -> str:
        return "false" if self.disable_airflow_repo_cache else "true"

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
        extra_flags = []
        extra_flags.append(f"--progress={self.build_progress}")
        if self.docker_cache == "registry":
            extra_flags.append(f"--cache-from={self.get_cache(self.platform)}")
        elif self.docker_cache == "disabled":
            extra_flags.append("--no-cache")
        return extra_flags

    @property
    def python_base_image(self):
        """Construct Python Base Image"""
        if self.python_image is not None:
            return self.python_image
        return f"python:{self.python}-slim-{self.debian_version}"

    @property
    def airflow_image_repository(self):
        return f"https://github.com/{self.github_repository}"

    @property
    def airflow_image_date_created(self):
        now = datetime.now()
        return now.strftime("%Y-%m-%dT%H:%M:%SZ")

    @property
    def airflow_image_readme_url(self):
        return "https://raw.githubusercontent.com/apache/airflow/main/docs/docker-stack/README.md"

    @property
    def airflow_image_name_with_tag(self):
        """Construct image link"""
        image = (
            f"{self.airflow_base_image_name}/{self.airflow_branch}/"
            f"{self.image_type.lower()}/python{self.python}"
        )
        return image if self.image_tag is None else image + f":{self.image_tag}"

    def get_cache(self, single_platform: str) -> str:
        if "," in single_platform:
            get_console().print(
                "[error]Cache can only be retrieved for single platform and you "
                f"tried for {single_platform}[/]"
            )
            sys.exit(1)
        return f"{self.airflow_image_name}:cache-{get_real_platform(single_platform)}"

    def is_multi_platform(self) -> bool:
        return "," in self.platform

    def preparing_latest_image(self) -> bool:
        return (
            self.tag_as_latest
            or self.airflow_image_name == self.airflow_image_name_with_tag
            or self.airflow_image_name_with_tag.endswith("latest")
        )

    @property
    def platforms(self) -> list[str]:
        return self.platform.split(",")

    def _build_arg(self, name: str, value: Any, optional: bool):
        if value is None or "":
            if optional:
                return
            else:
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
            if self.version_suffix_for_pypi and self.version_suffix_for_pypi not in airflow_version:
                version = Version(airflow_version)
                return version.base_version + f".{self.version_suffix_for_pypi}"
        except Exception:
            # in case of any failure just fall back to the original version set
            pass
        return airflow_version
