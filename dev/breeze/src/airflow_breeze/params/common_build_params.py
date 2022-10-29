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
from dataclasses import dataclass
from datetime import datetime

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH, DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import APACHE_AIRFLOW_GITHUB_REPOSITORY, DOCKER_DEFAULT_PLATFORM
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.platforms import get_real_platform


@dataclass
class CommonBuildParams:
    """
    Common build parameters. Those parameters are common parameters for CI And PROD build.
    """

    additional_airflow_extras: str = ""
    additional_dev_apt_command: str = ""
    additional_dev_apt_deps: str = ""
    additional_dev_apt_env: str = ""
    additional_python_deps: str = ""
    additional_pip_install_flags: str = ""
    airflow_branch: str = os.environ.get("DEFAULT_BRANCH", AIRFLOW_BRANCH)
    default_constraints_branch: str = os.environ.get(
        "DEFAULT_CONSTRAINTS_BRANCH", DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    )
    airflow_constraints_location: str = ""
    build_id: int = 0
    builder: str = "default"
    constraints_github_repository: str = APACHE_AIRFLOW_GITHUB_REPOSITORY
    dev_apt_command: str = ""
    dev_apt_deps: str = ""
    docker_cache: str = "registry"
    empty_image: bool = False
    github_actions: str = os.environ.get("GITHUB_ACTIONS", "false")
    github_repository: str = APACHE_AIRFLOW_GITHUB_REPOSITORY
    github_token: str = os.environ.get("GITHUB_TOKEN", "")
    github_username: str = ""
    image_tag: str | None = None
    install_providers_from_sources: bool = False
    platform: str = DOCKER_DEFAULT_PLATFORM
    prepare_buildx_cache: bool = False
    python_image: str | None = None
    push: bool = False
    python: str = "3.7"
    tag_as_latest: bool = False
    upgrade_to_newer_dependencies: bool = False
    upgrade_on_failure: bool = False
    dry_run: bool = False
    verbose: bool = False

    @property
    def airflow_version(self):
        raise NotImplementedError()

    @property
    def image_type(self) -> str:
        raise NotImplementedError()

    @property
    def airflow_pre_cached_pip_packages(self):
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
    def extra_docker_build_flags(self) -> list[str]:
        raise NotImplementedError()

    @property
    def docker_cache_directive(self) -> list[str]:
        docker_cache_directive = []
        if self.docker_cache == "registry":
            for platform in self.platforms:
                docker_cache_directive.append(f"--cache-from={self.get_cache(platform)}")
        elif self.docker_cache == "disabled":
            docker_cache_directive.append("--no-cache")
        else:
            docker_cache_directive = []
        return docker_cache_directive

    @property
    def python_base_image(self):
        """Construct Python Base Image"""
        if self.python_image is not None:
            return self.python_image
        return f"python:{self.python}-slim-bullseye"

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

    @property
    def required_image_args(self) -> list[str]:
        raise NotImplementedError()

    @property
    def optional_image_args(self) -> list[str]:
        raise NotImplementedError()

    def __post_init__(self):
        pass
