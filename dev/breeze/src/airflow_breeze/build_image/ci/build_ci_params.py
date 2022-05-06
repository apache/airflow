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
"""Parameters for Build CI Image."""
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH, DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import get_airflow_version
from airflow_breeze.utils.path_utils import BUILD_CACHE_DIR


@dataclass
class BuildCiParams:
    """
    CI build parameters. Those parameters are used to determine command issued to build CI image.
    """

    upgrade_to_newer_dependencies: bool = False
    python: str = "3.7"
    airflow_branch: str = AIRFLOW_BRANCH
    build_id: int = 0
    docker_cache: str = "pulled"
    airflow_extras: str = "devel_ci"
    install_providers_from_sources: bool = True
    additional_airflow_extras: str = ""
    additional_python_deps: str = ""
    github_repository: str = "apache/airflow"
    constraints_github_repository: str = "apache/airflow"
    default_constraints_branch: str = DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    airflow_constraints: str = "constraints-source-providers"
    airflow_constraints_reference: Optional[str] = "constraints-main"
    airflow_constraints_location: Optional[str] = ""
    airflow_pre_cached_pip_packages: str = "true"
    login_to_github_registry: str = "false"
    github_username: str = ""
    dev_apt_command: str = ""
    dev_apt_deps: str = ""
    image_tag: Optional[str] = None
    github_token: str = os.environ.get('GITHUB_TOKEN', "")
    github_actions: str = os.environ.get('GITHUB_ACTIONS', "false")
    additional_dev_apt_command: str = ""
    additional_dev_apt_deps: str = ""
    additional_dev_apt_env: str = ""
    runtime_apt_command: str = ""
    runtime_apt_deps: str = ""
    additional_runtime_apt_command: str = ""
    additional_runtime_apt_deps: str = ""
    additional_runtime_apt_env: str = ""
    platform: str = f"linux/{os.uname().machine}"
    debian_version: str = "bullseye"
    prepare_buildx_cache: bool = False
    push_image: bool = False
    empty_image: bool = False
    force_build: bool = False
    skip_rebuild_check: bool = False
    answer: Optional[str] = None

    @property
    def the_image_type(self) -> str:
        return 'CI'

    @property
    def airflow_base_image_name(self):
        image = f'ghcr.io/{self.github_repository.lower()}'
        return image

    @property
    def airflow_image_name(self):
        """Construct CI image link"""
        image = f'{self.airflow_base_image_name}/{self.airflow_branch}/ci/python{self.python}'
        return image

    @property
    def airflow_image_name_with_tag(self):
        """Construct CI image link"""
        image = f'{self.airflow_base_image_name}/{self.airflow_branch}/ci/python{self.python}'
        return image if self.image_tag is None else image + f":{self.image_tag}"

    @property
    def airflow_image_repository(self):
        return f'https://github.com/{self.github_repository}'

    @property
    def python_base_image(self):
        """Construct Python Base Image"""
        return f'python:{self.python}-slim-{self.debian_version}'

    @property
    def airflow_ci_local_manifest_image(self):
        """Construct CI Local Manifest Image"""
        return f'local-airflow-ci-manifest/{self.airflow_branch}/python{self.python}'

    @property
    def airflow_ci_remote_manifest_image(self):
        """Construct CI Remote Manifest Image"""
        return f'{self.airflow_image_name}/{self.airflow_branch}/ci-manifest//python:{self.python}'

    @property
    def airflow_image_date_created(self):
        now = datetime.now()
        return now.strftime("%Y-%m-%dT%H:%M:%SZ")

    @property
    def airflow_version(self):
        return get_airflow_version()

    @property
    def docker_cache_directive(self) -> List[str]:
        docker_cache_directive = []

        if self.docker_cache == "pulled":
            docker_cache_directive.append(f"--cache-from={self.airflow_image_name}")
        elif self.docker_cache == "disabled":
            docker_cache_directive.append("--no-cache")
        else:
            docker_cache_directive = []
        if self.prepare_buildx_cache:
            docker_cache_directive.extend(["--cache-to=type=inline,mode=max", "--push"])
        return docker_cache_directive

    @property
    def extra_docker_build_flags(self) -> List[str]:
        extra_ci_flags = []
        if self.airflow_constraints_location is not None and len(self.airflow_constraints_location) > 0:
            extra_ci_flags.extend(
                ["--build-arg", f"AIRFLOW_CONSTRAINTS_LOCATION={self.airflow_constraints_location}"]
            )
        return extra_ci_flags

    @property
    def md5sum_cache_dir(self) -> Path:
        return Path(BUILD_CACHE_DIR, self.airflow_branch, self.python, "CI")


REQUIRED_CI_IMAGE_ARGS = [
    "python_base_image",
    "airflow_version",
    "airflow_branch",
    "airflow_extras",
    "airflow_pre_cached_pip_packages",
    "additional_airflow_extras",
    "additional_python_deps",
    "additional_dev_apt_command",
    "additional_dev_apt_deps",
    "additional_dev_apt_env",
    "additional_runtime_apt_command",
    "additional_runtime_apt_deps",
    "additional_runtime_apt_env",
    "upgrade_to_newer_dependencies",
    "constraints_github_repository",
    "airflow_constraints_reference",
    "airflow_constraints",
    "airflow_image_repository",
    "airflow_image_date_created",
    "build_id",
]
OPTIONAL_CI_IMAGE_ARGS = [
    "dev_apt_command",
    "dev_apt_deps",
    "runtime_apt_command",
    "runtime_apt_deps",
]
