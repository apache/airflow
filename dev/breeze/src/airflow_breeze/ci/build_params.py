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
import os
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH, DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import get_airflow_version
from airflow_breeze.utils.run_utils import run_command


@dataclass
class BuildParams:
    # To construct ci_image_name
    upgrade_newer_dependencies: bool = False
    python_version: str = "3.7"
    airflow_branch: str = AIRFLOW_BRANCH
    build_id: int = 0
    # To construct docker cache ci directive
    docker_cache: str = "pulled"
    airflow_extras: str = "devel_ci"
    additional_airflow_extras: str = ""
    additional_python_deps: str = ""
    # To construct ci_image_name
    tag: str = "latest"
    # To construct airflow_image_repository
    github_repository: str = "apache/airflow"
    constraints_github_repository: str = "apache/airflow"
    # Not sure if defaultConstraintsBranch and airflow_constraints_reference are different
    default_constraints_branch: str = DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    airflow_constraints: str = "constraints-source-providers"
    airflow_constraints_reference: Optional[str] = "constraints-main"
    airflow_constraints_location: Optional[str] = ""
    airflow_pre_cached_pip_packages: str = "true"
    dev_apt_command: str = ""
    dev_apt_deps: str = ""
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

    @property
    def airflow_image_name(self):
        image = f'ghcr.io/{self.github_repository.lower()}'
        return image

    @property
    def airflow_ci_image_name(self):
        """Construct CI image link"""
        image = f'{self.airflow_image_name}/{self.airflow_branch}/ci/python{self.python_version}'
        return image

    @property
    def airflow_ci_image_name_with_cache(self):
        """Construct CI image link"""
        image = f'{self.airflow_image_name}/{self.airflow_branch}/ci/python{self.python_version}:cache'
        return image

    @property
    def airflow_ci_image_name_with_tag(self):
        """Construct CI image link"""
        image = f'{self.airflow_image_name}/{self.airflow_branch}/ci/python{self.python_version}'
        return image if not self.tag else image + f":{self.tag}"

    @property
    def airflow_image_repository(self):
        return f'https://github.com/{self.github_repository}'

    @property
    def python_base_image(self):
        """Construct Python Base Image"""
        return f'python:{self.python_version}-slim-{self.debian_version}'

    @property
    def airflow_ci_local_manifest_image(self):
        """Construct CI Local Manifest Image"""
        return f'local-airflow-ci-manifest/{self.airflow_branch}/python{self.python_version}'

    @property
    def airflow_ci_remote_manifest_image(self):
        """Construct CI Remote Manifest Image"""
        return f'{self.airflow_ci_image_name}/{self.airflow_branch}/ci-manifest//python:{self.python_version}'

    @property
    def airflow_image_date_created(self):
        now = datetime.now()
        return now.strftime("%Y-%m-%dT%H:%M:%SZ")

    @property
    def commit_sha(self):
        output = run_command(['git', 'rev-parse', 'HEAD'], capture_output=True, text=True)
        return output.stdout.strip()

    @property
    def docker_cache_ci_directive(self) -> List:
        docker_cache_ci_directive = []
        if self.docker_cache == "pulled":
            docker_cache_ci_directive.append("--cache-from")
            docker_cache_ci_directive.append(self.airflow_ci_image_name_with_cache)
        elif self.docker_cache == "disabled":
            docker_cache_ci_directive.append("--no-cache")
        else:
            pass
        return docker_cache_ci_directive

    @property
    def airflow_version(self):
        return get_airflow_version()

    @property
    def upgrade_to_newer_dependencies(self) -> str:
        upgrade_to_newer_dependencies = 'false'
        if self.upgrade_newer_dependencies:
            upgrade_to_newer_dependencies = 'true'
        return upgrade_to_newer_dependencies
