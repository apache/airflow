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
from pathlib import Path
from typing import List

from airflow_breeze.branch_defaults import DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import get_airflow_version
from airflow_breeze.params.common_build_params import CommonBuildParams
from airflow_breeze.utils.path_utils import BUILD_CACHE_DIR


@dataclass
class BuildCiParams(CommonBuildParams):
    """
    CI build parameters. Those parameters are used to determine command issued to build CI image.
    """

    airflow_constraints_mode: str = "constraints-source-providers"
    default_constraints_branch: str = os.environ.get(
        'DEFAULT_CONSTRAINTS_BRANCH', DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    )
    airflow_constraints_reference: str = ""
    airflow_extras: str = "devel_ci"
    airflow_pre_cached_pip_packages: bool = True
    force_build: bool = False

    @property
    def airflow_version(self):
        return get_airflow_version()

    @property
    def image_type(self) -> str:
        return 'CI'

    @property
    def extra_docker_build_flags(self) -> List[str]:
        extra_ci_flags = []
        extra_ci_flags.extend(
            ["--build-arg", f"AIRFLOW_CONSTRAINTS_REFERENCE={self.airflow_constraints_reference}"]
        )
        if self.airflow_constraints_location is not None and len(self.airflow_constraints_location) > 0:
            extra_ci_flags.extend(
                ["--build-arg", f"AIRFLOW_CONSTRAINTS_LOCATION={self.airflow_constraints_location}"]
            )
        return extra_ci_flags

    @property
    def md5sum_cache_dir(self) -> Path:
        return Path(BUILD_CACHE_DIR, self.airflow_branch, self.python, "CI")

    @property
    def required_image_args(self) -> List[str]:
        return [
            "additional_airflow_extras",
            "additional_dev_apt_command",
            "additional_dev_apt_deps",
            "additional_dev_apt_env",
            "additional_pip_install_flags",
            "additional_python_deps",
            "additional_runtime_apt_command",
            "additional_runtime_apt_deps",
            "additional_runtime_apt_env",
            "airflow_branch",
            "airflow_constraints_mode",
            "airflow_constraints_reference",
            "airflow_extras",
            "airflow_image_date_created",
            "airflow_image_repository",
            "airflow_pre_cached_pip_packages",
            "airflow_version",
            "build_id",
            "constraints_github_repository",
            "python_base_image",
            "upgrade_to_newer_dependencies",
        ]

    @property
    def optional_image_args(self) -> List[str]:
        return [
            "dev_apt_command",
            "dev_apt_deps",
            "runtime_apt_command",
            "runtime_apt_deps",
        ]

    def __post_init__(self):
        pass
