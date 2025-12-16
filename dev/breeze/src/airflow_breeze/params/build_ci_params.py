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

import random
from dataclasses import dataclass
from pathlib import Path

from airflow_breeze.branch_defaults import DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.params.common_build_params import CommonBuildParams
from airflow_breeze.utils.path_utils import BUILD_CACHE_DIR


@dataclass
class BuildCiParams(CommonBuildParams):
    """
    CI build parameters. Those parameters are used to determine command issued to build CI image.
    """

    airflow_constraints_mode: str = "constraints-source-providers"
    airflow_constraints_reference: str = DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    airflow_extras: str = "devel-ci"
    airflow_pre_cached_pip_packages: bool = True
    force_build: bool = False
    upgrade_to_newer_dependencies: bool = False
    upgrade_on_failure: bool = False
    eager_upgrade_additional_requirements: str | None = None
    skip_provider_dependencies_check: bool = False
    skip_image_upgrade_check: bool = False
    use_uv: bool = True
    warn_image_upgrade_needed: bool = False

    @property
    def airflow_version(self):
        return self._get_version_with_suffix()

    @property
    def image_type(self) -> str:
        return "CI"

    @property
    def md5sum_cache_dir(self) -> Path:
        return Path(BUILD_CACHE_DIR, self.airflow_branch, self.python, "CI")

    def prepare_arguments_for_docker_build_command(self) -> list[str]:
        self.build_arg_values: list[str] = []
        # Required build args
        self._req_arg("AIRFLOW_BRANCH", self.airflow_branch)
        self._req_arg("AIRFLOW_REPO", self.github_repository)
        self._req_arg("AIRFLOW_CONSTRAINTS_MODE", self.airflow_constraints_mode)
        self._req_arg("AIRFLOW_CONSTRAINTS_REFERENCE", self.airflow_constraints_reference)
        self._req_arg("AIRFLOW_EXTRAS", self.airflow_extras)
        self._req_arg("AIRFLOW_IMAGE_DATE_CREATED", self.airflow_image_date_created)
        self._req_arg("AIRFLOW_IMAGE_REPOSITORY", self.airflow_image_repository)
        self._req_arg("AIRFLOW_PRE_CACHED_PIP_PACKAGES", self.airflow_pre_cached_pip_packages)
        self._req_arg("AIRFLOW_USE_UV", self.use_uv)
        if self.use_uv:
            from airflow_breeze.utils.uv_utils import get_uv_timeout

            self._opt_arg("UV_HTTP_TIMEOUT", get_uv_timeout(self))
        self._req_arg("AIRFLOW_VERSION", self.airflow_version)
        self._req_arg("BUILD_ID", self.build_id)
        self._req_arg("CONSTRAINTS_GITHUB_REPOSITORY", self.constraints_github_repository)
        self._req_arg("PYTHON_BASE_IMAGE", self.python_base_image)
        if self.upgrade_to_newer_dependencies:
            self._opt_arg("UPGRADE_INVALIDATION_STRING", f"{random.randrange(2**32):x}")
            if self.eager_upgrade_additional_requirements:
                # in case eager upgrade additional requirements have EOL, connect them together
                self._opt_arg(
                    "EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS",
                    self.eager_upgrade_additional_requirements.replace("\n", ""),
                )
        # optional build args
        self._opt_arg("AIRFLOW_CONSTRAINTS_LOCATION", self.airflow_constraints_location)
        self._opt_arg("ADDITIONAL_AIRFLOW_EXTRAS", self.additional_airflow_extras)
        self._opt_arg("ADDITIONAL_DEV_APT_COMMAND", self.additional_dev_apt_command)
        self._opt_arg("ADDITIONAL_DEV_APT_DEPS", self.additional_dev_apt_deps)
        self._opt_arg("ADDITIONAL_DEV_APT_ENV", self.additional_dev_apt_env)
        self._opt_arg("ADDITIONAL_PIP_INSTALL_FLAGS", self.additional_pip_install_flags)
        self._opt_arg("ADDITIONAL_PYTHON_DEPS", self.additional_python_deps)
        self._opt_arg("BUILD_PROGRESS", self.build_progress)
        self._opt_arg("COMMIT_SHA", self.commit_sha)
        self._opt_arg("DEV_APT_COMMAND", self.dev_apt_command)
        self._opt_arg("DEV_APT_DEPS", self.dev_apt_deps)
        self._opt_arg("DOCKER_HOST", self.docker_host)
        self._opt_arg("INSTALL_MYSQL_CLIENT_TYPE", self.install_mysql_client_type)
        self._opt_arg("VERSION_SUFFIX_FOR_PYPI", self.version_suffix_for_pypi)
        # Convert to build args
        build_args = self._to_build_args()
        # Add cache directive
        return build_args

    def __post_init__(self):
        self.version_suffix_for_pypi = self.version_suffix_for_pypi or "dev0"
