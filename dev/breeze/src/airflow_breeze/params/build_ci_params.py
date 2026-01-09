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
from airflow_breeze.global_constants import ALL_PYTHON_VERSION_TO_PATCHLEVEL_VERSION
from airflow_breeze.params.common_build_params import CommonBuildParams
from airflow_breeze.utils.path_utils import BUILD_CACHE_PATH


@dataclass
class BuildCiParams(CommonBuildParams):
    """
    CI build parameters. Those parameters are used to determine command issued to build CI image.
    """

    airflow_constraints_mode: str = "constraints-source-providers"
    airflow_constraints_reference: str = DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    airflow_extras: str = "all"
    force_build: bool = False
    upgrade_to_newer_dependencies: bool = False
    upgrade_on_failure: bool = False
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
        return Path(BUILD_CACHE_PATH, self.airflow_branch, self.python, "CI")

    def prepare_arguments_for_docker_build_command(self) -> list[str]:
        self.build_arg_values: list[str] = []
        # Required build args
        self._set_common_req_args()
        self._req_arg("AIRFLOW_CONSTRAINTS_MODE", self.airflow_constraints_mode)
        self._req_arg("AIRFLOW_CONSTRAINTS_REFERENCE", self.airflow_constraints_reference)
        self._req_arg("AIRFLOW_EXTRAS", self.airflow_extras)
        self._req_arg("AIRFLOW_USE_UV", self.use_uv)
        if self.use_uv:
            from airflow_breeze.utils.uv_utils import get_uv_timeout

            self._opt_arg("UV_HTTP_TIMEOUT", get_uv_timeout(self))
        self._req_arg("AIRFLOW_VERSION", self.airflow_version)
        self._req_arg("BASE_IMAGE", self.python_base_image)
        self._req_arg(
            "AIRFLOW_PYTHON_VERSION", ALL_PYTHON_VERSION_TO_PATCHLEVEL_VERSION.get(self.python, self.python)
        )
        if self.upgrade_to_newer_dependencies:
            self._opt_arg("UPGRADE_RANDOM_INDICATOR_STRING", f"{random.randrange(2**32):x}")
        # optional build args
        self._set_common_opt_args()
        self._opt_arg("INSTALL_MYSQL_CLIENT_TYPE", self.install_mysql_client_type)
        # Convert to build args
        build_args = self._to_build_args()
        # Add cache directive
        return build_args

    def __post_init__(self):
        self.version_suffix = self.version_suffix or "dev0"
