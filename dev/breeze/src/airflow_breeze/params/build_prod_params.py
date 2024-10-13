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

import json
import re
import sys
from dataclasses import dataclass, field

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH, DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import (
    AIRFLOW_SOURCES_FROM,
    AIRFLOW_SOURCES_TO,
    get_airflow_extras,
)
from airflow_breeze.params.common_build_params import CommonBuildParams
from airflow_breeze.utils.console import get_console


@dataclass
class BuildProdParams(CommonBuildParams):
    """
    PROD build parameters. Those parameters are used to determine command issued to build PROD image.
    """

    additional_runtime_apt_command: str | None = None
    additional_runtime_apt_deps: str | None = None
    additional_runtime_apt_env: str | None = None
    airflow_constraints_mode: str = "constraints"
    airflow_constraints_reference: str = DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    cleanup_context: bool = False
    airflow_extras: str = field(default_factory=get_airflow_extras)
    disable_mssql_client_installation: bool = False
    disable_mysql_client_installation: bool = False
    disable_postgres_client_installation: bool = False
    install_airflow_reference: str | None = None
    install_airflow_version: str | None = None
    install_packages_from_context: bool = False
    installation_method: str = "."
    runtime_apt_command: str | None = None
    runtime_apt_deps: str | None = None
    use_constraints_for_context_packages: bool = False
    use_uv: bool = True

    @property
    def airflow_version(self) -> str:
        if self.install_airflow_version:
            return self.install_airflow_version
        else:
            return self._get_version_with_suffix()

    @property
    def image_type(self) -> str:
        return "PROD"

    @property
    def args_for_remote_install(self) -> list:
        build_args = []
        build_args.extend(
            [
                "--build-arg",
                "AIRFLOW_SOURCES_FROM=empty",
                "--build-arg",
                "AIRFLOW_SOURCES_TO=/empty",
            ]
        )
        if re.match("v?2.*", self.airflow_version):
            build_args.extend(
                ["--build-arg", f"AIRFLOW_CONSTRAINTS_REFERENCE=constraints-{self.airflow_version}"]
            )
        else:
            build_args.extend(
                ["--build-arg", f"AIRFLOW_CONSTRAINTS_REFERENCE={self.airflow_constraints_reference}"]
            )
        if self.airflow_constraints_location:
            # override location if specified
            build_args.extend(
                ["--build-arg", f"AIRFLOW_CONSTRAINTS_LOCATION={self.airflow_constraints_location}"]
            )
        if self.airflow_version == "v2-0-test":
            self.airflow_branch_for_pypi_preloading = "v2-0-test"
        elif self.airflow_version == "v2-1-test":
            self.airflow_branch_for_pypi_preloading = "v2-1-test"
        elif self.airflow_version == "v2-2-test":
            self.airflow_branch_for_pypi_preloading = "v2-2-test"
        elif re.match(r"^2\.0.*$", self.airflow_version):
            self.airflow_branch_for_pypi_preloading = "v2-0-stable"
        elif re.match(r"^2\.1.*$", self.airflow_version):
            self.airflow_branch_for_pypi_preloading = "v2-1-stable"
        elif re.match(r"^2\.2.*$", self.airflow_version):
            self.airflow_branch_for_pypi_preloading = "v2-2-stable"
        elif re.match(r"^2\.3.*$", self.airflow_version):
            self.airflow_branch_for_pypi_preloading = "v2-3-stable"
        else:
            self.airflow_branch_for_pypi_preloading = AIRFLOW_BRANCH
        return build_args

    def _extra_prod_docker_build_flags(self) -> list[str]:
        extra_build_flags = []
        if self.install_airflow_reference:
            extra_build_flags.extend(
                [
                    "--build-arg",
                    "https://github.com/apache/airflow/archive/"
                    + self.install_airflow_reference
                    + ".tar.gz#egg=apache-airflow",
                ]
            )
            extra_build_flags.extend(self.args_for_remote_install)
        elif self.install_airflow_version:
            if not re.match(r"^[0-9.]+((a|b|rc|alpha|beta|pre)[0-9]+)?$", self.install_airflow_version):
                get_console().print(
                    f"\n[error]ERROR: Bad value for install-airflow-version:{self.install_airflow_version}"
                )
                get_console().print("[error]Only numerical versions allowed for PROD image here !")
                sys.exit()
            extra_build_flags.extend(["--build-arg", "AIRFLOW_INSTALLATION_METHOD=apache-airflow"])
            extra_build_flags.extend(
                ["--build-arg", f"AIRFLOW_VERSION_SPECIFICATION==={self.install_airflow_version}"]
            )
            extra_build_flags.extend(["--build-arg", f"AIRFLOW_VERSION={self.install_airflow_version}"])
            constraints_base = (
                f"https://raw.githubusercontent.com/{self.github_repository}/"
                f"{self.airflow_constraints_reference}"
            )
            constraints_location = (
                f"{constraints_base}/constraints-{self.install_airflow_version}/constraints-{self.python}.txt"
            )
            self.airflow_constraints_location = constraints_location
            extra_build_flags.extend(self.args_for_remote_install)
        elif self.install_packages_from_context:
            extra_build_flags.extend(
                [
                    "--build-arg",
                    "AIRFLOW_SOURCES_FROM=/empty",
                    "--build-arg",
                    "AIRFLOW_SOURCES_TO=/empty",
                    "--build-arg",
                    f"AIRFLOW_INSTALLATION_METHOD={self.installation_method}",
                    "--build-arg",
                    f"AIRFLOW_CONSTRAINTS_REFERENCE={self.airflow_constraints_reference}",
                ],
            )
        else:
            extra_build_flags.extend(
                [
                    "--build-arg",
                    f"AIRFLOW_SOURCES_FROM={AIRFLOW_SOURCES_FROM}",
                    "--build-arg",
                    f"AIRFLOW_SOURCES_TO={AIRFLOW_SOURCES_TO}",
                    "--build-arg",
                    f"AIRFLOW_INSTALLATION_METHOD={self.installation_method}",
                    "--build-arg",
                    f"AIRFLOW_CONSTRAINTS_REFERENCE={self.airflow_constraints_reference}",
                ]
            )
        maintainers = json.dumps([{"name": "Apache Airflow PMC", "email": "dev@airflow.apache.org"}])
        logo_url = "https://github.com/apache/airflow/raw/main/docs/apache-airflow/img/logos/wordmark_1.png"
        readme_url = "https://raw.githubusercontent.com/apache/airflow/main/docs/docker-stack/README.md"
        extra_build_flags.extend(
            [
                "--label",
                "io.artifacthub.package.license=Apache-2.0",
                "--label",
                f"io.artifacthub.package.readme-url={readme_url}",
                "--label",
                f"io.artifacthub.package.maintainers={maintainers}",
                "--label",
                f"io.artifacthub.package.logo-url={logo_url}",
            ]
        )
        return extra_build_flags

    @property
    def install_mssql_client(self) -> str:
        return "false" if self.disable_mssql_client_installation else "true"

    @property
    def install_mysql_client(self) -> str:
        return "false" if self.disable_mysql_client_installation else "true"

    @property
    def install_postgres_client(self) -> str:
        return "false" if self.disable_postgres_client_installation else "true"

    @property
    def docker_context_files(self) -> str:
        return "./docker-context-files"

    @property
    def airflow_image_kubernetes(self) -> str:
        return f"{self.airflow_image_name}-kubernetes"

    def prepare_arguments_for_docker_build_command(self) -> list[str]:
        self.build_arg_values: list[str] = []
        # Required build args
        self._req_arg("AIRFLOW_BRANCH", self.airflow_branch)
        self._req_arg("AIRFLOW_CONSTRAINTS_MODE", self.airflow_constraints_mode)
        self._req_arg("AIRFLOW_EXTRAS", self.airflow_extras)
        self._req_arg("AIRFLOW_IMAGE_DATE_CREATED", self.airflow_image_date_created)
        self._req_arg("AIRFLOW_IMAGE_README_URL", self.airflow_image_readme_url)
        self._req_arg("AIRFLOW_IMAGE_REPOSITORY", self.airflow_image_repository)
        self._req_arg("AIRFLOW_PRE_CACHED_PIP_PACKAGES", self.airflow_pre_cached_pip_packages)
        self._opt_arg("AIRFLOW_USE_UV", self.use_uv)
        if self.use_uv:
            from airflow_breeze.utils.uv_utils import get_uv_timeout

            self._req_arg("UV_HTTP_TIMEOUT", get_uv_timeout(self))
        self._req_arg("AIRFLOW_VERSION", self.airflow_version)
        self._req_arg("BUILD_ID", self.build_id)
        self._req_arg("CONSTRAINTS_GITHUB_REPOSITORY", self.constraints_github_repository)
        self._req_arg("DOCKER_CONTEXT_FILES", self.docker_context_files)
        self._req_arg("INSTALL_PACKAGES_FROM_CONTEXT", self.install_packages_from_context)
        self._req_arg("INSTALL_POSTGRES_CLIENT", self.install_postgres_client)
        self._req_arg("PYTHON_BASE_IMAGE", self.python_base_image)
        # optional build args
        self._opt_arg("AIRFLOW_CONSTRAINTS_LOCATION", self.airflow_constraints_location)
        self._opt_arg("ADDITIONAL_AIRFLOW_EXTRAS", self.additional_airflow_extras)
        self._opt_arg("ADDITIONAL_DEV_APT_COMMAND", self.additional_dev_apt_command)
        self._opt_arg("ADDITIONAL_DEV_APT_DEPS", self.additional_dev_apt_deps)
        self._opt_arg("ADDITIONAL_DEV_APT_ENV", self.additional_dev_apt_env)
        self._opt_arg("ADDITIONAL_PIP_INSTALL_FLAGS", self.additional_pip_install_flags)
        self._opt_arg("ADDITIONAL_PYTHON_DEPS", self.additional_python_deps)
        self._opt_arg("ADDITIONAL_RUNTIME_APT_COMMAND", self.additional_runtime_apt_command)
        self._opt_arg("ADDITIONAL_RUNTIME_APT_DEPS", self.additional_runtime_apt_deps)
        self._opt_arg("ADDITIONAL_RUNTIME_APT_ENV", self.additional_runtime_apt_env)
        self._opt_arg("BUILD_PROGRESS", self.build_progress)
        self._opt_arg("COMMIT_SHA", self.commit_sha)
        self._opt_arg("DEV_APT_COMMAND", self.dev_apt_command)
        self._opt_arg("DEV_APT_DEPS", self.dev_apt_deps)
        self._opt_arg("DOCKER_HOST", self.docker_host)
        self._req_arg("INSTALL_MSSQL_CLIENT", self.install_mssql_client)
        self._opt_arg("INSTALL_MYSQL_CLIENT", self.install_mysql_client)
        self._req_arg("INSTALL_MYSQL_CLIENT_TYPE", self.install_mysql_client_type)
        self._opt_arg("RUNTIME_APT_COMMAND", self.runtime_apt_command)
        self._opt_arg("RUNTIME_APT_DEPS", self.runtime_apt_deps)
        self._opt_arg("USE_CONSTRAINTS_FOR_CONTEXT_PACKAGES", self.use_constraints_for_context_packages)
        self._opt_arg("VERSION_SUFFIX_FOR_PYPI", self.version_suffix_for_pypi)
        build_args = self._to_build_args()
        build_args.extend(self._extra_prod_docker_build_flags())
        return build_args
