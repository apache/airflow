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

import json
import os
import re
import sys
from dataclasses import dataclass
from typing import List

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH, DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import (
    AIRFLOW_SOURCES_FROM,
    AIRFLOW_SOURCES_TO,
    get_airflow_extras,
    get_airflow_version,
)
from airflow_breeze.params.common_build_params import CommonBuildParams
from airflow_breeze.utils.console import get_console


@dataclass
class BuildProdParams(CommonBuildParams):
    """
    PROD build parameters. Those parameters are used to determine command issued to build PROD image.
    """

    airflow_constraints_mode: str = "constraints"
    default_constraints_branch: str = os.environ.get(
        'DEFAULT_CONSTRAINTS_BRANCH', DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    )
    airflow_constraints_reference: str = ""
    cleanup_context: bool = False
    disable_airflow_repo_cache: bool = False
    disable_mssql_client_installation: bool = False
    disable_mysql_client_installation: bool = False
    disable_postgres_client_installation: bool = False
    install_airflow_reference: str = ""
    install_airflow_version: str = ""
    install_packages_from_context: bool = False
    installation_method: str = "."
    airflow_extras: str = get_airflow_extras()

    @property
    def airflow_version(self) -> str:
        if self.install_airflow_version:
            return self.install_airflow_version
        else:
            return get_airflow_version()

    @property
    def image_type(self) -> str:
        return 'PROD'

    @property
    def args_for_remote_install(self) -> List:
        build_args = []
        build_args.extend(
            [
                "--build-arg",
                "AIRFLOW_SOURCES_FROM=empty",
                "--build-arg",
                "AIRFLOW_SOURCES_TO=/empty",
            ]
        )
        if re.match('v?2.*', self.airflow_version):
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
        if self.airflow_version == 'v2-0-test':
            self.airflow_branch_for_pypi_preloading = "v2-0-test"
        elif self.airflow_version == 'v2-1-test':
            self.airflow_branch_for_pypi_preloading = "v2-1-test"
        elif self.airflow_version == 'v2-2-test':
            self.airflow_branch_for_pypi_preloading = "v2-2-test"
        elif re.match(r'^2\.0.*$', self.airflow_version):
            self.airflow_branch_for_pypi_preloading = "v2-0-stable"
        elif re.match(r'^2\.1.*$', self.airflow_version):
            self.airflow_branch_for_pypi_preloading = "v2-1-stable"
        elif re.match(r'^2\.2.*$', self.airflow_version):
            self.airflow_branch_for_pypi_preloading = "v2-2-stable"
        elif re.match(r'^2\.3.*$', self.airflow_version):
            self.airflow_branch_for_pypi_preloading = "v2-3-stable"
        else:
            self.airflow_branch_for_pypi_preloading = AIRFLOW_BRANCH
        return build_args

    @property
    def extra_docker_build_flags(self) -> List[str]:
        extra_build_flags = []
        if len(self.install_airflow_reference) > 0:
            AIRFLOW_INSTALLATION_METHOD = (
                "https://github.com/apache/airflow/archive/"
                + self.install_airflow_reference
                + ".tar.gz#egg=apache-airflow"
            )
            extra_build_flags.extend(
                [
                    "--build-arg",
                    AIRFLOW_INSTALLATION_METHOD,
                ]
            )
            extra_build_flags.extend(self.args_for_remote_install)
        elif len(self.install_airflow_version) > 0:
            if not re.match(r'^[0-9\.]+((a|b|rc|alpha|beta|pre)[0-9]+)?$', self.install_airflow_version):
                get_console().print(
                    f'\n[error]ERROR: Bad value for install-airflow-version:{self.install_airflow_version}'
                )
                get_console().print('[error]Only numerical versions allowed for PROD image here !')
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
    def airflow_pre_cached_pip_packages(self) -> str:
        return 'false' if self.disable_airflow_repo_cache else 'true'

    @property
    def install_mssql_client(self) -> str:
        return 'false' if self.disable_mssql_client_installation else 'true'

    @property
    def install_mysql_client(self) -> str:
        return 'false' if self.disable_mysql_client_installation else 'true'

    @property
    def install_postgres_client(self) -> str:
        return 'false' if self.disable_postgres_client_installation else 'true'

    @property
    def docker_context_files(self) -> str:
        return "docker-context-files"

    @property
    def required_image_args(self) -> List[str]:
        return [
            "additional_airflow_extras",
            "additional_dev_apt_command",
            "additional_dev_apt_deps",
            "additional_dev_apt_env",
            "additional_python_deps",
            "additional_runtime_apt_command",
            "additional_runtime_apt_deps",
            "additional_runtime_apt_env",
            "airflow_branch",
            "airflow_constraints_mode",
            "airflow_extras",
            "airflow_image_date_created",
            "airflow_image_readme_url",
            "airflow_image_repository",
            "airflow_pre_cached_pip_packages",
            "airflow_version",
            "build_id",
            "constraints_github_repository",
            "docker_context_files",
            "install_mssql_client",
            "install_mysql_client",
            "install_packages_from_context",
            "install_postgres_client",
            "install_providers_from_sources",
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
