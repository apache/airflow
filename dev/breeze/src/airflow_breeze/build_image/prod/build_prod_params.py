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
"""Parameters to build PROD image."""
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH, DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import (
    AIRFLOW_SOURCES_FROM,
    AIRFLOW_SOURCES_TO,
    AIRFLOW_SOURCES_WWW_FROM,
    AIRFLOW_SOURCES_WWW_TO,
    get_airflow_extras,
    get_airflow_version,
)
from airflow_breeze.utils.console import get_console


@dataclass
class BuildProdParams:
    """
    PROD build parameters. Those parameters are used to determine command issued to build PROD image.
    """

    docker_cache: str = "pulled"
    disable_mysql_client_installation: bool = False
    disable_mssql_client_installation: bool = False
    disable_postgres_client_installation: bool = False
    disable_airflow_repo_cache: bool = False
    install_providers_from_sources: bool = False
    cleanup_context: bool = False
    prepare_buildx_cache: bool = False
    push_image: bool = False
    empty_image: bool = False
    airflow_is_in_context: bool = False
    install_packages_from_context: bool = False
    upgrade_to_newer_dependencies: bool = False
    python: str = "3.7"
    airflow_branch_for_pypi_preloading: str = AIRFLOW_BRANCH
    install_airflow_reference: str = ""
    install_airflow_version: str = ""
    default_constraints_branch = DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    build_id: int = 0
    airflow_constraints: str = "constraints-source-providers"
    github_repository: str = "apache/airflow"
    constraints_github_repository: str = "apache/airflow"
    dev_apt_command: str = ""
    dev_apt_deps: str = ""
    runtime_apt_command: str = ""
    runtime_apt_deps: str = ""
    additional_dev_apt_command: str = ""
    additional_dev_apt_deps: str = ""
    additional_dev_apt_env: str = ""
    additional_runtime_apt_command: str = ""
    additional_runtime_apt_deps: str = ""
    additional_runtime_apt_env: str = ""
    additional_python_deps: str = ""
    image_tag: Optional[str] = None
    extras: str = ""
    additional_airflow_extras: str = ""
    github_token: str = os.environ.get('GITHUB_TOKEN', "")
    github_actions: str = os.environ.get('GITHUB_ACTIONS', "false")
    login_to_github_registry: str = "false"
    github_username: str = ""
    platform: str = f"linux/{os.uname().machine}"
    airflow_constraints_reference: str = ""
    airflow_constraints_location: str = ""
    installation_method: str = "."
    debian_version: str = "bullseye"
    answer: Optional[str] = None

    @property
    def airflow_version(self) -> str:
        if self.install_airflow_version:
            return self.install_airflow_version
        else:
            return get_airflow_version()

    @property
    def airflow_branch(self) -> str:
        return self.airflow_branch_for_pypi_preloading

    @property
    def airflow_base_image_name(self):
        image = f'ghcr.io/{self.github_repository.lower()}'
        return image

    @property
    def airflow_image_name(self):
        """Construct PROD image link"""
        image = f'{self.airflow_base_image_name}/{self.airflow_branch}/prod/python{self.python}'
        return image

    @property
    def the_image_type(self) -> str:
        return 'PROD'

    @property
    def args_for_remote_install(self) -> List:
        build_args = []
        build_args.extend(
            [
                "--build-arg",
                "AIRFLOW_SOURCES_WWW_FROM=empty",
                "--build-arg",
                "AIRFLOW_SOURCES_WWW_TO=/empty",
                "--build-arg",
                "AIRFLOW_SOURCES_FROM=empty",
                "--build-arg",
                "AIRFLOW_SOURCES_TO=/empty",
            ]
        )
        if len(self.airflow_constraints_reference) > 0:
            build_args.extend(
                ["--build-arg", f"AIRFLOW_CONSTRAINTS_REFERENCE={self.airflow_constraints_reference}"]
            )
        else:
            if re.match('v?2.*', self.airflow_version):
                build_args.extend(
                    ["--build-arg", f"AIRFLOW_CONSTRAINTS_REFERENCE=constraints-{self.airflow_version}"]
                )
            else:
                build_args.extend(
                    ["--build-arg", f"AIRFLOW_CONSTRAINTS_REFERENCE={self.default_constraints_branch}"]
                )
        if len(self.airflow_constraints_location) > 0:
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
                    f"AIRFLOW_SOURCES_WWW_FROM={AIRFLOW_SOURCES_WWW_FROM}",
                    "--build-arg",
                    f"AIRFLOW_SOURCES_WWW_TO={AIRFLOW_SOURCES_WWW_TO}",
                    "--build-arg",
                    f"AIRFLOW_INSTALLATION_METHOD={self.installation_method}",
                    "--build-arg",
                    f"AIRFLOW_CONSTRAINTS_REFERENCE={self.default_constraints_branch}",
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
    def python_base_image(self):
        """Construct Python Base Image"""
        #  ghcr.io/apache/airflow/main/python:3.8-slim-bullseye
        return f'python:{self.python}-slim-{self.debian_version}'

    @property
    def airflow_image_repository(self):
        return f'https://github.com/{self.github_repository}'

    @property
    def airflow_image_date_created(self):
        now = datetime.now()
        return now.strftime("%Y-%m-%dT%H:%M:%SZ")

    @property
    def airflow_image_readme_url(self):
        return "https://raw.githubusercontent.com/apache/airflow/main/docs/docker-stack/README.md"

    @property
    def airflow_pre_cached_pip_packages(self) -> str:
        airflow_pre_cached_pip = 'true'
        if not self.airflow_is_in_context or self.disable_airflow_repo_cache:
            airflow_pre_cached_pip = 'false'
        return airflow_pre_cached_pip

    @property
    def install_mssql_client(self) -> str:
        install_mssql = 'true'
        if self.disable_mssql_client_installation:
            install_mssql = 'false'
        return install_mssql

    @property
    def install_mysql_client(self) -> str:
        install_mysql = 'true'
        if self.disable_mysql_client_installation:
            install_mysql = 'false'
        return install_mysql

    @property
    def install_postgres_client(self) -> str:
        install_postgres = 'true'
        if self.disable_postgres_client_installation:
            install_postgres = 'false'
        return install_postgres

    @property
    def airflow_extras(self):
        return get_airflow_extras()

    @property
    def docker_context_files(self) -> str:
        return "docker-context-files"

    @property
    def airflow_image_name_with_tag(self):
        """Construct PROD image link"""
        image = f'{self.airflow_base_image_name}/{self.airflow_branch}/prod/python{self.python}'
        return image if self.image_tag is None else image + f":{self.image_tag}"
