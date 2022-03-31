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
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import List

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH, DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.console import console
from airflow_breeze.global_constants import (
    AIRFLOW_SOURCES_FROM,
    AIRFLOW_SOURCES_TO,
    AIRFLOW_SOURCES_WWW_FROM,
    AIRFLOW_SOURCES_WWW_TO,
    get_airflow_extras,
    get_airflow_version,
)
from airflow_breeze.utils.docker_command_utils import check_if_buildx_plugin_available
from airflow_breeze.utils.run_utils import is_multi_platform, run_command


@dataclass
class ProdParams:
    build_cache_local: bool
    build_cache_pulled: bool
    build_cache_disabled: bool
    skip_rebuild_check: bool
    disable_mysql_client_installation: bool
    disable_mssql_client_installation: bool
    disable_postgres_client_installation: bool
    install_docker_context_files: bool
    disable_pypi_when_building: bool
    disable_pip_cache: bool
    upgrade_to_newer_dependencies: str = "false"
    skip_installing_airflow_providers_from_sources: bool = False
    cleanup_docker_context_files: bool = False
    prepare_buildx_cache: bool = False
    airflow_version: str = get_airflow_version()
    python_version: str = "3.7"
    airflow_branch_for_pypi_preloading: str = AIRFLOW_BRANCH
    install_airflow_reference: str = ""
    install_airflow_version: str = ""
    default_constraints_branch = DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    ci: str = "false"
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
    image_tag: str = ""
    additional_airflow_extras: str = ""
    github_token: str = ""
    airflow_login_to_github_registry: str = "false"
    github_username: str = "apache"
    platform: str = f"linux/{os.uname().machine}"
    airflow_constraints_reference: str = ""
    airflow_constraints_location: str = ""
    installation_method: str = "."
    debian_version: str = "bullseye"

    @property
    def airflow_branch(self) -> str:
        return self.airflow_branch_for_pypi_preloading

    @property
    def airflow_image_name(self):
        image = f'ghcr.io/{self.github_repository.lower()}'
        return image

    @property
    def airflow_prod_image_name(self):
        """Construct PROD image link"""
        image = f'{self.airflow_image_name}/{self.airflow_branch}/prod/python{self.python_version}'
        return image

    @property
    def the_image_type(self) -> str:
        the_image_type = 'PROD'
        return the_image_type

    @property
    def image_description(self) -> str:
        image_description = 'Airflow production'
        return image_description

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
        if self.ci:
            build_args.extend(["--build-arg", "PIP_PROGRESS_BAR=off"])
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
        elif re.match(r'v?2\.0*', self.airflow_version):
            self.airflow_branch_for_pypi_preloading = "v2-0-stable"
        elif re.match(r'v?2\.1*', self.airflow_version):
            self.airflow_branch_for_pypi_preloading = "v2-1-stable"
        elif re.match(r'v?2\.2*', self.airflow_version):
            self.airflow_branch_for_pypi_preloading = "v2-2-stable"
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
            self.airflow_version = self.install_airflow_reference
        elif len(self.install_airflow_version) > 0:
            if not re.match(r'^[0-9\.]+((a|b|rc|alpha|beta|pre)[0-9]+)?$', self.install_airflow_version):
                console.print(
                    f'\n[red]ERROR: Bad value for install-airflow-version:{self.install_airflow_version}'
                )
                console.print('[red]Only numerical versions allowed for PROD image here !')
                sys.exit()
            extra_build_flags.extend(["--build-arg", "AIRFLOW_INSTALLATION_METHOD=apache-airflow"])
            extra_build_flags.extend(
                ["--build-arg", f"AIRFLOW_VERSION_SPECIFICATION==={self.install_airflow_version}"]
            )
            extra_build_flags.extend(["--build-arg", f"AIRFLOW_VERSION={self.install_airflow_version}"])
            extra_build_flags.extend(self.args_for_remote_install)
            self.airflow_version = self.install_airflow_version
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
        return extra_build_flags

    @property
    def docker_cache(self) -> str:
        if self.build_cache_local:
            docker_cache = "local"
        elif self.build_cache_disabled:
            docker_cache = "disabled"
        else:
            docker_cache = "pulled"
        return docker_cache

    @property
    def docker_cache_prod_directive(self) -> List:
        docker_cache_prod_directive = []

        if self.docker_cache == "pulled":
            docker_cache_prod_directive.append(f"--cache-from={self.airflow_prod_image_name}")
        elif self.docker_cache == "disabled":
            docker_cache_prod_directive.append("--no-cache")
        else:
            docker_cache_prod_directive = []

        if self.prepare_buildx_cache:
            docker_cache_prod_directive.extend(["--cache-to=type=inline,mode=max", "--push"])
            if is_multi_platform(self.platform):
                console.print("\nSkip loading docker image on multi-platform build")
            else:
                docker_cache_prod_directive.extend(["--load"])
        return docker_cache_prod_directive

    def check_buildx_plugin_build_command(self):
        build_command_param = []
        is_buildx_available = check_if_buildx_plugin_available(True)
        if is_buildx_available:
            if self.prepare_buildx_cache:
                build_command_param.extend(
                    ["buildx", "build", "--builder", "airflow_cache", "--progress=tty"]
                )
                cmd = ['docker', 'buildx', 'inspect', 'airflow_cache']
                output = run_command(cmd, verbose=True, text=True)
                if output.returncode != 0:
                    next_cmd = ['docker', 'buildx', 'create', '--name', 'airflow_cache']
                    run_command(next_cmd, verbose=True, text=True)
            else:
                build_command_param.extend(["buildx", "build", "--builder", "default", "--progress=tty"])
        else:
            if self.prepare_buildx_cache:
                console.print(
                    '\n[red] Buildx cli plugin is not available and you need it to prepare buildx cache. \n'
                )
                console.print(
                    '[red] Please install it following https://docs.docker.com/buildx/working-with-buildx/ \n'
                )
                sys.exit()
            build_command_param.append("build")
        return build_command_param

    @property
    def python_base_image(self):
        """Construct Python Base Image"""
        #  ghcr.io/apache/airflow/main/python:3.8-slim-bullseye
        return f'python:{self.python_version}-slim-{self.debian_version}'

    @property
    def commit_sha(self):
        output = run_command(['git', 'rev-parse', 'HEAD'], capture_output=True, text=True)
        return output.stdout.strip()

    @property
    def airflow_image_repository(self):
        return f'https://github.com/{self.github_repository}'

    @property
    def airflow_image_date_created(self):
        now = datetime.now()
        return now.strftime("%Y-%m-%dT%H:%M:%SZ")

    @property
    def airflow_image_readme_url(self):
        return (
            f"https://raw.githubusercontent.com/apache/airflow/{self.commit_sha}/docs/docker-stack/README.md"
        )

    def print_info(self):
        console.print(
            f"Airflow {self.airflow_version} Python: {self.python_version}.\
                 Image description: {self.image_description}"
        )

    @property
    def skip_building_prod_image(self) -> bool:
        skip_build = False
        if self.skip_rebuild_check:
            skip_build = True
        return skip_build

    @property
    def check_image_for_rebuild(self) -> bool:
        check_image = True
        if self.skip_rebuild_check:
            check_image = False
        return check_image

    @property
    def install_from_pypi(self) -> str:
        install_from_pypi = 'true'
        if self.disable_pypi_when_building:
            install_from_pypi = 'false'
        return install_from_pypi

    @property
    def airflow_pre_cached_pip_packages(self) -> str:
        airflow_pre_cached_pip = 'true'
        if self.disable_pypi_when_building or self.disable_pip_cache:
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
    def install_providers_from_sources(self) -> str:
        install_providers_source = 'true'
        if (
            self.skip_installing_airflow_providers_from_sources
            or len(self.install_airflow_reference) > 0
            or len(self.install_airflow_version) > 0
        ):
            install_providers_source = 'false'
        return install_providers_source

    @property
    def install_from_docker_context_files(self) -> str:
        install_from_docker_context_files = 'false'
        if self.install_docker_context_files:
            install_from_docker_context_files = 'true'
        return install_from_docker_context_files

    @property
    def airflow_extras(self):
        return get_airflow_extras()

    @property
    def docker_context_files(self) -> str:
        return "docker-context-files"
