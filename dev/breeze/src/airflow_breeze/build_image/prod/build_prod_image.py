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
"""Command to build PROD image."""
import contextlib
import os
import sys
from typing import Tuple

from airflow_breeze.build_image.prod.build_prod_params import BuildProdParams
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.docker_command_utils import (
    construct_docker_build_command,
    construct_empty_docker_build_command,
    tag_and_push_image,
)
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT, DOCKER_CONTEXT_DIR
from airflow_breeze.utils.registry import login_to_docker_registry
from airflow_breeze.utils.run_utils import fix_group_permissions, run_command

REQUIRED_PROD_IMAGE_ARGS = [
    "python_base_image",
    "install_mysql_client",
    "install_mssql_client",
    "install_postgres_client",
    "airflow_version",
    "airflow_branch",
    "airflow_extras",
    "airflow_pre_cached_pip_packages",
    "docker_context_files",
    "extras",
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
    "airflow_constraints",
    "airflow_image_repository",
    "airflow_image_date_created",
    "build_id",
    "airflow_image_readme_url",
    "install_providers_from_sources",
    "airflow_is_in_context",
    "install_packages_from_context",
]

OPTIONAL_PROD_IMAGE_ARGS = [
    "dev_apt_command",
    "dev_apt_deps",
    "runtime_apt_command",
    "runtime_apt_deps",
]


def clean_docker_context_files(verbose: bool, dry_run: bool):
    """
    Cleans up docker context files folder - leaving only .README.md there.
    """
    if verbose or dry_run:
        get_console().print("[info]Cleaning docker-context-files[/]")
    if dry_run:
        return
    with contextlib.suppress(FileNotFoundError):
        context_files_to_delete = DOCKER_CONTEXT_DIR.glob('**/*')
        for file_to_delete in context_files_to_delete:
            if file_to_delete.name != '.README.md':
                file_to_delete.unlink()


def check_docker_context_files(install_packages_from_context: bool):
    """
    Sanity check - if we want to install from docker-context-files we expect some packages there but if
    we don't - we don't expect them, and they might invalidate Docker cache.

    This method exits with an error if what we see is unexpected for given operation.

    :param install_packages_from_context: whether we want to install from docker-context-files
    """
    context_file = DOCKER_CONTEXT_DIR.glob('**/*')
    number_of_context_files = len(
        [context for context in context_file if context.is_file() and context.name != '.README.md']
    )
    if number_of_context_files == 0:
        if install_packages_from_context:
            get_console().print('[warning]\nERROR! You want to install packages from docker-context-files')
            get_console().print('[warning]\n but there are no packages to install in this folder.')
            sys.exit(1)
    else:
        if not install_packages_from_context:
            get_console().print(
                '[warning]\n ERROR! There are some extra files in docker-context-files except README.md'
            )
            get_console().print('[warning]\nAnd you did not choose --install-packages-from-context flag')
            get_console().print(
                '[warning]\nThis might result in unnecessary cache invalidation and long build times'
            )
            get_console().print(
                '[warning]\nExiting now \
                    - please restart the command with --cleanup-context switch'
            )
            sys.exit(1)


def build_production_image(
    verbose: bool, dry_run: bool, prod_image_params: BuildProdParams
) -> Tuple[int, str]:
    """
    Builds PROD image:

      * fixes group permissions for files (to improve caching when umask is 002)
      * converts all the parameters received via kwargs into BuildProdParams (including cache)
      * prints info about the image to build
      * removes docker-context-files if requested
      * performs sanity check if the files are present in docker-context-files if expected
      * logs int to docker registry on CI if build cache is being executed
      * removes "tag" for previously build image so that inline cache uses only remote image
      * constructs docker-compose command to run based on parameters passed
      * run the build command
      * update cached information that the build completed and saves checksums of all files
        for quick future check if the build is needed

    :param verbose: print commands when running
    :param dry_run: do not execute "write" commands - just print what would happen
    :param prod_image_params: PROD image parameters
    """
    fix_group_permissions(verbose=verbose)
    if verbose or dry_run:
        get_console().print(
            f"\n[info]Building PROD image of airflow from {AIRFLOW_SOURCES_ROOT} "
            f"python version: {prod_image_params.python}[/]\n"
        )
    if prod_image_params.cleanup_context:
        clean_docker_context_files(verbose=verbose, dry_run=dry_run)
    check_docker_context_files(prod_image_params.install_packages_from_context)
    if prod_image_params.prepare_buildx_cache:
        login_to_docker_registry(prod_image_params, dry_run=dry_run)
    run_command(
        ["docker", "rmi", "--no-prune", "--force", prod_image_params.airflow_image_name],
        verbose=verbose,
        dry_run=dry_run,
        cwd=AIRFLOW_SOURCES_ROOT,
        text=True,
        check=False,
    )
    get_console().print(f"\n[info]Building PROD Image for Python {prod_image_params.python}\n")
    if prod_image_params.empty_image:
        env = os.environ.copy()
        env['DOCKER_BUILDKIT'] = "1"
        get_console().print(f"\n[info]Building empty PROD Image for Python {prod_image_params.python}\n")
        cmd = construct_empty_docker_build_command(image_params=prod_image_params)
        build_command_result = run_command(
            cmd,
            input="FROM scratch\n",
            verbose=verbose,
            dry_run=dry_run,
            cwd=AIRFLOW_SOURCES_ROOT,
            check=False,
            text=True,
            env=env,
        )
    else:
        cmd = construct_docker_build_command(
            image_params=prod_image_params,
            verbose=verbose,
            required_args=REQUIRED_PROD_IMAGE_ARGS,
            optional_args=OPTIONAL_PROD_IMAGE_ARGS,
            production_image=True,
        )
        build_command_result = run_command(
            cmd, verbose=verbose, dry_run=dry_run, cwd=AIRFLOW_SOURCES_ROOT, check=False, text=True
        )
    if build_command_result.returncode == 0:
        if prod_image_params.push_image:
            return tag_and_push_image(image_params=prod_image_params, dry_run=dry_run, verbose=verbose)
    return build_command_result.returncode, f"Image build: {prod_image_params.python}"
