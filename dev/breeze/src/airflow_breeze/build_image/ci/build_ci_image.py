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
"""Command to build CI image."""
from typing import Dict

from airflow_breeze.build_image.ci.build_ci_params import BuildCiParams
from airflow_breeze.utils.cache import synchronize_parameters_with_cache, touch_cache_file
from airflow_breeze.utils.console import console
from airflow_breeze.utils.docker_command_utils import (
    construct_docker_build_command,
    construct_empty_docker_build_command,
    tag_and_push_image,
)
from airflow_breeze.utils.md5_build_check import calculate_md5_checksum_for_files
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT, BUILD_CACHE_DIR
from airflow_breeze.utils.registry import login_to_docker_registry
from airflow_breeze.utils.run_utils import filter_out_none, fix_group_permissions, run_command

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


def get_ci_image_build_params(parameters_passed: Dict) -> BuildCiParams:
    """
    Converts parameters received as dict into BuildCiParams. In case cacheable
    parameters are missing, it reads the last used value for that parameter
    from the cache and if it is not found, it uses default value for that parameter.

    This method updates cached based on parameters passed via Dict.

    :param parameters_passed: parameters to use when constructing BuildCiParams
    """
    ci_image_params = BuildCiParams(**parameters_passed)
    synchronize_parameters_with_cache(ci_image_params, parameters_passed)
    return ci_image_params


def build_image(verbose: bool, dry_run: bool, **kwargs) -> None:
    """
    Builds CI image:

      * fixes group permissions for files (to improve caching when umask is 002)
      * converts all the parameters received via kwargs into BuildCIParams (including cache)
      * prints info about the image to build
      * logs int to docker registry on CI if build cache is being executed
      * removes "tag" for previously build image so that inline cache uses only remote image
      * constructs docker-compose command to run based on parameters passed
      * run the build command
      * update cached information that the build completed and saves checksums of all files
        for quick future check if the build is needed

    :param verbose: print commands when running
    :param dry_run: do not execute "write" commands - just print what would happen
    :param kwargs: arguments passed from the command
    """
    fix_group_permissions()
    parameters_passed = filter_out_none(**kwargs)
    ci_image_params = get_ci_image_build_params(parameters_passed)
    ci_image_params.print_info()
    run_command(
        ["docker", "rmi", "--no-prune", "--force", ci_image_params.airflow_image_name],
        verbose=verbose,
        dry_run=dry_run,
        cwd=AIRFLOW_SOURCES_ROOT,
        text=True,
        check=False,
    )
    if ci_image_params.prepare_buildx_cache:
        login_to_docker_registry(ci_image_params)
    cmd = construct_docker_build_command(
        image_params=ci_image_params,
        verbose=verbose,
        required_args=REQUIRED_CI_IMAGE_ARGS,
        optional_args=OPTIONAL_CI_IMAGE_ARGS,
        production_image=False,
    )
    if ci_image_params.empty_image:
        console.print(f"\n[blue]Building empty CI Image for Python {ci_image_params.python}\n")
        cmd = construct_empty_docker_build_command(image_params=ci_image_params)
        run_command(
            cmd, input="FROM scratch\n", verbose=verbose, dry_run=dry_run, cwd=AIRFLOW_SOURCES_ROOT, text=True
        )
    else:
        console.print(f"\n[blue]Building CI Image for Python {ci_image_params.python}\n")
        run_command(cmd, verbose=verbose, dry_run=dry_run, cwd=AIRFLOW_SOURCES_ROOT, text=True)
    if not dry_run:
        ci_image_cache_dir = BUILD_CACHE_DIR / ci_image_params.airflow_branch
        ci_image_cache_dir.mkdir(parents=True, exist_ok=True)
        touch_cache_file(f"built_{ci_image_params.python}", root_dir=ci_image_cache_dir)
        calculate_md5_checksum_for_files(ci_image_params.md5sum_cache_dir, update=True)
    else:
        console.print("[blue]Not updating build cache because we are in `dry_run` mode.[/]")
    if ci_image_params.push_image:
        tag_and_push_image(image_params=ci_image_params, dry_run=dry_run, verbose=verbose)
