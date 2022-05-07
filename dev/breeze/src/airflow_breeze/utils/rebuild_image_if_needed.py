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

from pathlib import Path
from typing import Union

from airflow_breeze.build_image.ci.build_ci_image import build_ci_image
from airflow_breeze.build_image.ci.build_ci_params import BuildCiParams
from airflow_breeze.shell.shell_params import ShellParams
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.docker_command_utils import (
    check_docker_compose_version,
    check_docker_resources,
    check_docker_version,
)
from airflow_breeze.utils.path_utils import BUILD_CACHE_DIR


def rebuild_ci_image_if_needed(
    build_params: Union[ShellParams, BuildCiParams], dry_run: bool, verbose: bool
) -> None:
    """
    Rebuilds CI image if needed and user confirms it.

    :param build_params: parameters of the shell
    :param dry_run: whether it's a dry_run
    :param verbose: should we print verbose messages
    """
    check_docker_version(verbose=verbose)
    check_docker_compose_version(verbose=verbose)
    check_docker_resources(build_params.airflow_image_name, verbose=verbose, dry_run=dry_run)
    build_ci_image_check_cache = Path(
        BUILD_CACHE_DIR, build_params.airflow_branch, f".built_{build_params.python}"
    )
    ci_image_params = BuildCiParams(python=build_params.python, upgrade_to_newer_dependencies=False)
    if build_ci_image_check_cache.exists():
        if verbose:
            get_console().print(f'[info]{build_params.the_image_type} image already built locally.[/]')
    else:
        get_console().print(
            f'[warning]{build_params.the_image_type} image not built locally. Forcing build.[/]'
        )
        ci_image_params.force_build = True
    build_ci_image(verbose, dry_run=dry_run, ci_image_params=ci_image_params)
