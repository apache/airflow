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

import multiprocessing as mp
import os
import sys
from typing import List, Tuple

from airflow_breeze.build_image.ci.build_ci_params import (
    OPTIONAL_CI_IMAGE_ARGS,
    REQUIRED_CI_IMAGE_ARGS,
    BuildCiParams,
)
from airflow_breeze.utils.cache import touch_cache_file
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.confirm import Answer, user_confirm
from airflow_breeze.utils.console import console
from airflow_breeze.utils.docker_command_utils import (
    construct_docker_build_command,
    construct_empty_docker_build_command,
    tag_and_push_image,
)
from airflow_breeze.utils.md5_build_check import (
    calculate_md5_checksum_for_files,
    md5sum_check_if_build_is_needed,
)
from airflow_breeze.utils.parallel import check_async_run_results
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT, BUILD_CACHE_DIR
from airflow_breeze.utils.registry import login_to_docker_registry
from airflow_breeze.utils.run_utils import (
    fix_group_permissions,
    instruct_build_image,
    is_repo_rebased,
    run_command,
)


def should_we_run_the_build(build_ci_params: BuildCiParams) -> bool:
    """
    Check if we should run the build based on what files have been modified since last build and answer from
    the user.

    * If build is needed, the user is asked for confirmation
    * If the branch is not rebased it warns the user to rebase (to make sure latest remote cache is useful)
    * Builds Image/Skips/Quits depending on the answer

    :param build_ci_params: parameters for the build
    """
    # We import those locally so that click autocomplete works
    from inputimeout import TimeoutOccurred

    if not md5sum_check_if_build_is_needed(md5sum_cache_dir=build_ci_params.md5sum_cache_dir):
        return False
    try:
        answer = user_confirm(message="Do you want to build image?", timeout=5, default_answer=Answer.NO)
        if answer == answer.YES:
            if is_repo_rebased(build_ci_params.github_repository, build_ci_params.airflow_branch):
                return True
            else:
                console.print(
                    "\n[bright_yellow]This might take a lot of time, w"
                    "e think you should rebase first.[/]\n"
                )
                answer = user_confirm(
                    "But if you really, really want - you can do it", timeout=5, default_answer=Answer.NO
                )
                if answer == Answer.YES:
                    return True
                else:
                    console.print(
                        "[bright_blue]Please rebase your code before continuing.[/]\n"
                        "Check this link to know more "
                        "https://github.com/apache/airflow/blob/main/CONTRIBUTING.rst#id15\n"
                    )
                    console.print('[red]Exiting the process[/]\n')
                    sys.exit(1)
        elif answer == Answer.NO:
            instruct_build_image(build_ci_params.python)
            return False
        else:  # users_status == Answer.QUIT:
            console.print('\n[bright_yellow]Quitting the process[/]\n')
            sys.exit()
    except TimeoutOccurred:
        console.print('\nTimeout. Considering your response as No\n')
        instruct_build_image(build_ci_params.python)
        return False
    except Exception as e:
        console.print(f'\nTerminating the process on {e}')
        sys.exit(1)


def build_ci_image(
    verbose: bool, dry_run: bool, with_ci_group: bool, ci_image_params: BuildCiParams
) -> Tuple[int, str]:
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
    :param with_ci_group: whether to wrap the build in CI logging group
    :param ci_image_params: CI image parameters
    """
    fix_group_permissions()
    if verbose or dry_run:
        console.print(
            f"\n[bright_blue]Building CI image of airflow from {AIRFLOW_SOURCES_ROOT} "
            f"python version: {ci_image_params.python}[/]\n"
        )
    with ci_group(
        f"Build CI image for Python {ci_image_params.python} " f"with tag: {ci_image_params.image_tag}",
        enabled=with_ci_group,
    ):
        ci_image_params.print_info()
        if not ci_image_params.force_build and not ci_image_params.upgrade_to_newer_dependencies:
            if not should_we_run_the_build(build_ci_params=ci_image_params):
                return 0, f"Image build: {ci_image_params.python}"
        run_command(
            ["docker", "rmi", "--no-prune", "--force", ci_image_params.airflow_image_name],
            verbose=verbose,
            dry_run=dry_run,
            cwd=AIRFLOW_SOURCES_ROOT,
            text=True,
            check=False,
        )
        if ci_image_params.prepare_buildx_cache:
            login_to_docker_registry(ci_image_params, dry_run=dry_run)
        cmd = construct_docker_build_command(
            image_params=ci_image_params,
            verbose=verbose,
            required_args=REQUIRED_CI_IMAGE_ARGS,
            optional_args=OPTIONAL_CI_IMAGE_ARGS,
            production_image=False,
        )
        if ci_image_params.empty_image:
            env = os.environ.copy()
            env['DOCKER_BUILDKIT'] = "1"
            console.print(f"\n[blue]Building empty CI Image for Python {ci_image_params.python}\n")
            cmd = construct_empty_docker_build_command(image_params=ci_image_params)
            build_result = run_command(
                cmd,
                input="FROM scratch\n",
                verbose=verbose,
                dry_run=dry_run,
                cwd=AIRFLOW_SOURCES_ROOT,
                text=True,
                env=env,
            )
        else:
            console.print(f"\n[blue]Building CI Image for Python {ci_image_params.python}\n")
            build_result = run_command(
                cmd, verbose=verbose, dry_run=dry_run, cwd=AIRFLOW_SOURCES_ROOT, text=True, check=False
            )
        if not dry_run:
            if build_result.returncode == 0:
                ci_image_cache_dir = BUILD_CACHE_DIR / ci_image_params.airflow_branch
                ci_image_cache_dir.mkdir(parents=True, exist_ok=True)
                touch_cache_file(f"built_{ci_image_params.python}", root_dir=ci_image_cache_dir)
                calculate_md5_checksum_for_files(ci_image_params.md5sum_cache_dir, update=True)
            else:
                console.print("[red]Error when building image![/]")
                return (
                    build_result.returncode,
                    f"Image build: {ci_image_params.python}",
                )
        else:
            console.print("[blue]Not updating build cache because we are in `dry_run` mode.[/]")
        if ci_image_params.push_image:
            return tag_and_push_image(image_params=ci_image_params, dry_run=dry_run, verbose=verbose)
        return build_result.returncode, f"Image build: {ci_image_params.python}"


def build_ci_image_in_parallel(
    verbose: bool, dry_run: bool, parallelism: int, python_version_list: List[str], **kwargs
):
    """Run CI image builds in parallel."""
    console.print(
        f"\n[bright_blue]Running with parallelism = {parallelism} for the images: {python_version_list}:"
    )
    pool = mp.Pool(parallelism)
    results = [pool.apply_async(build_ci_image, args=(verbose, dry_run, False), kwds=kwargs)]
    check_async_run_results(results)
    pool.close()
