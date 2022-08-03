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
import subprocess
import time
from typing import List, Tuple, Union

from airflow_breeze.global_constants import (
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    MOUNT_ALL,
)
from airflow_breeze.params.build_ci_params import BuildCiParams
from airflow_breeze.params.build_prod_params import BuildProdParams
from airflow_breeze.params.common_build_params import CommonBuildParams
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.mark_image_as_refreshed import mark_image_as_refreshed
from airflow_breeze.utils.parallel import check_async_run_results
from airflow_breeze.utils.registry import login_to_github_docker_registry
from airflow_breeze.utils.run_tests import verify_an_image
from airflow_breeze.utils.run_utils import RunCommandResult, run_command


def run_pull_in_parallel(
    dry_run: bool,
    parallelism: int,
    image_params_list: Union[List[BuildCiParams], List[BuildProdParams]],
    python_version_list: List[str],
    verbose: bool,
    verify: bool,
    tag_as_latest: bool,
    wait_for_image: bool,
    extra_pytest_args: Tuple,
):
    """Run image pull in parallel"""
    get_console().print(
        f"\n[info]Pulling with parallelism = {parallelism} for the images: {python_version_list}:"
    )
    pool = mp.Pool(parallelism)
    poll_time = 10.0
    if not verify:
        results = [
            pool.apply_async(
                run_pull_image,
                args=(image_param, dry_run, verbose, wait_for_image, tag_as_latest, poll_time, True),
            )
            for image_param in image_params_list
        ]
    else:
        results = [
            pool.apply_async(
                run_pull_and_verify_image,
                args=(
                    image_param,
                    dry_run,
                    verbose,
                    wait_for_image,
                    tag_as_latest,
                    poll_time,
                    extra_pytest_args,
                ),
            )
            for image_param in image_params_list
        ]
    check_async_run_results(results)
    pool.close()


def run_pull_image(
    image_params: CommonBuildParams,
    dry_run: bool,
    verbose: bool,
    wait_for_image: bool,
    tag_as_latest: bool,
    poll_time: float = 10.0,
    parallel: bool = False,
) -> Tuple[int, str]:
    """
    Pull image specified.
    :param image_params: Image parameters.
    :param dry_run: whether it's dry run
    :param verbose: whether it's verbose
    :param wait_for_image: whether we should wait for the image to be available
    :param tag_as_latest: tag the image as latest
    :param poll_time: what's the polling time between checks if images are there (default 10 s)
    :param parallel: whether the pull is run as part of parallel execution
    :return: Tuple of return code and description of the image pulled
    """
    get_console().print(
        f"\n[info]Pulling {image_params.image_type} image of airflow python version: "
        f"{image_params.python} image: {image_params.airflow_image_name_with_tag} "
        f"with wait for image: {wait_for_image}[/]\n"
    )
    while True:
        login_to_github_docker_registry(image_params=image_params, dry_run=dry_run, verbose=verbose)
        command_to_run = ["docker", "pull", image_params.airflow_image_name_with_tag]
        command_result = run_command(
            command_to_run,
            verbose=verbose,
            dry_run=dry_run,
            check=False,
            enabled_output_group=not parallel,
        )
        if command_result.returncode == 0:
            command_result = run_command(
                ["docker", "inspect", image_params.airflow_image_name_with_tag, "-f", "{{.Size}}"],
                capture_output=True,
                verbose=verbose,
                dry_run=dry_run,
                text=True,
                check=False,
            )
            if not dry_run:
                if command_result.returncode == 0:
                    image_size = int(command_result.stdout.strip())
                    if image_size == 0:
                        get_console().print("\n[error]The image size was 0 - image creation failed.[/]\n")
                        return 1, f"Image Python {image_params.python}"
                else:
                    get_console().print(
                        "\n[error]There was an error pulling the size of the image. Failing.[/]\n"
                    )
                    return (
                        command_result.returncode,
                        f"Image Python {image_params.python}",
                    )
            if tag_as_latest:
                command_result = tag_image_as_latest(image_params, dry_run, verbose)
                if command_result.returncode == 0 and isinstance(image_params, BuildCiParams):
                    mark_image_as_refreshed(image_params)
            return command_result.returncode, f"Image Python {image_params.python}"
        if wait_for_image:
            if verbose or dry_run:
                get_console().print(
                    f"\n[info]Waiting for {poll_time} seconds for "
                    f"{image_params.airflow_image_name_with_tag}.[/]\n"
                )
            time.sleep(poll_time)
            continue
        else:
            get_console().print(
                f"\n[error]There was an error pulling the image {image_params.python}. Failing.[/]\n"
            )
            return command_result.returncode, f"Image Python {image_params.python}"


def tag_image_as_latest(image_params: CommonBuildParams, dry_run: bool, verbose: bool) -> RunCommandResult:
    if image_params.airflow_image_name_with_tag == image_params.airflow_image_name:
        get_console().print(
            f"[info]Skip tagging {image_params.airflow_image_name} as latest as it is already 'latest'[/]"
        )
        return subprocess.CompletedProcess(returncode=0, args=[])
    return run_command(
        [
            "docker",
            "tag",
            image_params.airflow_image_name_with_tag,
            image_params.airflow_image_name,
        ],
        capture_output=True,
        verbose=verbose,
        dry_run=dry_run,
        check=False,
    )


def run_pull_and_verify_image(
    image_params: CommonBuildParams,
    dry_run: bool,
    verbose: bool,
    wait_for_image: bool,
    tag_as_latest: bool,
    poll_time: float,
    extra_pytest_args: Tuple,
) -> Tuple[int, str]:
    return_code, info = run_pull_image(
        image_params, dry_run, verbose, wait_for_image, tag_as_latest, poll_time
    )
    if return_code != 0:
        get_console().print(
            f"\n[error]Not running verification for {image_params.python} as pulling failed.[/]\n"
        )
    return verify_an_image(
        image_name=image_params.airflow_image_name_with_tag,
        image_type=image_params.image_type,
        dry_run=dry_run,
        verbose=verbose,
        slim_image=False,
        extra_pytest_args=extra_pytest_args,
    )


def just_pull_ci_image(
    github_repository, python_version: str, dry_run: bool, verbose: bool
) -> Tuple[ShellParams, RunCommandResult]:
    shell_params = ShellParams(
        verbose=verbose,
        mount_sources=MOUNT_ALL,
        python=python_version,
        github_repository=github_repository,
        skip_environment_initialization=True,
    )
    get_console().print(f"[info]Pulling {shell_params.airflow_image_name_with_tag}.[/]")
    pull_command_result = run_command(
        ["docker", "pull", shell_params.airflow_image_name_with_tag],
        verbose=verbose,
        dry_run=dry_run,
        check=True,
    )
    return shell_params, pull_command_result


def check_if_ci_image_available(
    github_repository: str, python_version: str, dry_run: bool, verbose: bool
) -> Tuple[ShellParams, RunCommandResult]:
    shell_params = ShellParams(
        verbose=verbose,
        mount_sources=MOUNT_ALL,
        python=python_version,
        github_repository=github_repository,
        skip_environment_initialization=True,
    )
    inspect_command_result = run_command(
        ["docker", "inspect", shell_params.airflow_image_name_with_tag],
        stdout=subprocess.DEVNULL,
        verbose=verbose,
        dry_run=dry_run,
        check=False,
    )
    return (
        shell_params,
        inspect_command_result,
    )


def find_available_ci_image(github_repository: str, dry_run: bool, verbose: bool) -> ShellParams:
    for python_version in ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS:
        shell_params, inspect_command_result = check_if_ci_image_available(
            github_repository, python_version, dry_run, verbose
        )
        if inspect_command_result.returncode == 0:
            get_console().print(
                f"[info]Running fix_ownership with {shell_params.airflow_image_name_with_tag}.[/]"
            )
            return shell_params
    shell_params, _ = just_pull_ci_image(
        github_repository, DEFAULT_PYTHON_MAJOR_MINOR_VERSION, dry_run, verbose
    )
    return shell_params
