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
import time
from typing import List, Tuple, Union

from airflow_breeze.build_image.ci.build_ci_image import mark_image_as_refreshed
from airflow_breeze.build_image.ci.build_ci_params import BuildCiParams
from airflow_breeze.build_image.prod.build_prod_params import BuildProdParams
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.parallel import check_async_run_results
from airflow_breeze.utils.run_tests import verify_an_image
from airflow_breeze.utils.run_utils import run_command


def run_pull_in_parallel(
    dry_run: bool,
    parallelism: int,
    image_params_list: Union[List[BuildCiParams], List[BuildProdParams]],
    python_version_list: List[str],
    verbose: bool,
    verify_image: bool,
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
    if not verify_image:
        results = [
            pool.apply_async(
                run_pull_image, args=(image_param, dry_run, verbose, wait_for_image, tag_as_latest, poll_time)
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
    image_params: Union[BuildCiParams, BuildProdParams],
    dry_run: bool,
    verbose: bool,
    wait_for_image: bool,
    tag_as_latest: bool,
    poll_time: float,
) -> Tuple[int, str]:
    """
    Pull image specified.
    :param image_params: Image parameters.
    :param dry_run: whether it's dry run
    :param verbose: whether it's verbose
    :param wait_for_image: whether we should wait for the image to be available
    :param tag_as_latest: tag the image as latest
    :param poll_time: what's the polling time between checks if images are there
    :return: Tuple of return code and description of the image pulled
    """
    get_console().print(
        f"\n[info]Pulling {image_params.the_image_type} image of airflow python version: "
        f"{image_params.python} image: {image_params.airflow_image_name_with_tag} "
        f"with wait for image: {wait_for_image}[/]\n"
    )
    while True:
        command_to_run = ["docker", "pull", image_params.airflow_image_name_with_tag]
        command_result = run_command(
            command_to_run,
            verbose=verbose,
            dry_run=dry_run,
            check=False,
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
                command_result = run_command(
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
                if command_result.returncode == 0 and isinstance(image_params, BuildCiParams):
                    mark_image_as_refreshed(image_params)
            return command_result.returncode, f"Image Python {image_params.python}"
        if wait_for_image:
            if verbose or dry_run:
                get_console().print(f"\n[info]Waiting for {poll_time} seconds.[/]\n")
            time.sleep(poll_time)
            continue
        else:
            get_console().print(
                f"\n[error]There was an error pulling the image {image_params.python}. Failing.[/]\n"
            )
            return command_result.returncode, f"Image Python {image_params.python}"


def run_pull_and_verify_image(
    image_params: Union[BuildCiParams, BuildProdParams],
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
        image_type=image_params.the_image_type,
        dry_run=dry_run,
        verbose=verbose,
        extra_pytest_args=extra_pytest_args,
    )
