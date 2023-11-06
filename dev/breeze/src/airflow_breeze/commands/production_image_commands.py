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

import os
import sys
from typing import Any

import click

from airflow_breeze.global_constants import ALLOWED_INSTALLATION_METHODS, DEFAULT_EXTRAS
from airflow_breeze.params.build_prod_params import BuildProdParams
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.common_options import (
    option_additional_dev_apt_command,
    option_additional_dev_apt_deps,
    option_additional_dev_apt_env,
    option_additional_extras,
    option_additional_pip_install_flags,
    option_additional_python_deps,
    option_additional_runtime_apt_command,
    option_additional_runtime_apt_deps,
    option_additional_runtime_apt_env,
    option_airflow_constraints_location,
    option_airflow_constraints_mode_prod,
    option_airflow_constraints_reference_build,
    option_build_progress,
    option_builder,
    option_commit_sha,
    option_debian_version,
    option_debug_resources,
    option_dev_apt_command,
    option_dev_apt_deps,
    option_docker_cache,
    option_dry_run,
    option_github_repository,
    option_github_token,
    option_image_name,
    option_image_tag_for_building,
    option_image_tag_for_pulling,
    option_image_tag_for_verifying,
    option_include_success_outputs,
    option_install_providers_from_sources,
    option_parallelism,
    option_platform_multiple,
    option_prepare_buildx_cache,
    option_pull,
    option_push,
    option_python,
    option_python_image,
    option_python_versions,
    option_run_in_parallel,
    option_runtime_apt_command,
    option_runtime_apt_deps,
    option_skip_cleanup,
    option_tag_as_latest,
    option_verbose,
    option_verify,
    option_version_suffix_for_pypi,
    option_wait_for_image,
)
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.custom_param_types import BetterChoice
from airflow_breeze.utils.docker_command_utils import (
    build_cache,
    check_remote_ghcr_io_commands,
    make_sure_builder_configured,
    perform_environment_checks,
    prepare_docker_build_command,
    warm_up_docker_builder,
)
from airflow_breeze.utils.image import run_pull_image, run_pull_in_parallel, tag_image_as_latest
from airflow_breeze.utils.parallel import DockerBuildxProgressMatcher, check_async_run_results, run_with_pool
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT, DOCKER_CONTEXT_DIR
from airflow_breeze.utils.python_versions import get_python_version_list
from airflow_breeze.utils.registry import login_to_github_docker_registry
from airflow_breeze.utils.run_tests import verify_an_image
from airflow_breeze.utils.run_utils import filter_out_none, fix_group_permissions, run_command
from airflow_breeze.utils.shared_options import get_dry_run, get_verbose


def run_build_in_parallel(
    image_params_list: list[BuildProdParams],
    python_version_list: list[str],
    parallelism: int,
    include_success_outputs: bool,
    skip_cleanup: bool,
    debug_resources: bool,
) -> None:
    warm_up_docker_builder(image_params_list[0])
    with ci_group(f"Building for {python_version_list}"):
        all_params = [f"PROD {image_params.python}" for image_params in image_params_list]
        with run_with_pool(
            parallelism=parallelism,
            all_params=all_params,
            debug_resources=debug_resources,
            progress_matcher=DockerBuildxProgressMatcher(),
        ) as (pool, outputs):
            results = [
                pool.apply_async(
                    run_build_production_image,
                    kwds={
                        "prod_image_params": image_params,
                        "output": outputs[index],
                    },
                )
                for index, image_params in enumerate(image_params_list)
            ]
    check_async_run_results(
        results=results,
        success="All images built correctly",
        outputs=outputs,
        include_success_outputs=include_success_outputs,
        skip_cleanup=skip_cleanup,
    )


def prepare_for_building_prod_image(prod_image_params: BuildProdParams):
    make_sure_builder_configured(params=prod_image_params)
    if prod_image_params.cleanup_context:
        clean_docker_context_files()
    check_docker_context_files(prod_image_params.install_packages_from_context)
    login_to_github_docker_registry(
        github_token=prod_image_params.github_token,
        output=None,
    )


@click.group(
    cls=BreezeGroup, name="prod-image", help="Tools that developers can use to manually manage PROD images"
)
def prod_image():
    pass


@prod_image.command(name="build")
@option_python
@option_debian_version
@option_run_in_parallel
@option_parallelism
@option_skip_cleanup
@option_debug_resources
@option_include_success_outputs
@option_python_versions
@option_platform_multiple
@option_github_token
@option_docker_cache
@option_image_tag_for_building
@option_prepare_buildx_cache
@option_push
@option_airflow_constraints_location
@option_airflow_constraints_mode_prod
@click.option(
    "--installation-method",
    help="Install Airflow from: sources or PyPI.",
    type=BetterChoice(ALLOWED_INSTALLATION_METHODS),
)
@option_install_providers_from_sources
@click.option(
    "--install-packages-from-context",
    help="Install wheels from local docker-context-files when building image. "
    "Implies --disable-airflow-repo-cache.",
    is_flag=True,
)
@click.option(
    "--use-constraints-for-context-packages",
    help="Uses constraints for context packages installation - "
    "either from constraints store in docker-context-files or from github.",
    is_flag=True,
)
@click.option(
    "--cleanup-context",
    help="Clean up docker context files before running build (cannot be used together"
    " with --install-packages-from-context).",
    is_flag=True,
)
@click.option(
    "--airflow-extras",
    default=",".join(DEFAULT_EXTRAS),
    show_default=True,
    help="Extras to install by default.",
)
@click.option("--disable-mysql-client-installation", help="Do not install MySQL client.", is_flag=True)
@click.option("--disable-mssql-client-installation", help="Do not install MsSQl client.", is_flag=True)
@click.option("--disable-postgres-client-installation", help="Do not install Postgres client.", is_flag=True)
@click.option(
    "--disable-airflow-repo-cache",
    help="Disable cache from Airflow repository during building.",
    is_flag=True,
)
@click.option(
    "--install-airflow-reference",
    help="Install Airflow using GitHub tag or branch.",
)
@option_airflow_constraints_reference_build
@click.option("-V", "--install-airflow-version", help="Install version of Airflow from PyPI.")
@option_additional_extras
@option_additional_dev_apt_deps
@option_additional_runtime_apt_deps
@option_additional_python_deps
@option_additional_dev_apt_command
@option_additional_dev_apt_env
@option_additional_runtime_apt_env
@option_additional_runtime_apt_command
@option_builder
@option_build_progress
@option_dev_apt_command
@option_dev_apt_deps
@option_python_image
@option_runtime_apt_command
@option_runtime_apt_deps
@option_tag_as_latest
@option_additional_pip_install_flags
@option_github_repository
@option_version_suffix_for_pypi
@option_commit_sha
@option_verbose
@option_dry_run
def build(
    run_in_parallel: bool,
    parallelism: int,
    skip_cleanup: bool,
    debug_resources: bool,
    include_success_outputs: bool,
    python_versions: str,
    **kwargs: dict[str, Any],
):
    """
    Build Production image. Include building multiple images for all or selected Python versions sequentially.
    """

    def run_build(prod_image_params: BuildProdParams) -> None:
        return_code, info = run_build_production_image(output=None, prod_image_params=prod_image_params)
        if return_code != 0:
            get_console().print(f"[error]Error when building image! {info}")
            sys.exit(return_code)

    perform_environment_checks()
    check_remote_ghcr_io_commands()
    parameters_passed = filter_out_none(**kwargs)

    fix_group_permissions()
    if run_in_parallel:
        python_version_list = get_python_version_list(python_versions)
        params_list: list[BuildProdParams] = []
        for python in python_version_list:
            params = BuildProdParams(**parameters_passed)
            params.python = python
            params_list.append(params)
        prepare_for_building_prod_image(prod_image_params=params_list[0])
        run_build_in_parallel(
            image_params_list=params_list,
            python_version_list=python_version_list,
            parallelism=parallelism,
            skip_cleanup=skip_cleanup,
            debug_resources=debug_resources,
            include_success_outputs=include_success_outputs,
        )
    else:
        params = BuildProdParams(**parameters_passed)
        prepare_for_building_prod_image(prod_image_params=params)
        run_build(prod_image_params=params)


@prod_image.command(name="pull")
@option_python
@option_run_in_parallel
@option_parallelism
@option_skip_cleanup
@option_debug_resources
@option_include_success_outputs
@option_python_versions
@option_github_token
@option_image_tag_for_pulling
@option_wait_for_image
@option_tag_as_latest
@option_verify
@option_github_repository
@option_verbose
@option_dry_run
@click.argument("extra_pytest_args", nargs=-1, type=click.UNPROCESSED)
def pull_prod_image(
    python: str,
    run_in_parallel: bool,
    parallelism: int,
    skip_cleanup: bool,
    debug_resources: bool,
    include_success_outputs,
    python_versions: str,
    github_token: str,
    image_tag: str,
    wait_for_image: bool,
    tag_as_latest: bool,
    verify: bool,
    github_repository: str,
    extra_pytest_args: tuple,
):
    """Pull and optionally verify Production images - possibly in parallel for all Python versions."""
    perform_environment_checks()
    check_remote_ghcr_io_commands()
    login_to_github_docker_registry(
        github_token=github_token,
        output=None,
    )
    if run_in_parallel:
        python_version_list = get_python_version_list(python_versions)
        prod_image_params_list = [
            BuildProdParams(
                image_tag=image_tag,
                python=python,
                github_repository=github_repository,
                github_token=github_token,
            )
            for python in python_version_list
        ]
        run_pull_in_parallel(
            parallelism=parallelism,
            skip_cleanup=skip_cleanup,
            debug_resources=debug_resources,
            include_success_outputs=include_success_outputs,
            image_params_list=prod_image_params_list,
            python_version_list=python_version_list,
            verify=verify,
            wait_for_image=wait_for_image,
            tag_as_latest=tag_as_latest,
            extra_pytest_args=extra_pytest_args if extra_pytest_args is not None else (),
        )
    else:
        image_params = BuildProdParams(
            image_tag=image_tag, python=python, github_repository=github_repository, github_token=github_token
        )
        return_code, info = run_pull_image(
            image_params=image_params,
            output=None,
            wait_for_image=wait_for_image,
            tag_as_latest=tag_as_latest,
            poll_time_seconds=10.0,
        )
        if return_code != 0:
            get_console().print(f"[error]There was an error when pulling PROD image: {info}[/]")
            sys.exit(return_code)


@prod_image.command(
    name="verify",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_python
@option_image_tag_for_verifying
@option_image_name
@option_pull
@click.option(
    "--slim-image",
    help="The image to verify is slim and non-slim tests should be skipped.",
    is_flag=True,
)
@option_github_repository
@option_github_token
@option_verbose
@option_dry_run
@click.argument("extra_pytest_args", nargs=-1, type=click.UNPROCESSED)
def verify(
    python: str,
    github_repository: str,
    image_name: str,
    image_tag: str | None,
    pull: bool,
    slim_image: bool,
    github_token: str,
    extra_pytest_args: tuple,
):
    """Verify Production image."""
    perform_environment_checks()
    login_to_github_docker_registry(
        github_token=github_token,
        output=None,
    )
    if image_name is None:
        build_params = BuildProdParams(
            python=python,
            image_tag=image_tag,
            github_repository=github_repository,
            github_token=github_token,
        )
        image_name = build_params.airflow_image_name_with_tag
    if pull:
        check_remote_ghcr_io_commands()
        command_to_run = ["docker", "pull", image_name]
        run_command(command_to_run, check=True)
    get_console().print(f"[info]Verifying PROD image: {image_name}[/]")
    return_code, info = verify_an_image(
        image_name=image_name,
        output=None,
        image_type="PROD",
        extra_pytest_args=extra_pytest_args,
        slim_image=slim_image,
    )
    sys.exit(return_code)


def clean_docker_context_files():
    """
    Cleans up docker context files folder - leaving only .README.md there.
    """
    if get_verbose() or get_dry_run():
        get_console().print("[info]Cleaning docker-context-files[/]")
    if get_dry_run():
        return
    context_files_to_delete = DOCKER_CONTEXT_DIR.rglob("*")
    for file_to_delete in context_files_to_delete:
        if file_to_delete.name != ".README.md":
            file_to_delete.unlink(missing_ok=True)


def check_docker_context_files(install_packages_from_context: bool):
    """
    Quick check - if we want to install from docker-context-files we expect some packages there but if
    we don't - we don't expect them, and they might invalidate Docker cache.

    This method exits with an error if what we see is unexpected for given operation.

    :param install_packages_from_context: whether we want to install from docker-context-files
    """
    context_file = DOCKER_CONTEXT_DIR.rglob("*")
    any_context_files = any(
        context.is_file()
        and context.name not in (".README.md", ".DS_Store")
        and not context.parent.name.startswith("constraints")
        for context in context_file
    )
    if not any_context_files and install_packages_from_context:
        get_console().print("[warning]\nERROR! You want to install packages from docker-context-files")
        get_console().print("[warning]\n but there are no packages to install in this folder.")
        sys.exit(1)
    elif any_context_files and not install_packages_from_context:
        get_console().print(
            "[warning]\n ERROR! There are some extra files in docker-context-files except README.md"
        )
        get_console().print("[warning]\nAnd you did not choose --install-packages-from-context flag")
        get_console().print(
            "[warning]\nThis might result in unnecessary cache invalidation and long build times"
        )
        get_console().print("[warning]Please restart the command with --cleanup-context switch\n")
        sys.exit(1)


def run_build_production_image(
    prod_image_params: BuildProdParams,
    output: Output | None,
) -> tuple[int, str]:
    """
    Builds PROD image:

      * fixes group permissions for files (to improve caching when umask is 002)
      * converts all the parameters received via kwargs into BuildProdParams (including cache)
      * prints info about the image to build
      * removes docker-context-files if requested
      * performs quick check if the files are present in docker-context-files if expected
      * logs int to docker registry on CI if build cache is being executed
      * removes "tag" for previously build image so that inline cache uses only remote image
      * constructs docker-compose command to run based on parameters passed
      * run the build command
      * update cached information that the build completed and saves checksums of all files
        for quick future check if the build is needed



    :param prod_image_params: PROD image parameters
    :param output: output redirection
    """
    if (
        prod_image_params.is_multi_platform()
        and not prod_image_params.push
        and not prod_image_params.prepare_buildx_cache
    ):
        get_console(output=output).print(
            "\n[red]You cannot use multi-platform build without using --push flag"
            " or preparing buildx cache![/]\n"
        )
        return 1, "Error: building multi-platform image without --push."
    get_console(output=output).print(f"\n[info]Building PROD Image for Python {prod_image_params.python}\n")
    if prod_image_params.prepare_buildx_cache:
        build_command_result = build_cache(image_params=prod_image_params, output=output)
    else:
        env = os.environ.copy()
        env["DOCKER_BUILDKIT"] = "1"
        build_command_result = run_command(
            prepare_docker_build_command(
                image_params=prod_image_params,
            ),
            cwd=AIRFLOW_SOURCES_ROOT,
            check=False,
            env=env,
            text=True,
            output=output,
        )
        if build_command_result.returncode == 0 and prod_image_params.tag_as_latest:
            build_command_result = tag_image_as_latest(image_params=prod_image_params, output=output)
    return build_command_result.returncode, f"Image build: {prod_image_params.python}"
