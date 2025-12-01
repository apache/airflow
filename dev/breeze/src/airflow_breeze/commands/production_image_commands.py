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

import sys
from copy import deepcopy
from pathlib import Path

import click

from airflow_breeze.commands.common_image_options import (
    option_additional_airflow_extras,
    option_additional_dev_apt_command,
    option_additional_dev_apt_deps,
    option_additional_dev_apt_env,
    option_additional_pip_install_flags,
    option_additional_python_deps,
    option_additional_runtime_apt_command,
    option_additional_runtime_apt_deps,
    option_additional_runtime_apt_env,
    option_airflow_constraints_reference_build,
    option_build_progress,
    option_debian_version,
    option_dev_apt_command,
    option_dev_apt_deps,
    option_disable_airflow_repo_cache,
    option_docker_cache,
    option_from_pr,
    option_from_run,
    option_github_token_for_images,
    option_image_file_dir,
    option_install_mysql_client_type,
    option_platform_multiple,
    option_prepare_buildx_cache,
    option_pull,
    option_push,
    option_python_image,
    option_runtime_apt_command,
    option_runtime_apt_deps,
    option_skip_image_file_deletion,
    option_verify,
    option_wait_for_image,
)
from airflow_breeze.commands.common_options import (
    option_answer,
    option_builder,
    option_commit_sha,
    option_debug_resources,
    option_docker_host,
    option_dry_run,
    option_github_repository,
    option_github_token,
    option_image_name,
    option_include_success_outputs,
    option_parallelism,
    option_platform_single,
    option_python,
    option_python_versions,
    option_run_in_parallel,
    option_skip_cleanup,
    option_use_uv_default_depends_on_installation_method,
    option_uv_http_timeout,
    option_verbose,
    option_version_suffix,
)
from airflow_breeze.commands.common_package_installation_options import (
    option_airflow_constraints_location,
    option_airflow_constraints_mode_prod,
)
from airflow_breeze.global_constants import (
    ALLOWED_INSTALLATION_METHODS,
    CONSTRAINTS_SOURCE_PROVIDERS,
    DEFAULT_EXTRAS,
)
from airflow_breeze.params.build_prod_params import BuildProdParams
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.custom_param_types import BetterChoice
from airflow_breeze.utils.docker_command_utils import (
    build_cache,
    check_remote_ghcr_io_commands,
    get_docker_build_env,
    make_sure_builder_configured,
    perform_environment_checks,
    prepare_docker_build_command,
    warm_up_docker_builder,
)
from airflow_breeze.utils.github import download_artifact_from_pr, download_artifact_from_run_id
from airflow_breeze.utils.image import run_pull_image, run_pull_in_parallel
from airflow_breeze.utils.parallel import (
    DockerBuildxProgressMatcher,
    ShowLastLineProgressMatcher,
    check_async_run_results,
    run_with_pool,
)
from airflow_breeze.utils.path_utils import (
    AIRFLOW_DIST_PATH,
    AIRFLOW_ROOT_PATH,
    DOCKER_CONTEXT_PATH,
    FAST_API_SIMPLE_AUTH_MANAGER_VITE_MANIFEST_PATH,
    UI_VITE_MANIFEST_PATH,
)
from airflow_breeze.utils.python_versions import get_python_version_list
from airflow_breeze.utils.run_tests import verify_an_image
from airflow_breeze.utils.run_utils import fix_group_permissions, run_command
from airflow_breeze.utils.shared_options import get_dry_run, get_verbose


def run_build_in_parallel(
    image_params_list: list[BuildProdParams],
    params_description_list: list[str],
    parallelism: int,
    include_success_outputs: bool,
    skip_cleanup: bool,
    debug_resources: bool,
) -> None:
    warm_up_docker_builder(image_params_list)
    with ci_group(f"Building for {params_description_list}"):
        all_params = [f"PROD {param_description}" for param_description in params_description_list]
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
                        "param_description": params_description_list[index],
                        "output": outputs[index],
                    },
                )
                for index, image_params in enumerate(image_params_list)
            ]
    check_async_run_results(
        results=results,
        success_message="All images built correctly",
        outputs=outputs,
        include_success_outputs=include_success_outputs,
        skip_cleanup=skip_cleanup,
    )


def prepare_for_building_prod_image(params: BuildProdParams):
    make_sure_builder_configured(params=params)
    if params.cleanup_context:
        clean_docker_context_files()
    check_docker_context_files(params.install_distributions_from_context)


option_prod_image_file_to_save = click.option(
    "--image-file",
    required=False,
    type=click.Path(exists=False, dir_okay=False, writable=True, path_type=Path),
    envvar="IMAGE_FILE",
    help="Optional file to save the image to.",
)

option_prod_image_file_to_load = click.option(
    "--image-file",
    required=False,
    type=click.Path(dir_okay=False, readable=True, path_type=Path, resolve_path=True),
    envvar="IMAGE_FILE",
    help="Optional file name to load the image from - name must follow the convention:"
    "`prod-image-save-{escaped_platform}-*-{python_version}.tar` where escaped_platform is one of "
    "linux_amd64 or linux_arm64.",
)


@click.group(
    cls=BreezeGroup, name="prod-image", help="Tools that developers can use to manually manage PROD images"
)
def prod_image_group():
    pass


@prod_image_group.command(name="build")
@click.option(
    "--installation-method",
    help="Install Airflow from: sources (.) or packages (apache-airflow).",
    type=BetterChoice(ALLOWED_INSTALLATION_METHODS),
    default=ALLOWED_INSTALLATION_METHODS[0],
    show_default=True,
)
@click.option(
    "--install-distributions-from-context",
    help="Install distributions from local docker-context-files when building image. "
    "Implies --disable-airflow-repo-cache",
    is_flag=True,
    envvar="INSTALL_DISTRIBUTIONS_FROM_CONTEXT",
)
@click.option(
    "--use-constraints-for-context-distributions",
    help="Uses constraints for context distributions installation - "
    "either from constraints store in docker-context-files or from github.",
    is_flag=True,
    envvar="USE_CONSTRAINTS_FOR_CONTEXT_DISTRIBUTIONS",
)
@click.option(
    "--cleanup-context",
    help="Clean up docker context files before running build (cannot be used together"
    " with --install-distributions-from-context).",
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
    "--install-airflow-reference",
    help="Install Airflow using GitHub tag or branch.",
)
@click.option("-V", "--install-airflow-version", help="Install version of Airflow from PyPI.")
@option_additional_airflow_extras
@option_additional_dev_apt_command
@option_additional_dev_apt_deps
@option_additional_dev_apt_env
@option_additional_pip_install_flags
@option_additional_python_deps
@option_additional_runtime_apt_command
@option_additional_runtime_apt_deps
@option_additional_runtime_apt_env
@option_airflow_constraints_location
@option_airflow_constraints_mode_prod
@option_airflow_constraints_reference_build
@option_answer
@option_build_progress
@option_builder
@option_commit_sha
@option_debian_version
@option_debug_resources
@option_dev_apt_command
@option_dev_apt_deps
@option_disable_airflow_repo_cache
@option_docker_cache
@option_docker_host
@option_dry_run
@option_github_repository
@option_github_token
@option_include_success_outputs
@option_install_mysql_client_type
@option_parallelism
@option_platform_multiple
@option_prepare_buildx_cache
@option_push
@option_python
@option_python_image
@option_python_versions
@option_run_in_parallel
@option_runtime_apt_command
@option_runtime_apt_deps
@click.option(
    "--skip-asset-compiled-check",
    help="Skip asset compilation check when building image even for source build method.",
    is_flag=True,
    envvar="SKIP_ASSET_COMPILED_CHECK",
)
@option_skip_cleanup
@option_use_uv_default_depends_on_installation_method
@option_uv_http_timeout
@option_verbose
@option_version_suffix
def build(
    additional_airflow_extras: str | None,
    additional_dev_apt_command: str | None,
    additional_dev_apt_deps: str | None,
    additional_dev_apt_env: str | None,
    additional_pip_install_flags: str | None,
    additional_python_deps: str | None,
    additional_runtime_apt_command: str | None,
    additional_runtime_apt_deps: str | None,
    additional_runtime_apt_env: str | None,
    airflow_constraints_location: str | None,
    airflow_constraints_mode: str,
    airflow_constraints_reference: str | None,
    airflow_extras: str,
    build_progress: str,
    builder: str,
    cleanup_context: bool,
    commit_sha: str | None,
    debian_version: str,
    debug_resources: bool,
    dev_apt_command: str | None,
    dev_apt_deps: str | None,
    disable_airflow_repo_cache: bool,
    disable_mssql_client_installation: bool,
    disable_mysql_client_installation: bool,
    disable_postgres_client_installation: bool,
    docker_cache: str,
    docker_host: str | None,
    github_repository: str,
    github_token: str | None,
    include_success_outputs,
    install_airflow_reference: str | None,
    install_airflow_version: str | None,
    install_mysql_client_type: str,
    install_distributions_from_context: bool,
    installation_method: str,
    parallelism: int,
    platform: str | None,
    prepare_buildx_cache: bool,
    push: bool,
    python: str,
    python_image: str | None,
    python_versions: str,
    run_in_parallel: bool,
    runtime_apt_command: str | None,
    runtime_apt_deps: str | None,
    skip_asset_compiled_check: bool,
    skip_cleanup: bool,
    use_constraints_for_context_distributions: bool,
    use_uv: bool | None,
    uv_http_timeout: int,
    version_suffix: str,
):
    """
    Build Production image. Include building multiple images for all or selected Python versions sequentially.
    """

    def run_build(prod_image_params: BuildProdParams) -> None:
        return_code, info = run_build_production_image(
            output=None,
            param_description=prod_image_params.python + prod_image_params.platform,
            prod_image_params=prod_image_params,
        )
        if return_code != 0:
            get_console().print(f"[error]Error when building image! {info}")
            sys.exit(return_code)

    if not install_airflow_version and not airflow_constraints_location:
        get_console().print(f"[yellow]Using {CONSTRAINTS_SOURCE_PROVIDERS} constraints mode[/]")
        airflow_constraints_mode = CONSTRAINTS_SOURCE_PROVIDERS

    airflow_wheel_found_in_dist = len([AIRFLOW_DIST_PATH.glob("apache_airflow-*.whl")]) > 0
    if installation_method == ".":
        if install_distributions_from_context and airflow_wheel_found_in_dist:
            get_console().print("[info]Installing Airflow from local distributions in docker context[/]")
        else:
            get_console().print("[info]Installing Airflow from sources[/]")
            if skip_asset_compiled_check:
                get_console().print(
                    "[info]Skipping asset compilation check as requested.[/]\n\n"
                    "You can still build the assets manually with\n\n   [info]breeze compile-assets[/]\n"
                )
            else:
                if not UI_VITE_MANIFEST_PATH.exists():
                    get_console().print(
                        f"\n[error]UI Vite manifest file {UI_VITE_MANIFEST_PATH} does not exist.[/]\n\n"
                        f"You should build the UI assets with\n\n   [info]breeze ui compile-assets[/]\n"
                    )
                    sys.exit(1)
                if not FAST_API_SIMPLE_AUTH_MANAGER_VITE_MANIFEST_PATH.exists():
                    get_console().print(
                        f"\n[error]UI Vite manifest file {FAST_API_SIMPLE_AUTH_MANAGER_VITE_MANIFEST_PATH} "
                        f"does not exist.[/]\n\n"
                        f"You should build the UI assets with\n\n   [info]breeze ui compile-assets[/]\n"
                    )
                    sys.exit(1)
    else:
        get_console().print("[info]Installing Airflow from packages[/]")

    if use_uv is None:
        if installation_method == ".":
            get_console().print("[warning]Since we are installing from sources - UV is used by default[/]")
            use_uv = True
        else:
            get_console().print("[warning]Since we are installing from sources - PIP is used by default[/]")
            use_uv = False

    perform_environment_checks()
    check_remote_ghcr_io_commands()
    base_build_params = BuildProdParams(
        additional_airflow_extras=additional_airflow_extras,
        additional_dev_apt_command=additional_dev_apt_command,
        additional_dev_apt_deps=additional_dev_apt_deps,
        additional_dev_apt_env=additional_dev_apt_env,
        additional_pip_install_flags=additional_pip_install_flags,
        additional_python_deps=additional_python_deps,
        additional_runtime_apt_command=additional_runtime_apt_command,
        additional_runtime_apt_deps=additional_runtime_apt_deps,
        additional_runtime_apt_env=additional_runtime_apt_env,
        airflow_constraints_location=airflow_constraints_location,
        airflow_constraints_mode=airflow_constraints_mode,
        airflow_constraints_reference=airflow_constraints_reference,
        airflow_extras=airflow_extras,
        build_progress=build_progress,
        builder=builder,
        cleanup_context=cleanup_context,
        commit_sha=commit_sha,
        debian_version=debian_version,
        dev_apt_command=dev_apt_command,
        dev_apt_deps=dev_apt_deps,
        docker_host=docker_host,
        disable_airflow_repo_cache=disable_airflow_repo_cache,
        disable_mssql_client_installation=disable_mssql_client_installation,
        disable_mysql_client_installation=disable_mysql_client_installation,
        disable_postgres_client_installation=disable_postgres_client_installation,
        docker_cache=docker_cache,
        github_repository=github_repository,
        github_token=github_token,
        install_airflow_reference=install_airflow_reference,
        install_airflow_version=install_airflow_version,
        install_mysql_client_type=install_mysql_client_type,
        install_distributions_from_context=install_distributions_from_context,
        installation_method=installation_method,
        prepare_buildx_cache=prepare_buildx_cache,
        push=push,
        python=python,
        python_image=python_image,
        runtime_apt_command=runtime_apt_command,
        runtime_apt_deps=runtime_apt_deps,
        use_constraints_for_context_distributions=use_constraints_for_context_distributions,
        use_uv=use_uv,
        uv_http_timeout=uv_http_timeout,
        version_suffix=version_suffix,
    )
    if platform:
        base_build_params.platform = platform
    fix_group_permissions()
    if run_in_parallel:
        params_list: list[BuildProdParams] = []
        if prepare_buildx_cache:
            platforms_list = base_build_params.platform.split(",")
            for platform in platforms_list:
                build_params = deepcopy(base_build_params)
                build_params.platform = platform
                params_list.append(build_params)
            prepare_for_building_prod_image(params=params_list[0])
            run_build_in_parallel(
                image_params_list=params_list,
                params_description_list=platforms_list,
                include_success_outputs=include_success_outputs,
                parallelism=parallelism,
                skip_cleanup=skip_cleanup,
                debug_resources=debug_resources,
            )
        else:
            python_version_list = get_python_version_list(python_versions)
            for python in python_version_list:
                params = deepcopy(base_build_params)
                params.python = python
                params_list.append(params)
            prepare_for_building_prod_image(params=params_list[0])
            run_build_in_parallel(
                image_params_list=params_list,
                params_description_list=python_version_list,
                parallelism=parallelism,
                skip_cleanup=skip_cleanup,
                debug_resources=debug_resources,
                include_success_outputs=include_success_outputs,
            )
    else:
        prepare_for_building_prod_image(params=base_build_params)
        run_build(prod_image_params=base_build_params)


@prod_image_group.command(name="pull")
@option_python
@option_run_in_parallel
@option_parallelism
@option_skip_cleanup
@option_debug_resources
@option_include_success_outputs
@option_python_versions
@option_github_token
@option_wait_for_image
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
    wait_for_image: bool,
    verify: bool,
    github_repository: str,
    extra_pytest_args: tuple,
):
    """Pull and optionally verify Production images - possibly in parallel for all Python versions."""
    perform_environment_checks()
    check_remote_ghcr_io_commands()
    if run_in_parallel:
        python_version_list = get_python_version_list(python_versions)
        prod_image_params_list = [
            BuildProdParams(
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
            extra_pytest_args=extra_pytest_args if extra_pytest_args is not None else (),
        )
    else:
        image_params = BuildProdParams(
            python=python, github_repository=github_repository, github_token=github_token
        )
        return_code, info = run_pull_image(
            image_params=image_params,
            output=None,
            wait_for_image=wait_for_image,
            poll_time_seconds=10.0,
        )
        if return_code != 0:
            get_console().print(f"[error]There was an error when pulling PROD image: {info}[/]")
            sys.exit(return_code)


def run_verify_in_parallel(
    image_params_list: list[BuildProdParams],
    python_version_list: list[str],
    extra_pytest_args: tuple[str, ...],
    include_success_outputs: bool,
    parallelism: int,
    skip_cleanup: bool,
    debug_resources: bool,
) -> None:
    with ci_group(f"Verifying PROD images for {python_version_list}"):
        all_params = [f"PROD {image_params.python}" for image_params in image_params_list]
        with run_with_pool(
            parallelism=parallelism,
            all_params=all_params,
            debug_resources=debug_resources,
            progress_matcher=ShowLastLineProgressMatcher(),
        ) as (pool, outputs):
            results = [
                pool.apply_async(
                    verify_an_image,
                    kwds={
                        "image_name": image_params.airflow_image_name,
                        "image_type": "PROD",
                        "slim_image": False,
                        "extra_pytest_args": extra_pytest_args,
                        "output": outputs[index],
                    },
                )
                for index, image_params in enumerate(image_params_list)
            ]
    check_async_run_results(
        results=results,
        success_message="All images verified",
        outputs=outputs,
        include_success_outputs=include_success_outputs,
        skip_cleanup=skip_cleanup,
    )


@prod_image_group.command(
    name="verify",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.option(
    "--slim-image",
    help="The image to verify is slim and non-slim tests should be skipped.",
    is_flag=True,
)
@click.option(
    "--manifest-file",
    type=click.Path(exists=True, dir_okay=False, file_okay=True, path_type=Path),
    help="Read digest of the image from the manifest file instead of using name and pulling it.",
)
@click.argument("extra_pytest_args", nargs=-1, type=click.UNPROCESSED)
@option_python
@option_python_versions
@option_image_name
@option_pull
@option_github_repository
@option_github_token
@option_run_in_parallel
@option_parallelism
@option_skip_cleanup
@option_include_success_outputs
@option_debug_resources
@option_verbose
@option_dry_run
def verify(
    python: str,
    python_versions: str,
    github_repository: str,
    image_name: str,
    manifest_file: Path | None,
    pull: bool,
    slim_image: bool,
    github_token: str,
    extra_pytest_args: tuple,
    run_in_parallel: bool,
    parallelism: int,
    skip_cleanup: bool,
    debug_resources: bool,
    include_success_outputs: bool,
):
    """Verify Production image."""
    perform_environment_checks()
    check_remote_ghcr_io_commands()
    if image_name and manifest_file:
        get_console().print(
            "[error]You cannot use --image-name and --manifest-file at the same time. Exiting[/"
        )
        sys.exit(1)
    if (pull or image_name or manifest_file) and run_in_parallel:
        get_console().print(
            "[error]You cannot use --pull,--image-name,--manifest-file and "
            "--run-in-parallel at the same time. Exiting[/]"
        )
        sys.exit(1)
    if run_in_parallel:
        base_build_params = BuildProdParams(
            python=python,
            github_repository=github_repository,
        )
        python_version_list = get_python_version_list(python_versions)
        params_list: list[BuildProdParams] = []
        for python in python_version_list:
            build_params = deepcopy(base_build_params)
            build_params.python = python
            params_list.append(build_params)
        run_verify_in_parallel(
            image_params_list=params_list,
            python_version_list=python_version_list,
            extra_pytest_args=extra_pytest_args,
            include_success_outputs=include_success_outputs,
            parallelism=parallelism,
            skip_cleanup=skip_cleanup,
            debug_resources=debug_resources,
        )
    else:
        if image_name is None:
            if manifest_file:
                import json

                manifest_dict = json.loads(manifest_file.read_text())
                name = manifest_dict["image.name"]
                digest = manifest_dict["containerimage.descriptor"]["digest"]
                image_name = f"{name}@{digest}"
            else:
                build_params = BuildProdParams(
                    python=python,
                    github_repository=github_repository,
                    github_token=github_token,
                )
                image_name = build_params.airflow_image_name
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


@prod_image_group.command(name="save")
@option_github_repository
@option_image_file_dir
@option_platform_single
@option_prod_image_file_to_save
@option_python
@option_verbose
@option_dry_run
def save(
    python: str,
    platform: str,
    github_repository: str,
    image_file: Path | None,
    image_file_dir: Path,
):
    """Save PROD image to a file."""
    perform_environment_checks()
    image_name = BuildProdParams(
        python=python,
        github_repository=github_repository,
    ).airflow_image_name
    with ci_group("Buildx disk usage"):
        run_command(["docker", "buildx", "du", "--verbose"], check=False)
    escaped_platform = platform.replace("/", "_")
    if not image_file:
        image_file_to_store = image_file_dir / f"prod-image-save-{escaped_platform}-{python}.tar"
    elif image_file.is_absolute():
        image_file_to_store = image_file
    else:
        image_file_to_store = image_file_dir / image_file
    get_console().print(f"[info]Saving Python PROD image {image_name} to {image_file_to_store}[/]")
    result = run_command(
        ["docker", "image", "save", "-o", image_file_to_store.as_posix(), image_name], check=False
    )
    if result.returncode != 0:
        get_console().print(f"[error]Error when saving image: {result.stdout}[/]")
        sys.exit(result.returncode)


@prod_image_group.command(name="load")
@option_dry_run
@option_from_run
@option_from_pr
@option_github_repository
@option_github_token_for_images
@option_image_file_dir
@option_platform_single
@option_prod_image_file_to_save
@option_python
@option_skip_image_file_deletion
@option_verbose
def load(
    from_run: str | None,
    from_pr: str | None,
    github_repository: str,
    github_token: str | None,
    image_file: Path | None,
    image_file_dir: Path,
    platform: str,
    python: str,
    skip_image_file_deletion: bool,
):
    """Load PROD image from a file."""
    perform_environment_checks()
    escaped_platform = platform.replace("/", "_")

    if from_run or from_pr and not github_token:
        get_console().print(
            "[error]The parameter `--github-token` must be provided if `--from-run` or `--from-pr` is "
            "provided. Exiting.[/]"
        )
        sys.exit(1)

    if not image_file:
        image_file_to_load = image_file_dir / f"prod-image-save-{escaped_platform}-{python}.tar"
    elif image_file.is_absolute() or image_file.exists():
        image_file_to_load = image_file
    else:
        image_file_to_load = image_file_dir / image_file
    if from_run:
        download_artifact_from_run_id(from_run, image_file_to_load, github_repository, github_token)
    elif from_pr:
        download_artifact_from_pr(from_pr, image_file_to_load, github_repository, github_token)

    if not image_file_to_load.exists():
        get_console().print(f"[error]The image {image_file_to_load} does not exist.[/]")
        sys.exit(1)
    if not image_file_to_load.name.endswith(f"-{python}.tar"):
        get_console().print(
            f"[error]The image file {image_file_to_load} does not end with '-{python}.tar'. Exiting.[/]"
        )
        sys.exit(1)
    if not image_file_to_load.name.startswith(f"prod-image-save-{escaped_platform}"):
        get_console().print(
            f"[error]The image file {image_file_to_load} does not start with 'prod-image-save-{escaped_platform}'"
            f". Exiting.[/]"
        )
        sys.exit(1)
    get_console().print(f"[info]Loading Python PROD image from {image_file_to_load}[/]")
    result = run_command(["docker", "image", "load", "-i", image_file_to_load.as_posix()], check=False)
    if result.returncode != 0:
        get_console().print(f"[error]Error when loading image: {result.stdout}[/]")
        sys.exit(result.returncode)
    if not skip_image_file_deletion:
        get_console().print(f"[info]Deleting image file {image_file_to_load}[/]")
        image_file_to_load.unlink()
    if get_verbose():
        run_command(["docker", "images", "-a"])


def clean_docker_context_files():
    """
    Cleans up docker context files folder - leaving only .README.md there.
    """
    if get_verbose() or get_dry_run():
        get_console().print("[info]Cleaning docker-context-files[/]")
    if get_dry_run():
        return
    context_files_to_delete = DOCKER_CONTEXT_PATH.rglob("*")
    for file_to_delete in context_files_to_delete:
        if file_to_delete.name != ".README.md":
            file_to_delete.unlink(missing_ok=True)


def check_docker_context_files(install_distributions_from_context: bool):
    """
    Quick check - if we want to install from docker-context-files we expect some packages there but if
    we don't - we don't expect them, and they might invalidate Docker cache.

    This method exits with an error if what we see is unexpected for given operation.

    :param install_distributions_from_context: whether we want to install from docker-context-files
    """
    context_file = DOCKER_CONTEXT_PATH.rglob("*")
    any_context_files = any(
        context.is_file()
        and context.name not in (".README.md", ".DS_Store")
        and not context.parent.name.startswith("constraints")
        for context in context_file
    )
    if not any_context_files and install_distributions_from_context:
        get_console().print("[warning]\nERROR! You want to install packages from docker-context-files")
        get_console().print("[warning]\n but there are no packages to install in this folder.")
        sys.exit(1)
    elif any_context_files and not install_distributions_from_context:
        get_console().print(
            "[warning]\n ERROR! There are some extra files in docker-context-files except README.md"
        )
        get_console().print("[warning]\nAnd you did not choose --install-distributions-from-context flag")
        get_console().print(
            "[warning]\nThis might result in unnecessary cache invalidation and long build times"
        )
        get_console().print("[warning]Please restart the command with --cleanup-context switch\n")
        sys.exit(1)


def run_build_production_image(
    prod_image_params: BuildProdParams,
    param_description: str,
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
    :param param_description: description of the parameters
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
    get_console(output=output).print(f"\n[info]Building PROD Image for {param_description}\n")
    if prod_image_params.prepare_buildx_cache:
        build_command_result = build_cache(image_params=prod_image_params, output=output)
    else:
        env = get_docker_build_env(prod_image_params)
        build_command_result = run_command(
            prepare_docker_build_command(
                image_params=prod_image_params,
            ),
            cwd=AIRFLOW_ROOT_PATH,
            check=False,
            env=env,
            text=True,
            output=output,
        )
    return build_command_result.returncode, f"Image build: {param_description}"
