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
import shlex
import sys
import time
from copy import deepcopy
from re import match
from typing import IO, Dict, List, Optional, Tuple

import click

from airflow_breeze.commands.ci_image_commands import rebuild_ci_image_if_needed
from airflow_breeze.commands.main_command import main
from airflow_breeze.global_constants import (
    ALLOWED_PLATFORMS,
    CURRENT_PYTHON_MAJOR_MINOR_VERSIONS,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    MOUNT_ALL,
    MOUNT_SELECTED,
    MULTI_PLATFORM,
)
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.common_options import (
    argument_packages,
    option_airflow_constraints_mode_ci,
    option_airflow_constraints_reference,
    option_airflow_extras,
    option_answer,
    option_dry_run,
    option_github_repository,
    option_image_tag,
    option_installation_package_format,
    option_max_age,
    option_package_format,
    option_parallelism,
    option_python,
    option_python_versions,
    option_run_in_parallel,
    option_timezone,
    option_updated_on_or_after,
    option_use_airflow_version,
    option_use_packages_from_dist,
    option_verbose,
    option_version_suffix_for_pypi,
)
from airflow_breeze.utils.confirm import Answer, user_confirm
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.custom_param_types import BetterChoice
from airflow_breeze.utils.docker_command_utils import (
    get_env_variables_for_docker_commands,
    get_extra_docker_flags,
    perform_environment_checks,
)
from airflow_breeze.utils.find_newer_dependencies import find_newer_dependencies
from airflow_breeze.utils.parallel import check_async_run_results
from airflow_breeze.utils.python_versions import get_python_version_list
from airflow_breeze.utils.run_utils import RunCommandResult, run_command

RELEASE_MANAGEMENT_PARAMETERS = {
    "breeze prepare-airflow-package": [
        {"name": "Package flags", "options": ["--package-format", "--version-suffix-for-pypi"]}
    ],
    "breeze verify-provider-packages": [
        {
            "name": "Provider verification flags",
            "options": [
                "--use-airflow-version",
                "--airflow-constraints-reference",
                "--airflow-extras",
                "--use-packages-from-dist",
                "--package-format",
                "--debug",
            ],
        }
    ],
    "breeze prepare-provider-packages": [
        {
            "name": "Package flags",
            "options": [
                "--package-format",
                "--version-suffix-for-pypi",
                "--package-list-file",
                "--debug",
            ],
        }
    ],
    "breeze prepare-provider-documentation": [
        {
            "name": "Provider documentation preparation flags",
            "options": [
                "--debug",
            ],
        }
    ],
    "breeze generate-constraints": [
        {
            "name": "Generate constraints flags",
            "options": [
                "--image-tag",
                "--python",
                "--airflow-constraints-mode",
                "--debug",
            ],
        },
        {
            "name": "Parallel running",
            "options": [
                "--run-in-parallel",
                "--parallelism",
                "--python-versions",
            ],
        },
    ],
    "breeze release-prod-images": [
        {
            "name": "Release PROD IMAGE flags",
            "options": [
                "--airflow-version",
                "--dockerhub-repo",
                "--slim-images",
                "--limit-python",
                "--limit-platform",
                "--skip-latest",
            ],
        }
    ],
    "breeze find-newer-dependencies": [
        {
            "name": "Find newer dependencies flags",
            "options": [
                "--python",
                "--timezone",
                "--constraints-branch",
                "--updated-on-or-after",
                "--max-age",
            ],
        }
    ],
}

RELEASE_MANAGEMENT_COMMANDS = {
    "name": "Release management",
    "commands": [
        "verify-provider-packages",
        "prepare-provider-documentation",
        "prepare-provider-packages",
        "prepare-airflow-package",
        "release-prod-images",
        "generate-constraints",
        "find-newer-dependencies",
    ],
}

option_debug_release_management = click.option(
    "--debug",
    is_flag=True,
    help="Drop user in shell instead of running the command. Useful for debugging.",
    envvar='DEBUG',
)


def run_with_debug(
    params: ShellParams,
    command: List[str],
    verbose: bool,
    dry_run: bool,
    debug: bool,
    enable_input: bool = False,
    enabled_output_group: bool = False,
) -> RunCommandResult:
    env_variables = get_env_variables_for_docker_commands(params)
    extra_docker_flags = get_extra_docker_flags(mount_sources=params.mount_sources)
    if enable_input or debug:
        term_flag = "-it"
    else:
        term_flag = "-t"
    base_command = [
        "docker",
        "run",
        term_flag,
        *extra_docker_flags,
        "--pull",
        "never",
        params.airflow_image_name_with_tag,
    ]
    if debug:
        cmd_string = ' '.join([shlex.quote(s) for s in command if s != "-c"])
        base_command.extend(
            [
                "-c",
                f"""
echo -e '\\e[34mRun this command to debug:

    {cmd_string}

\\e[0m\n'; exec bash
""",
            ]
        )
        return run_command(
            base_command,
            verbose=verbose,
            dry_run=dry_run,
            env=env_variables,
            enabled_output_group=enabled_output_group,
        )
    else:
        base_command.extend(command)
        return run_command(
            base_command,
            enabled_output_group=enabled_output_group,
            verbose=verbose,
            dry_run=dry_run,
            env=env_variables,
            check=False,
        )


@main.command(
    name='prepare-airflow-package',
    help="Prepare sdist/whl package of Airflow.",
)
@option_verbose
@option_dry_run
@option_github_repository
@option_package_format
@option_version_suffix_for_pypi
@option_debug_release_management
def prepare_airflow_packages(
    verbose: bool,
    dry_run: bool,
    github_repository: str,
    package_format: str,
    version_suffix_for_pypi: str,
    debug: bool,
):
    perform_environment_checks(verbose=verbose)
    shell_params = ShellParams(
        verbose=verbose,
        github_repository=github_repository,
        python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
        package_format=package_format,
        version_suffix_for_pypi=version_suffix_for_pypi,
        skip_environment_initialization=True,
        install_providers_from_sources=False,
        mount_sources=MOUNT_ALL,
    )
    rebuild_ci_image_if_needed(build_params=shell_params, dry_run=dry_run, verbose=verbose)
    result_command = run_with_debug(
        params=shell_params,
        command=["/opt/airflow/scripts/in_container/run_prepare_airflow_packages.sh"],
        verbose=verbose,
        dry_run=dry_run,
        debug=debug,
        enabled_output_group=True,
    )
    sys.exit(result_command.returncode)


@main.command(
    name='prepare-provider-documentation',
    help="Prepare CHANGELOG, README and COMMITS information for providers.",
)
@option_verbose
@option_dry_run
@option_github_repository
@option_answer
@option_debug_release_management
@argument_packages
def prepare_provider_documentation(
    verbose: bool,
    dry_run: bool,
    github_repository: str,
    answer: Optional[str],
    debug: bool,
    packages: List[str],
):
    perform_environment_checks(verbose=verbose)
    shell_params = ShellParams(
        verbose=verbose,
        mount_sources=MOUNT_ALL,
        github_repository=github_repository,
        python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
        answer=answer,
        skip_environment_initialization=True,
    )
    rebuild_ci_image_if_needed(build_params=shell_params, dry_run=dry_run, verbose=verbose)
    cmd_to_run = ["/opt/airflow/scripts/in_container/run_prepare_provider_documentation.sh", *packages]
    result_command = run_with_debug(
        params=shell_params,
        command=cmd_to_run,
        enable_input=not answer or answer.lower() not in ['y', 'yes'],
        verbose=verbose,
        dry_run=dry_run,
        debug=debug,
    )
    sys.exit(result_command.returncode)


@main.command(
    name='prepare-provider-packages',
    help="Prepare sdist/whl packages of Airflow Providers.",
)
@option_verbose
@option_dry_run
@option_github_repository
@option_package_format
@option_version_suffix_for_pypi
@click.option(
    '--package-list-file',
    type=click.File('rt'),
    help='Read list of packages from text file (one package per line)',
)
@option_debug_release_management
@argument_packages
def prepare_provider_packages(
    verbose: bool,
    dry_run: bool,
    github_repository: str,
    package_format: str,
    version_suffix_for_pypi: str,
    package_list_file: IO,
    debug: bool,
    packages: Tuple[str, ...],
):
    perform_environment_checks(verbose=verbose)
    packages_list = list(packages)
    if package_list_file:
        packages_list.extend([package.strip() for package in package_list_file.readlines()])
    shell_params = ShellParams(
        verbose=verbose,
        mount_sources=MOUNT_ALL,
        github_repository=github_repository,
        python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
        package_format=package_format,
        skip_environment_initialization=True,
        version_suffix_for_pypi=version_suffix_for_pypi,
    )
    rebuild_ci_image_if_needed(build_params=shell_params, dry_run=dry_run, verbose=verbose)
    cmd_to_run = ["/opt/airflow/scripts/in_container/run_prepare_provider_packages.sh", *packages_list]
    result_command = run_with_debug(
        params=shell_params,
        command=cmd_to_run,
        verbose=verbose,
        dry_run=dry_run,
        debug=debug,
    )
    sys.exit(result_command.returncode)


def run_generate_constraints(
    shell_params: ShellParams, dry_run: bool, verbose: bool, debug: bool, parallel: bool = False
) -> Tuple[int, str]:
    cmd_to_run = [
        "/opt/airflow/scripts/in_container/run_generate_constraints.sh",
    ]
    generate_constraints_result = run_with_debug(
        params=shell_params,
        command=cmd_to_run,
        verbose=verbose,
        dry_run=dry_run,
        debug=debug,
        enabled_output_group=not parallel,
    )
    return (
        generate_constraints_result.returncode,
        f"Generate constraints Python {shell_params.python}:{shell_params.airflow_constraints_mode}",
    )


def run_generate_constraints_in_parallel(
    shell_params_list: List[ShellParams],
    python_version_list: List[str],
    parallelism: int,
    dry_run: bool,
    verbose: bool,
):
    """Run generate constraints in parallel"""
    get_console().print(
        f"\n[info]Generating constraints with parallelism = {parallelism} "
        f"for the constraints: {python_version_list}[/]"
    )
    pool = mp.Pool(parallelism)
    results = [
        pool.apply_async(
            run_generate_constraints,
            args=(shell_param, dry_run, verbose, False, True),
        )
        for shell_param in shell_params_list
    ]
    check_async_run_results(results)
    pool.close()


@main.command(
    name='generate-constraints',
    help="Generates pinned constraint files with all extras from setup.py in parallel.",
)
@option_verbose
@option_dry_run
@option_python
@option_github_repository
@option_run_in_parallel
@option_parallelism
@option_python_versions
@option_image_tag
@option_answer
@option_debug_release_management
@option_airflow_constraints_mode_ci
def generate_constraints(
    verbose: bool,
    dry_run: bool,
    python: str,
    github_repository: str,
    run_in_parallel: bool,
    parallelism: int,
    python_versions: str,
    image_tag: str,
    answer: Optional[str],
    debug: bool,
    airflow_constraints_mode: str,
):
    perform_environment_checks(verbose=verbose)
    if debug and run_in_parallel:
        get_console().print("\n[error]Cannot run --debug and --run-in-parallel at the same time[/]\n")
        sys.exit(1)
    if run_in_parallel:
        given_answer = user_confirm(
            f"Did you build all CI images {python_versions} with --upgrade-to-newer-dependencies flag set?",
        )
    else:
        given_answer = user_confirm(
            f"Did you build CI image {python} with --upgrade-to-newer-dependencies flag set?",
        )
    if given_answer != Answer.YES:
        if run_in_parallel:
            get_console().print("\n[info]Use this command to build the images:[/]\n")
            get_console().print(
                f"     breeze build-image --run-in-parallel --python-versions '{python_versions}' "
                f"--upgrade-to-newer-dependencies\n"
            )
        else:
            shell_params = ShellParams(
                image_tag=image_tag, python=python, github_repository=github_repository, answer=answer
            )
            get_console().print("\n[info]Use this command to build the image:[/]\n")
            get_console().print(
                f"     breeze build-image --python'{shell_params.python}' "
                f"--upgrade-to-newer-dependencies\n"
            )
        sys.exit(1)
    if run_in_parallel:
        python_version_list = get_python_version_list(python_versions)
        shell_params_list = [
            ShellParams(
                image_tag=image_tag,
                python=python,
                github_repository=github_repository,
                airflow_constraints_mode=airflow_constraints_mode,
                answer=answer,
            )
            for python in python_version_list
        ]
        run_generate_constraints_in_parallel(
            shell_params_list=shell_params_list,
            parallelism=parallelism,
            dry_run=dry_run,
            verbose=verbose,
            python_version_list=python_version_list,
        )
    else:
        shell_params = ShellParams(
            image_tag=image_tag,
            python=python,
            github_repository=github_repository,
            answer=answer,
            skip_environment_initialization=True,
            airflow_constraints_mode=airflow_constraints_mode,
        )
        return_code, info = run_generate_constraints(
            shell_params=shell_params,
            dry_run=dry_run,
            verbose=verbose,
            debug=debug,
            parallel=False,
        )
        if return_code != 0:
            get_console().print(f"[error]There was an error when generating constraints: {info}[/]")
            sys.exit(return_code)


@main.command(
    name='verify-provider-packages',
    help="Verifies if all provider code is following expectations for providers.",
)
@option_use_airflow_version
@option_airflow_extras
@option_airflow_constraints_reference
@option_use_packages_from_dist
@option_installation_package_format
@option_verbose
@option_dry_run
@option_github_repository
@option_debug_release_management
def verify_provider_packages(
    verbose: bool,
    dry_run: bool,
    use_airflow_version: Optional[str],
    airflow_constraints_reference: str,
    airflow_extras: str,
    use_packages_from_dist: bool,
    debug: bool,
    package_format: str,
    github_repository: str,
):
    perform_environment_checks(verbose=verbose)
    shell_params = ShellParams(
        verbose=verbose,
        mount_sources=MOUNT_SELECTED,
        github_repository=github_repository,
        python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
        use_airflow_version=use_airflow_version,
        airflow_extras=airflow_extras,
        airflow_constraints_reference=airflow_constraints_reference,
        use_packages_from_dist=use_packages_from_dist,
        package_format=package_format,
    )
    rebuild_ci_image_if_needed(build_params=shell_params, dry_run=dry_run, verbose=verbose)
    cmd_to_run = [
        "-c",
        "python /opt/airflow/scripts/in_container/verify_providers.py",
    ]
    result_command = run_with_debug(
        params=shell_params,
        command=cmd_to_run,
        verbose=verbose,
        dry_run=dry_run,
        debug=debug,
        enabled_output_group=True,
    )
    sys.exit(result_command.returncode)


def convert_build_args_dict_to_array_of_args(build_args: Dict[str, str]) -> List[str]:
    array_of_args = []
    for key, value in build_args.items():
        array_of_args.append("--build-arg")
        array_of_args.append(f'{key}={value}')
    return array_of_args


def alias_image(image_from: str, image_to: str, dry_run: bool, verbose: bool):
    get_console().print(f"[info]Creating {image_to} alias for {image_from}[/]")
    run_command(
        ["regctl", "image", "copy", "--force-recursive", "--digest-tags", image_from, image_to],
        dry_run=dry_run,
        verbose=verbose,
    )


@main.command(
    name="release-prod-images", help="Release production images to DockerHub (needs DockerHub permissions)."
)
@click.option('--airflow-version', required=True, help="Airflow version to release (2.3.0, 2.3.0rc1 etc.)")
@click.option(
    '--dockerhub-repo',
    default="apache/airflow",
    show_default=True,
    help="DockerHub repository for the images",
)
@click.option(
    '--slim-images',
    is_flag=True,
    help='Whether to prepare slim images instead of the regular ones.',
)
@click.option(
    '--limit-python',
    type=BetterChoice(CURRENT_PYTHON_MAJOR_MINOR_VERSIONS),
    help="Specific python to build slim images for (if not specified - the images are built for all"
    " available python versions)",
)
@click.option(
    '--limit-platform',
    type=BetterChoice(ALLOWED_PLATFORMS),
    default=MULTI_PLATFORM,
    show_default=True,
    help="Specific platform to build images for (if not specified, multiplatform images will be built.",
)
@click.option(
    '--skip-latest',
    is_flag=True,
    help="Whether to skip publishing the latest images (so that 'latest' images are not updated). "
    "This should only be used if you release image for previous branches. Automatically set when "
    "rc/alpha/beta images are built.",
)
@option_verbose
@option_dry_run
def release_prod_images(
    airflow_version: str,
    dockerhub_repo: str,
    slim_images: bool,
    limit_platform: str,
    limit_python: Optional[str],
    skip_latest: bool,
    verbose: bool,
    dry_run: bool,
):
    perform_environment_checks(verbose=verbose)
    rebuild_ci_image_if_needed(
        build_params=ShellParams(verbose=verbose, python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION),
        dry_run=dry_run,
        verbose=verbose,
    )
    if not match(r"^\d*\.\d*\.\d*$", airflow_version):
        get_console().print(
            f"[warning]Skipping latest image tagging as this is a pre-release version: {airflow_version}"
        )
        skip_latest = True
    else:
        if skip_latest:
            get_console().print("[info]Skipping latest image tagging as user requested it.[/]")
        else:
            get_console().print(
                "[info]Also tagging the images with latest tags as this is release version.[/]"
            )
    result_docker_buildx = run_command(
        ["docker", 'buildx', 'version'], check=False, dry_run=dry_run, verbose=verbose
    )
    if result_docker_buildx.returncode != 0:
        get_console().print("[error]Docker buildx plugin must be installed to release the images[/]")
        get_console().print()
        get_console().print("See https://docs.docker.com/buildx/working-with-buildx/ for installation info.")
        sys.exit(1)
    result_inspect_builder = run_command(
        ["docker", 'buildx', 'inspect', 'airflow_cache'], check=False, dry_run=dry_run, verbose=verbose
    )
    if result_inspect_builder.returncode != 0:
        get_console().print("[error]Regctl must be installed and on PATH to release the images[/]")
        get_console().print()
        get_console().print(
            "See https://github.com/apache/airflow/blob/main/dev/README_RELEASE_AIRFLOW.md"
            "#setting-up-cache-refreshing-with-hardware-armamd-support for "
            "instructions on setting it up."
        )
        sys.exit(1)
    result_regctl = run_command(["regctl", 'version'], check=False, dry_run=dry_run, verbose=verbose)
    if result_regctl.returncode != 0:
        get_console().print("[error]Regctl must be installed and on PATH to release the images[/]")
        get_console().print()
        get_console().print(
            "See https://github.com/regclient/regclient/blob/main/docs/regctl.md for installation info."
        )
        sys.exit(1)
    python_versions = CURRENT_PYTHON_MAJOR_MINOR_VERSIONS if limit_python is None else [limit_python]
    for python in python_versions:
        if slim_images:
            slim_build_args = {
                "AIRFLOW_EXTRAS": "",
                "AIRFLOW_CONSTRAINTS": "constraints-no-providers",
                "PYTHON_BASE_IMAGE": f"python:{python}-slim-bullseye",
                "AIRFLOW_VERSION": airflow_version,
            }
            get_console().print(f"[info]Building slim {airflow_version} image for Python {python}[/]")
            python_build_args = deepcopy(slim_build_args)
            slim_image_name = f"{dockerhub_repo}:slim-{airflow_version}-python{python}"
            docker_buildx_command = [
                "docker",
                "buildx",
                "build",
                "--builder",
                "airflow_cache",
                *convert_build_args_dict_to_array_of_args(build_args=python_build_args),
                "--platform",
                limit_platform,
                ".",
                "-t",
                slim_image_name,
                "--push",
            ]
            run_command(docker_buildx_command, verbose=verbose, dry_run=dry_run)
            if python == DEFAULT_PYTHON_MAJOR_MINOR_VERSION:
                alias_image(
                    slim_image_name,
                    f"{dockerhub_repo}:slim-{airflow_version}",
                    verbose=verbose,
                    dry_run=dry_run,
                )
        else:
            get_console().print(f"[info]Building regular {airflow_version} image for Python {python}[/]")
            image_name = f"{dockerhub_repo}:{airflow_version}-python{python}"
            regular_build_args = {
                "PYTHON_BASE_IMAGE": f"python:{python}-slim-bullseye",
                "AIRFLOW_VERSION": airflow_version,
            }
            docker_buildx_command = [
                "docker",
                "buildx",
                "build",
                "--builder",
                "airflow_cache",
                *convert_build_args_dict_to_array_of_args(build_args=regular_build_args),
                "--platform",
                limit_platform,
                ".",
                "-t",
                image_name,
                "--push",
            ]
            run_command(docker_buildx_command, verbose=verbose, dry_run=dry_run)
            if python == DEFAULT_PYTHON_MAJOR_MINOR_VERSION:
                alias_image(
                    image_name, f"{dockerhub_repo}:{airflow_version}", verbose=verbose, dry_run=dry_run
                )
    # in case of re-tagging the images might need few seconds to refresh multi-platform images in DockerHub
    time.sleep(10)
    if not skip_latest:
        get_console().print("[info]Replacing latest images with links to the newly created images.[/]")
        for python in python_versions:
            if slim_images:
                alias_image(
                    f"{dockerhub_repo}:slim-{airflow_version}-python{python}",
                    f"{dockerhub_repo}:slim-latest-python{python}",
                    verbose=verbose,
                    dry_run=dry_run,
                )
            else:
                alias_image(
                    f"{dockerhub_repo}:{airflow_version}-python{python}",
                    f"{dockerhub_repo}:latest-python{python}",
                    verbose=verbose,
                    dry_run=dry_run,
                )
        if slim_images:
            alias_image(
                f"{dockerhub_repo}:slim-{airflow_version}",
                f"{dockerhub_repo}:slim-latest",
                verbose=verbose,
                dry_run=dry_run,
            )
        else:
            alias_image(
                f"{dockerhub_repo}:{airflow_version}",
                f"{dockerhub_repo}:latest",
                verbose=verbose,
                dry_run=dry_run,
            )


@main.command(name="find-newer-dependencies", help="Finds which dependencies are being upgraded.")
@option_timezone
@option_airflow_constraints_reference
@option_python
@option_updated_on_or_after
@option_max_age
def breeze_find_newer_dependencies(
    airflow_constraints_reference: str, python: str, timezone: str, updated_on_or_after: str, max_age: int
):
    return find_newer_dependencies(
        constraints_branch=airflow_constraints_reference,
        python=python,
        timezone=timezone,
        updated_on_or_after=updated_on_or_after,
        max_age=max_age,
    )
