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
import shutil
import sys
import threading
from signal import SIGTERM
from time import sleep
from typing import Iterable

import click

from airflow_breeze.commands.ci_image_commands import rebuild_or_pull_ci_image_if_needed
from airflow_breeze.commands.main_command import main
from airflow_breeze.global_constants import (
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    DOCKER_DEFAULT_PLATFORM,
    MOUNT_SELECTED,
    get_available_documentation_packages,
)
from airflow_breeze.params.build_ci_params import BuildCiParams
from airflow_breeze.params.doc_build_params import DocBuildParams
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.pre_commit_ids import PRE_COMMIT_LIST
from airflow_breeze.utils.cache import read_from_cache_file
from airflow_breeze.utils.common_options import (
    option_airflow_constraints_reference,
    option_airflow_extras,
    option_answer,
    option_backend,
    option_db_reset,
    option_dry_run,
    option_force_build,
    option_forward_credentials,
    option_github_repository,
    option_image_tag_for_running,
    option_include_mypy_volume,
    option_installation_package_format,
    option_integration,
    option_load_default_connection,
    option_load_example_dags,
    option_max_time,
    option_mount_sources,
    option_mssql_version,
    option_mysql_version,
    option_platform_single,
    option_postgres_version,
    option_python,
    option_use_airflow_version,
    option_use_packages_from_dist,
    option_verbose,
)
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.custom_param_types import BetterChoice, NotVerifiedBetterChoice
from airflow_breeze.utils.docker_command_utils import (
    DOCKER_COMPOSE_COMMAND,
    check_docker_resources,
    get_env_variables_for_docker_commands,
    get_extra_docker_flags,
    perform_environment_checks,
)
from airflow_breeze.utils.path_utils import (
    AIRFLOW_SOURCES_ROOT,
    cleanup_python_generated_files,
    create_mypy_volume_if_needed,
)
from airflow_breeze.utils.run_utils import (
    RunCommandResult,
    assert_pre_commit_installed,
    filter_out_none,
    run_command,
    run_compile_www_assets,
)
from airflow_breeze.utils.shared_options import get_dry_run, get_verbose, set_forced_answer
from airflow_breeze.utils.visuals import ASCIIART, ASCIIART_STYLE, CHEATSHEET, CHEATSHEET_STYLE

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Make sure that whatever you add here as an option is also
# Added in the "main" command in breeze.py. The min command above
# Is used for a shorthand of shell and except the extra
# Args it should have the same parameters.
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


class TimerThread(threading.Thread):
    def __init__(self, max_time: int):
        super().__init__(daemon=True)
        self.max_time = max_time

    def run(self):
        get_console().print(f"[info]Setting timer to fail after {self.max_time} s.")
        sleep(self.max_time)
        get_console().print(f"[error]The command took longer than {self.max_time} s. Failing!")
        os.killpg(os.getpgid(0), SIGTERM)


@main.command()
@option_python
@option_platform_single
@option_backend
@option_postgres_version
@option_mysql_version
@option_mssql_version
@option_forward_credentials
@option_force_build
@option_use_airflow_version
@option_airflow_extras
@option_airflow_constraints_reference
@option_use_packages_from_dist
@option_installation_package_format
@option_mount_sources
@option_integration
@option_db_reset
@option_image_tag_for_running
@option_max_time
@option_include_mypy_volume
@option_verbose
@option_dry_run
@option_github_repository
@option_answer
@click.argument("extra-args", nargs=-1, type=click.UNPROCESSED)
def shell(
    python: str,
    backend: str,
    integration: tuple[str],
    postgres_version: str,
    mysql_version: str,
    mssql_version: str,
    forward_credentials: bool,
    mount_sources: str,
    use_packages_from_dist: bool,
    package_format: str,
    use_airflow_version: str | None,
    airflow_extras: str,
    airflow_constraints_reference: str,
    force_build: bool,
    db_reset: bool,
    include_mypy_volume: bool,
    max_time: int | None,
    image_tag: str | None,
    platform: str | None,
    github_repository: str,
    extra_args: tuple,
):
    """Enter breeze environment. this is the default command use when no other is selected."""
    if get_verbose() or get_dry_run():
        get_console().print("\n[success]Welcome to breeze.py[/]\n")
        get_console().print(f"\n[success]Root of Airflow Sources = {AIRFLOW_SOURCES_ROOT}[/]\n")
    if max_time:
        TimerThread(max_time=max_time).start()
        set_forced_answer("yes")
    result = enter_shell(
        python=python,
        github_repository=github_repository,
        backend=backend,
        integration=integration,
        postgres_version=postgres_version,
        mysql_version=mysql_version,
        mssql_version=mssql_version,
        forward_credentials=str(forward_credentials),
        mount_sources=mount_sources,
        use_airflow_version=use_airflow_version,
        airflow_extras=airflow_extras,
        airflow_constraints_reference=airflow_constraints_reference,
        use_packages_from_dist=use_packages_from_dist,
        package_format=package_format,
        force_build=force_build,
        db_reset=db_reset,
        include_mypy_volume=include_mypy_volume,
        extra_args=extra_args if not max_time else ["exit"],
        image_tag=image_tag,
        platform=platform,
    )
    sys.exit(result.returncode)


@main.command(name="start-airflow")
@option_python
@option_platform_single
@option_backend
@option_postgres_version
@option_load_example_dags
@option_load_default_connection
@option_mysql_version
@option_mssql_version
@option_forward_credentials
@option_force_build
@option_use_airflow_version
@option_airflow_extras
@option_airflow_constraints_reference
@option_use_packages_from_dist
@option_installation_package_format
@option_mount_sources
@option_integration
@option_image_tag_for_running
@click.option(
    "--skip-asset-compilation",
    help="Skips compilation of assets when starting airflow even if the content of www changed "
    "(mutually exclusive with --dev-mode).",
    is_flag=True,
)
@click.option(
    "--dev-mode",
    help="Starts webserver in dev mode (assets are always recompiled in this case when starting) "
    "(mutually exclusive with --skip-asset-compilation).",
    is_flag=True,
)
@option_db_reset
@option_github_repository
@option_verbose
@option_dry_run
@option_answer
@click.argument("extra-args", nargs=-1, type=click.UNPROCESSED)
def start_airflow(
    python: str,
    backend: str,
    integration: tuple[str],
    postgres_version: str,
    load_example_dags: bool,
    load_default_connections: bool,
    mysql_version: str,
    mssql_version: str,
    forward_credentials: bool,
    mount_sources: str,
    use_airflow_version: str | None,
    airflow_extras: str,
    airflow_constraints_reference: str,
    use_packages_from_dist: bool,
    package_format: str,
    force_build: bool,
    skip_asset_compilation: bool,
    dev_mode: bool,
    image_tag: str | None,
    db_reset: bool,
    platform: str | None,
    extra_args: tuple,
    github_repository: str,
):
    """
    Enter breeze environment and starts all Airflow components in the tmux session.
    Compile assets if contents of www directory changed.
    """
    if dev_mode and skip_asset_compilation:
        get_console().print(
            "[warning]You cannot skip asset compilation in dev mode! Assets will be compiled!"
        )
        skip_asset_compilation = True
    if use_airflow_version is None and not skip_asset_compilation:
        run_compile_www_assets(dev=dev_mode, run_in_background=True)
    result = enter_shell(
        python=python,
        github_repository=github_repository,
        backend=backend,
        integration=integration,
        postgres_version=postgres_version,
        load_default_connections=load_default_connections,
        load_example_dags=load_example_dags,
        mysql_version=mysql_version,
        mssql_version=mssql_version,
        forward_credentials=str(forward_credentials),
        mount_sources=mount_sources,
        use_airflow_version=use_airflow_version,
        airflow_extras=airflow_extras,
        airflow_constraints_reference=airflow_constraints_reference,
        use_packages_from_dist=use_packages_from_dist,
        package_format=package_format,
        force_build=force_build,
        db_reset=db_reset,
        start_airflow=True,
        dev_mode=dev_mode,
        image_tag=image_tag,
        platform=platform,
        extra_args=extra_args,
    )
    sys.exit(result.returncode)


@main.command(name="build-docs")
@click.option("-d", "--docs-only", help="Only build documentation.", is_flag=True)
@click.option("-s", "--spellcheck-only", help="Only run spell checking.", is_flag=True)
@click.option(
    "--package-filter",
    help="List of packages to consider.",
    type=NotVerifiedBetterChoice(get_available_documentation_packages()),
    multiple=True,
)
@click.option(
    "--clean-build",
    help="Clean inventories of Inter-Sphinx documentation and generated APIs and sphinx artifacts "
    "before the build - useful for a clean build.",
    is_flag=True,
)
@click.option(
    "--for-production",
    help="Builds documentation for official release i.e. all links point to stable version. "
    "Implies --clean-build",
    is_flag=True,
)
@option_github_repository
@option_verbose
@option_dry_run
def build_docs(
    docs_only: bool,
    spellcheck_only: bool,
    for_production: bool,
    clean_build: bool,
    package_filter: tuple[str],
    github_repository: str,
):
    """Build documentation in the container."""
    if for_production and not clean_build:
        get_console().print("\n[warning]When building docs for production, clan-build is forced\n")
        clean_build = True
    perform_environment_checks()
    cleanup_python_generated_files()
    params = BuildCiParams(github_repository=github_repository, python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION)
    rebuild_or_pull_ci_image_if_needed(command_params=params)
    if clean_build:
        docs_dir = AIRFLOW_SOURCES_ROOT / "docs"
        for dir_name in ["_build", "_doctrees", "_inventory_cache", "_api"]:
            for directory in docs_dir.rglob(dir_name):
                get_console().print(f"[info]Removing {directory}")
                shutil.rmtree(directory, ignore_errors=True)
    ci_image_name = params.airflow_image_name
    doc_builder = DocBuildParams(
        package_filter=package_filter,
        docs_only=docs_only,
        spellcheck_only=spellcheck_only,
        for_production=for_production,
        skip_environment_initialization=True,
    )
    extra_docker_flags = get_extra_docker_flags(MOUNT_SELECTED)
    env = get_env_variables_for_docker_commands(params)
    cmd = [
        "docker",
        "run",
        "-t",
        *extra_docker_flags,
        "--pull",
        "never",
        ci_image_name,
        "/opt/airflow/scripts/in_container/run_docs_build.sh",
        *doc_builder.args_doc_builder,
    ]
    process = run_command(cmd, text=True, env=env, check=False)
    sys.exit(process.returncode)


@main.command(
    name="static-checks",
    help="Run static checks.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.option(
    "-t",
    "--type",
    help="Type(s) of the static checks to run (multiple can be added).",
    type=BetterChoice(PRE_COMMIT_LIST),
    multiple=True,
)
@click.option("-a", "--all-files", help="Run checks on all files.", is_flag=True)
@click.option("-f", "--file", help="List of files to run the checks on.", type=click.Path(), multiple=True)
@click.option(
    "-s", "--show-diff-on-failure", help="Show diff for files modified by the checks.", is_flag=True
)
@click.option(
    "-c",
    "--last-commit",
    help="Run checks for all files in last commit. Mutually exclusive with --commit-ref.",
    is_flag=True,
)
@click.option(
    "-r",
    "--commit-ref",
    help="Run checks for this commit reference only "
    "(can be any git commit-ish reference). "
    "Mutually exclusive with --last-commit.",
)
@option_github_repository
@option_verbose
@option_dry_run
@click.argument("precommit_args", nargs=-1, type=click.UNPROCESSED)
def static_checks(
    all_files: bool,
    show_diff_on_failure: bool,
    last_commit: bool,
    commit_ref: str,
    type: tuple[str],
    file: Iterable[str],
    precommit_args: tuple,
    github_repository: str,
):
    assert_pre_commit_installed()
    perform_environment_checks()
    command_to_execute = [sys.executable, "-m", "pre_commit", "run"]
    if last_commit and commit_ref:
        get_console().print("\n[error]You cannot specify both --last-commit and --commit-ref[/]\n")
        sys.exit(1)
    for single_check in type:
        command_to_execute.append(single_check)
    if all_files:
        command_to_execute.append("--all-files")
    if show_diff_on_failure:
        command_to_execute.append("--show-diff-on-failure")
    if last_commit:
        command_to_execute.extend(["--from-ref", "HEAD^", "--to-ref", "HEAD"])
    if commit_ref:
        command_to_execute.extend(["--from-ref", f"{commit_ref}^", "--to-ref", f"{commit_ref}"])
    if get_verbose() or get_dry_run():
        command_to_execute.append("--verbose")
    if file:
        command_to_execute.append("--files")
        command_to_execute.extend(file)
    if precommit_args:
        command_to_execute.extend(precommit_args)
    env = os.environ.copy()
    env["GITHUB_REPOSITORY"] = github_repository
    static_checks_result = run_command(
        command_to_execute,
        check=False,
        no_output_dump_on_exception=True,
        text=True,
        env=env,
    )
    if static_checks_result.returncode != 0:
        if os.environ.get("CI"):
            get_console().print("[error]There were errors during pre-commit check. They should be fixed[/]")
    sys.exit(static_checks_result.returncode)


@main.command(
    name="compile-www-assets",
    help="Compiles www assets.",
)
@click.option(
    "--dev",
    help="Run development version of assets compilation - it will not quit and automatically "
    "recompile assets on-the-fly when they are changed.",
    is_flag=True,
)
@option_verbose
@option_dry_run
def compile_www_assets(dev: bool):
    perform_environment_checks()
    assert_pre_commit_installed()
    compile_www_assets_result = run_compile_www_assets(dev=dev, run_in_background=False)
    if compile_www_assets_result.returncode != 0:
        get_console().print("[warn]New assets were generated[/]")
    sys.exit(0)


@main.command(name="stop", help="Stop running breeze environment.")
@click.option(
    "-p",
    "--preserve-volumes",
    help="Skip removing volumes when stopping Breeze.",
    is_flag=True,
)
@option_verbose
@option_dry_run
def stop(preserve_volumes: bool):
    perform_environment_checks()
    command_to_execute = [*DOCKER_COMPOSE_COMMAND, "down", "--remove-orphans"]
    if not preserve_volumes:
        command_to_execute.append("--volumes")
    shell_params = ShellParams(backend="all", include_mypy_volume=True)
    env_variables = get_env_variables_for_docker_commands(shell_params)
    run_command(command_to_execute, env=env_variables)


@main.command(name="exec", help="Joins the interactive shell of running airflow container.")
@option_verbose
@option_dry_run
@click.argument("exec_args", nargs=-1, type=click.UNPROCESSED)
def exec(exec_args: tuple):
    perform_environment_checks()
    container_running = find_airflow_container()
    if container_running:
        cmd_to_run = [
            "docker",
            "exec",
            "-it",
            container_running,
            "/opt/airflow/scripts/docker/entrypoint_exec.sh",
        ]
        if exec_args:
            cmd_to_run.extend(exec_args)
        process = run_command(
            cmd_to_run,
            check=False,
            no_output_dump_on_exception=False,
            text=True,
        )
        if not process:
            sys.exit(1)
        sys.exit(process.returncode)


def enter_shell(**kwargs) -> RunCommandResult:
    """
    Executes entering shell using the parameters passed as kwargs:

    * checks if docker version is good
    * checks if docker-compose version is good
    * updates kwargs with cached parameters
    * displays ASCIIART and CHEATSHEET unless disabled
    * build ShellParams from the updated kwargs
    * executes the command to drop the user to Breeze shell

    """
    perform_environment_checks()
    cleanup_python_generated_files()
    if read_from_cache_file("suppress_asciiart") is None:
        get_console().print(ASCIIART, style=ASCIIART_STYLE)
    if read_from_cache_file("suppress_cheatsheet") is None:
        get_console().print(CHEATSHEET, style=CHEATSHEET_STYLE)
    shell_params = ShellParams(**filter_out_none(**kwargs))
    rebuild_or_pull_ci_image_if_needed(command_params=shell_params)
    if shell_params.include_mypy_volume:
        create_mypy_volume_if_needed()
    shell_params.print_badge_info()
    cmd = [*DOCKER_COMPOSE_COMMAND, "run", "--service-ports", "-e", "BREEZE", "--rm", "airflow"]
    cmd_added = shell_params.command_passed
    env_variables = get_env_variables_for_docker_commands(shell_params)
    if cmd_added is not None:
        cmd.extend(["-c", cmd_added])
    if "arm64" in DOCKER_DEFAULT_PLATFORM:
        if shell_params.backend == "mysql":
            get_console().print("\n[error]MySQL is not supported on ARM architecture.[/]\n")
            sys.exit(1)
        if shell_params.backend == "mssql":
            get_console().print("\n[error]MSSQL is not supported on ARM architecture[/]\n")
            return 1
    command_result = run_command(
        cmd, env=env_variables, text=True, check=False, output_outside_the_group=True
    )
    if command_result.returncode == 0:
        return command_result
    else:
        get_console().print(f"[red]Error {command_result.returncode} returned[/]")
        if get_verbose():
            get_console().print(command_result.stderr)
        return command_result


def stop_exec_on_error(returncode: int):
    get_console().print("\n[error]ERROR in finding the airflow docker-compose process id[/]\n")
    sys.exit(returncode)


def find_airflow_container() -> str | None:
    exec_shell_params = ShellParams()
    check_docker_resources(exec_shell_params.airflow_image_name)
    exec_shell_params.print_badge_info()
    env_variables = get_env_variables_for_docker_commands(exec_shell_params)
    cmd = [*DOCKER_COMPOSE_COMMAND, "ps", "--all", "--filter", "status=running", "airflow"]
    docker_compose_ps_command = run_command(
        cmd, text=True, capture_output=True, env=env_variables, check=False
    )
    if get_dry_run():
        return "CONTAINER_ID"
    if docker_compose_ps_command.returncode != 0:
        if get_verbose():
            get_console().print(docker_compose_ps_command.stdout)
            get_console().print(docker_compose_ps_command.stderr)
        stop_exec_on_error(docker_compose_ps_command.returncode)
        return None

    output = docker_compose_ps_command.stdout
    container_info = output.strip().split("\n")
    if container_info:
        container_running = container_info[-1].split(" ")[0]
        if container_running.startswith("-"):
            # On docker-compose v1 we get '--------' as output here
            stop_exec_on_error(docker_compose_ps_command.returncode)
        return container_running
    else:
        stop_exec_on_error(1)
        return None
