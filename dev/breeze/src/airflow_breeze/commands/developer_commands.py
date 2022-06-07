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

import os
import sys
from typing import Iterable, Optional, Tuple

import rich_click as click

from airflow_breeze.commands.ci_image_commands import rebuild_ci_image_if_needed
from airflow_breeze.commands.main_command import main
from airflow_breeze.global_constants import (
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    MOUNT_SELECTED,
    get_available_packages,
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
    option_debian_version,
    option_dry_run,
    option_force_build,
    option_forward_credentials,
    option_github_repository,
    option_installation_package_format,
    option_integration,
    option_load_default_connection,
    option_load_example_dags,
    option_mount_sources,
    option_mssql_version,
    option_mysql_version,
    option_postgres_version,
    option_python,
    option_use_airflow_version,
    option_use_packages_from_dist,
    option_verbose,
)
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.custom_param_types import BetterChoice, NotVerifiedBetterChoice
from airflow_breeze.utils.docker_command_utils import (
    check_docker_resources,
    get_env_variables_for_docker_commands,
    get_extra_docker_flags,
    perform_environment_checks,
)
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT
from airflow_breeze.utils.run_utils import (
    RunCommandResult,
    assert_pre_commit_installed,
    filter_out_none,
    run_command,
)
from airflow_breeze.utils.visuals import ASCIIART, ASCIIART_STYLE, CHEATSHEET, CHEATSHEET_STYLE

DEVELOPER_COMMANDS = {
    "name": "Developer tools",
    "commands": [
        "shell",
        "start-airflow",
        "exec",
        "stop",
        "build-docs",
        "static-checks",
    ],
}

DEVELOPER_PARAMETERS = {
    "breeze": [
        {
            "name": "Basic flags for the default (shell) command",
            "options": [
                "--python",
                "--backend",
                "--postgres-version",
                "--mysql-version",
                "--mssql-version",
                "--integration",
                "--forward-credentials",
                "--db-reset",
            ],
        },
        {
            "name": "Advanced flags for the default (shell) command",
            "options": [
                "--use-airflow-version",
                "--constraints-reference",
                "--airflow-extras",
                "--use-packages-from-dist",
                "--package-format",
                "--force-build",
                "--mount-sources",
                "--debian-version",
            ],
        },
    ],
    "breeze shell": [
        {
            "name": "Basic flags",
            "options": [
                "--python",
                "--backend",
                "--postgres-version",
                "--mysql-version",
                "--mssql-version",
                "--integration",
                "--forward-credentials",
                "--db-reset",
            ],
        },
        {
            "name": "Advanced flag for running",
            "options": [
                "--use-airflow-version",
                "--constraints-reference",
                "--airflow-extras",
                "--use-packages-from-dist",
                "--package-format",
                "--force-build",
                "--mount-sources",
                "--debian-version",
            ],
        },
    ],
    "breeze start-airflow": [
        {
            "name": "Basic flags",
            "options": [
                "--python",
                "--load-example-dags",
                "--load-default-connections",
                "--backend",
                "--postgres-version",
                "--mysql-version",
                "--mssql-version",
                "--integration",
                "--forward-credentials",
                "--db-reset",
            ],
        },
        {
            "name": "Advanced flag for running",
            "options": [
                "--use-airflow-version",
                "--constraints-reference",
                "--airflow-extras",
                "--use-packages-from-dist",
                "--package-format",
                "--force-build",
                "--mount-sources",
            ],
        },
    ],
    "breeze exec": [
        {"name": "Drops in the interactive shell of active airflow container"},
    ],
    "breeze stop": [
        {
            "name": "Stop flags",
            "options": [
                "--preserve-volumes",
            ],
        },
    ],
    "breeze build-docs": [
        {
            "name": "Doc flags",
            "options": [
                "--docs-only",
                "--spellcheck-only",
                "--for-production",
                "--package-filter",
            ],
        },
    ],
    "breeze static-checks": [
        {
            "name": "Pre-commit flags",
            "options": [
                "--type",
                "--file",
                "--all-files",
                "--show-diff-on-failure",
                "--last-commit",
            ],
        },
    ],
}


# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Make sure that whatever you add here as an option is also
# Added in the "main" command in breeze.py. The min command above
# Is used for a shorthand of shell and except the extra
# Args it should have the same parameters.
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


@main.command()
@option_verbose
@option_dry_run
@option_python
@option_backend
@option_debian_version
@option_github_repository
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
@option_answer
@click.argument('extra-args', nargs=-1, type=click.UNPROCESSED)
def shell(
    verbose: bool,
    dry_run: bool,
    python: str,
    github_repository: str,
    backend: str,
    integration: Tuple[str],
    postgres_version: str,
    mysql_version: str,
    mssql_version: str,
    debian_version: str,
    forward_credentials: bool,
    mount_sources: str,
    use_packages_from_dist: bool,
    package_format: str,
    use_airflow_version: Optional[str],
    airflow_extras: str,
    airflow_constraints_reference: str,
    force_build: bool,
    db_reset: bool,
    answer: Optional[str],
    extra_args: Tuple,
):
    """Enter breeze.py environment. this is the default command use when no other is selected."""
    if verbose or dry_run:
        get_console().print("\n[success]Welcome to breeze.py[/]\n")
        get_console().print(f"\n[success]Root of Airflow Sources = {AIRFLOW_SOURCES_ROOT}[/]\n")
    enter_shell(
        verbose=verbose,
        dry_run=dry_run,
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
        extra_args=extra_args,
        answer=answer,
        debian_version=debian_version,
    )


@option_verbose
@main.command(name='start-airflow')
@option_dry_run
@option_python
@option_github_repository
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
@option_db_reset
@option_answer
@click.argument('extra-args', nargs=-1, type=click.UNPROCESSED)
def start_airflow(
    verbose: bool,
    dry_run: bool,
    python: str,
    github_repository: str,
    backend: str,
    integration: Tuple[str],
    postgres_version: str,
    load_example_dags: bool,
    load_default_connections: bool,
    mysql_version: str,
    mssql_version: str,
    forward_credentials: bool,
    mount_sources: str,
    use_airflow_version: Optional[str],
    airflow_extras: str,
    airflow_constraints_reference: str,
    use_packages_from_dist: bool,
    package_format: str,
    force_build: bool,
    db_reset: bool,
    answer: Optional[str],
    extra_args: Tuple,
):
    """Enter breeze.py environment and starts all Airflow components in the tmux session."""
    enter_shell(
        verbose=verbose,
        dry_run=dry_run,
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
        extra_args=extra_args,
        answer=answer,
    )


@main.command(name='build-docs')
@option_verbose
@option_dry_run
@option_github_repository
@click.option('-d', '--docs-only', help="Only build documentation.", is_flag=True)
@click.option('-s', '--spellcheck-only', help="Only run spell checking.", is_flag=True)
@click.option(
    '-p',
    '--for-production',
    help="Builds documentation for official release i.e. all links point to stable version.",
    is_flag=True,
)
@click.option(
    '-p',
    '--package-filter',
    help="List of packages to consider.",
    type=NotVerifiedBetterChoice(get_available_packages()),
    multiple=True,
)
def build_docs(
    verbose: bool,
    dry_run: bool,
    github_repository: str,
    docs_only: bool,
    spellcheck_only: bool,
    for_production: bool,
    package_filter: Tuple[str],
):
    """Build documentation in the container."""
    perform_environment_checks(verbose=verbose)
    params = BuildCiParams(github_repository=github_repository, python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION)
    rebuild_ci_image_if_needed(build_params=params, dry_run=dry_run, verbose=verbose)
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
    process = run_command(cmd, verbose=verbose, dry_run=dry_run, text=True, env=env, check=False)
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
    '-t',
    '--type',
    help="Type(s) of the static checks to run (multiple can be added).",
    type=BetterChoice(PRE_COMMIT_LIST),
    multiple=True,
)
@click.option('-a', '--all-files', help="Run checks on all files.", is_flag=True)
@click.option('-f', '--file', help="List of files to run the checks on.", type=click.Path(), multiple=True)
@click.option(
    '-s', '--show-diff-on-failure', help="Show diff for files modified by the checks.", is_flag=True
)
@click.option(
    '-c',
    '--last-commit',
    help="Run checks for all files in last commit. Mutually exclusive with --commit-ref.",
    is_flag=True,
)
@click.option(
    '-r',
    '--commit-ref',
    help="Run checks for this commit reference only "
    "(can be any git commit-ish reference). "
    "Mutually exclusive with --last-commit.",
)
@option_verbose
@option_dry_run
@option_github_repository
@click.argument('precommit_args', nargs=-1, type=click.UNPROCESSED)
def static_checks(
    verbose: bool,
    dry_run: bool,
    github_repository: str,
    all_files: bool,
    show_diff_on_failure: bool,
    last_commit: bool,
    commit_ref: str,
    type: Tuple[str],
    file: Iterable[str],
    precommit_args: Tuple,
):
    assert_pre_commit_installed(verbose=verbose)
    perform_environment_checks(verbose=verbose)
    command_to_execute = [sys.executable, "-m", "pre_commit", 'run']
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
    if verbose or dry_run:
        command_to_execute.append("--verbose")
    if file:
        command_to_execute.append("--files")
        command_to_execute.extend(file)
    if precommit_args:
        command_to_execute.extend(precommit_args)
    env = os.environ.copy()
    env['GITHUB_REPOSITORY'] = github_repository
    static_checks_result = run_command(
        command_to_execute,
        verbose=verbose,
        dry_run=dry_run,
        check=False,
        no_output_dump_on_exception=True,
        text=True,
        env=env,
    )
    if static_checks_result.returncode != 0:
        get_console().print("[error]There were errors during pre-commit check. They should be fixed[/]")
    sys.exit(static_checks_result.returncode)


@main.command(name="stop", help="Stop running breeze environment.")
@option_verbose
@option_dry_run
@click.option(
    "-p",
    "--preserve-volumes",
    help="Skip removing volumes when stopping Breeze.",
    is_flag=True,
)
def stop(verbose: bool, dry_run: bool, preserve_volumes: bool):
    command_to_execute = ['docker-compose', 'down', "--remove-orphans"]
    if not preserve_volumes:
        command_to_execute.append("--volumes")
    shell_params = ShellParams(verbose=verbose, backend="all", include_mypy_volume=True)
    env_variables = get_env_variables_for_docker_commands(shell_params)
    run_command(command_to_execute, verbose=verbose, dry_run=dry_run, env=env_variables)


@main.command(name='exec', help='Joins the interactive shell of running airflow container')
@option_verbose
@option_dry_run
@click.argument('exec_args', nargs=-1, type=click.UNPROCESSED)
def exec(verbose: bool, dry_run: bool, exec_args: Tuple):
    perform_environment_checks(verbose=verbose)
    container_running = find_airflow_container(verbose, dry_run)
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
            verbose=verbose,
            dry_run=dry_run,
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
    verbose = kwargs['verbose']
    dry_run = kwargs['dry_run']
    perform_environment_checks(verbose=verbose)
    if read_from_cache_file('suppress_asciiart') is None:
        get_console().print(ASCIIART, style=ASCIIART_STYLE)
    if read_from_cache_file('suppress_cheatsheet') is None:
        get_console().print(CHEATSHEET, style=CHEATSHEET_STYLE)
    enter_shell_params = ShellParams(**filter_out_none(**kwargs))
    enter_shell_params.include_mypy_volume = True
    rebuild_ci_image_if_needed(build_params=enter_shell_params, dry_run=dry_run, verbose=verbose)
    return run_shell(verbose, dry_run, enter_shell_params)


def run_shell(verbose: bool, dry_run: bool, shell_params: ShellParams) -> RunCommandResult:
    """
    Executes a shell command built from params passed.
    * prints information about the build
    * constructs docker compose command to enter shell
    * executes it

    :param verbose: print commands when running
    :param dry_run: do not execute "write" commands - just print what would happen
    :param shell_params: parameters of the execution
    """
    shell_params.print_badge_info()
    cmd = ['docker-compose', 'run', '--service-ports', "-e", "BREEZE", '--rm', 'airflow']
    cmd_added = shell_params.command_passed
    env_variables = get_env_variables_for_docker_commands(shell_params)
    if cmd_added is not None:
        cmd.extend(['-c', cmd_added])

    command_result = run_command(
        cmd, verbose=verbose, dry_run=dry_run, env=env_variables, text=True, check=False
    )
    if command_result.returncode == 0:
        return command_result
    else:
        get_console().print(f"[red]Error {command_result.returncode} returned[/]")
        if verbose:
            get_console().print(command_result.stderr)
        return command_result


def stop_exec_on_error(returncode: int):
    get_console().print('\n[error]ERROR in finding the airflow docker-compose process id[/]\n')
    sys.exit(returncode)


def find_airflow_container(verbose, dry_run) -> Optional[str]:
    exec_shell_params = ShellParams(verbose=verbose, dry_run=dry_run)
    check_docker_resources(exec_shell_params.airflow_image_name, verbose=verbose, dry_run=dry_run)
    exec_shell_params.print_badge_info()
    env_variables = get_env_variables_for_docker_commands(exec_shell_params)
    cmd = ['docker-compose', 'ps', '--all', '--filter', 'status=running', 'airflow']
    docker_compose_ps_command = run_command(
        cmd, verbose=verbose, dry_run=dry_run, text=True, capture_output=True, env=env_variables, check=False
    )
    if dry_run:
        return "CONTAINER_ID"
    if docker_compose_ps_command.returncode != 0:
        if verbose:
            get_console().print(docker_compose_ps_command.stdout)
            get_console().print(docker_compose_ps_command.stderr)
        stop_exec_on_error(docker_compose_ps_command.returncode)
        return None

    output = docker_compose_ps_command.stdout
    container_info = output.strip().split('\n')
    if container_info:
        container_running = container_info[-1].split(' ')[0]
        if container_running.startswith('-'):
            # On docker-compose v1 we get '--------' as output here
            stop_exec_on_error(docker_compose_ps_command.returncode)
        return container_running
    else:
        stop_exec_on_error(1)
        return None
