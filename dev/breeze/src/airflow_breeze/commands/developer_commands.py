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
import platform
import re
import shlex
import shutil
import sys
import threading
from pathlib import Path
from signal import SIGTERM
from time import sleep

import click

from airflow_breeze.branch_defaults import DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.commands.ci_image_commands import rebuild_or_pull_ci_image_if_needed
from airflow_breeze.commands.common_options import (
    argument_doc_packages,
    option_airflow_extras,
    option_all_integration,
    option_allow_pre_releases,
    option_answer,
    option_auth_manager,
    option_backend,
    option_builder,
    option_clean_airflow_installation,
    option_db_reset,
    option_debug_components,
    option_debugger,
    option_docker_host,
    option_downgrade_pendulum,
    option_downgrade_sqlalchemy,
    option_dry_run,
    option_excluded_providers,
    option_force_lowest_dependencies,
    option_forward_credentials,
    option_github_repository,
    option_include_not_ready_providers,
    option_include_removed_providers,
    option_install_airflow_with_constraints_default_true,
    option_installation_distribution_format,
    option_keep_env_variables,
    option_max_time,
    option_mount_sources,
    option_mysql_version,
    option_no_db_cleanup,
    option_platform_single,
    option_postgres_version,
    option_project_name,
    option_python,
    option_run_db_tests_only,
    option_skip_db_tests,
    option_standalone_dag_processor,
    option_tty,
    option_upgrade_boto,
    option_upgrade_sqlalchemy,
    option_use_airflow_version,
    option_use_mprocs,
    option_use_uv,
    option_uv_http_timeout,
    option_verbose,
)
from airflow_breeze.commands.common_package_installation_options import (
    option_airflow_constraints_location,
    option_airflow_constraints_mode_ci,
    option_airflow_constraints_reference,
    option_install_selected_providers,
    option_providers_constraints_location,
    option_providers_constraints_mode_ci,
    option_providers_constraints_reference,
    option_providers_skip_constraints,
    option_use_distributions_from_dist,
)
from airflow_breeze.commands.main_command import cleanup, main
from airflow_breeze.commands.testing_commands import option_test_type
from airflow_breeze.global_constants import (
    ALLOWED_CELERY_BROKERS,
    ALLOWED_CELERY_EXECUTORS,
    ALLOWED_EXECUTORS,
    DEFAULT_ALLOWED_EXECUTOR,
    DEFAULT_CELERY_BROKER,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    GITHUB_REPO_BRANCH_PATTERN,
    MOUNT_ALL,
    START_AIRFLOW_ALLOWED_EXECUTORS,
    START_AIRFLOW_DEFAULT_ALLOWED_EXECUTOR,
)
from airflow_breeze.params.build_ci_params import BuildCiParams
from airflow_breeze.params.doc_build_params import DocBuildParams
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.confirm import Answer, user_confirm
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.docker_command_utils import (
    bring_compose_project_down,
    check_docker_resources,
    enter_shell,
    execute_command_in_shell,
    fix_ownership_using_docker,
    perform_environment_checks,
)
from airflow_breeze.utils.packages import expand_all_provider_distributions
from airflow_breeze.utils.path_utils import (
    AIRFLOW_ROOT_PATH,
    cleanup_python_generated_files,
)
from airflow_breeze.utils.platforms import get_normalized_platform
from airflow_breeze.utils.run_utils import (
    assert_prek_installed,
    run_command,
    run_compile_ui_assets,
)
from airflow_breeze.utils.shared_options import get_dry_run, get_verbose, set_forced_answer

CELERY_INTEGRATION = "celery"


def is_wsl() -> bool:
    """Detect if we are running inside WSL."""
    if platform.system().lower() != "linux":
        return False
    try:
        with open("/proc/version") as f:
            version_info = f.read().lower()
            return "microsoft" in version_info or "wsl" in version_info
    except FileNotFoundError:
        return False


def _determine_constraint_branch_used(airflow_constraints_reference: str, use_airflow_version: str | None):
    """
    Determine which constraints reference to use.

    When use-airflow-version is branch or version, we derive the constraints branch from it, unless
    someone specified the constraints branch explicitly.

    :param airflow_constraints_reference: the constraint reference specified (or default)
    :param use_airflow_version: which airflow version we are installing
    :return: the actual constraints reference to use
    """
    if use_airflow_version and airflow_constraints_reference == DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH:
        match_exact_version = re.match(r"^[0-9]+\.[0-9]+\.[0-9]+[0-9a-z.]*$", use_airflow_version)
        if match_exact_version:
            # If we are using an exact version, we use the constraints for that version
            get_console().print(
                f"[info]Using constraints for {use_airflow_version} - exact version specified."
            )
            return f"constraints-{use_airflow_version}"
        match_repo_branch = re.match(GITHUB_REPO_BRANCH_PATTERN, use_airflow_version)
        if match_repo_branch:
            branch = match_repo_branch.group(3)
            match_v_x_y_branch = re.match(r"v([0-9]+-[0-9]+)-(test|stable)", branch)
            if match_v_x_y_branch:
                branch_version = match_v_x_y_branch.group(1)
                get_console().print(f"[info]Using constraints for {branch_version} branch.")
                return f"constraints-{branch_version}"
            if branch == "main":
                get_console().print(
                    "[info]Using constraints for main branch - no specific version specified."
                )
                return DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
            get_console().print(
                f"[warning]Could not determine branch automatically from {use_airflow_version}. "
                f"using {DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH} but you can specify constraints by using "
                "--airflow-constraints-reference flag in breeze command."
            )
            return DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    return airflow_constraints_reference


class TimerThread(threading.Thread):
    def __init__(self, max_time: int):
        super().__init__(daemon=True)
        self.max_time = max_time

    def run(self):
        get_console().print(f"[info]Setting timer to fail after {self.max_time} s.")
        sleep(self.max_time)
        get_console().print(f"[error]The command took longer than {self.max_time} s. Failing!")
        os.killpg(os.getpgid(0), SIGTERM)


# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Make sure that whatever you add here as an option is also
# Added in the "main" command in breeze.py. The min command above
# Is used for a shorthand of shell and except the extra
# Args it should have the same parameters.
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

option_celery_broker = click.option(
    "--celery-broker",
    type=click.Choice(ALLOWED_CELERY_BROKERS, case_sensitive=False),
    help="Specify the celery message broker",
    default=DEFAULT_CELERY_BROKER,
    show_default=True,
)
option_celery_flower = click.option("--celery-flower", help="Start celery flower", is_flag=True)
option_executor_shell = click.option(
    "--executor",
    type=click.Choice(ALLOWED_EXECUTORS, case_sensitive=False),
    help="Specify the executor to use with shell command.",
    default=DEFAULT_ALLOWED_EXECUTOR,
    show_default=True,
)
option_force_build = click.option(
    "--force-build", help="Force image build no matter if it is determined as needed.", is_flag=True
)
option_include_mypy_volume = click.option(
    "--include-mypy-volume",
    help="Whether to include mounting of the mypy volume (useful for debugging mypy).",
    is_flag=True,
    envvar="INCLUDE_MYPY_VOLUME",
)
option_restart = click.option(
    "--restart",
    "--remove-orphans",
    help="Restart all containers before entering shell (also removes orphan containers).",
    is_flag=True,
    envvar="RESTART",
)
option_skip_environment_initialization = click.option(
    "--skip-environment-initialization",
    help="Skip running breeze entrypoint initialization - no user output, no db checks.",
    is_flag=True,
    envvar="SKIP_ENVIRONMENT_INITIALIZATION",
)
option_skip_image_upgrade_check = click.option(
    "--skip-image-upgrade-check",
    help="Skip checking if the CI image is up to date.",
    is_flag=True,
    envvar="SKIP_IMAGE_UPGRADE_CHECK",
)
option_warn_image_upgrade_needed = click.option(
    "--warn-image-upgrade-needed",
    help="Warn when image upgrade is needed even if --skip-upgrade-check is used.",
    is_flag=True,
    envvar="WARN_IMAGE_UPGRADE_NEEDED",
)
option_install_airflow_python_client = click.option(
    "--install-airflow-python-client",
    is_flag=True,
    help="Install airflow python client packages (--distribution-format determines type) from 'dist' folder "
    "when entering breeze.",
    envvar="INSTALL_AIRFLOW_PYTHON_CLIENT",
)

option_start_api_server_with_examples = click.option(
    "--start-api-server-with-examples",
    is_flag=True,
    help="Start minimal airflow api-server with examples (for testing purposes) when entering breeze.",
    envvar="START_API_SERVER_WITH_EXAMPLES",
)

option_load_example_dags = click.option(
    "-e",
    "--load-example-dags",
    help="Enable configuration to load example DAGs when starting Airflow.",
    is_flag=True,
    envvar="LOAD_EXAMPLES",
)

option_load_default_connections = click.option(
    "-c",
    "--load-default-connections",
    help="Enable configuration to load default connections when starting Airflow.",
    is_flag=True,
    envvar="LOAD_DEFAULT_CONNECTIONS",
)


@main.command()
@click.argument("extra-args", nargs=-1, type=click.UNPROCESSED)
@click.option("--quiet", is_flag=True, envvar="QUIET", help="Suppress initialization output when starting.")
@option_tty
@click.option(
    "--verbose-commands",
    help="Show details of commands executed.",
    is_flag=True,
    envvar="VERBOSE_COMMANDS",
)
@option_install_airflow_python_client
@option_start_api_server_with_examples
@option_airflow_constraints_location
@option_airflow_constraints_mode_ci
@option_airflow_constraints_reference
@option_airflow_extras
@option_answer
@option_auth_manager
@option_backend
@option_builder
@option_celery_broker
@option_celery_flower
@option_clean_airflow_installation
@option_db_reset
@option_docker_host
@option_downgrade_sqlalchemy
@option_downgrade_pendulum
@option_dry_run
@option_executor_shell
@option_excluded_providers
@option_force_build
@option_force_lowest_dependencies
@option_forward_credentials
@option_github_repository
@option_include_mypy_volume
@option_install_airflow_with_constraints_default_true
@option_install_selected_providers
@option_installation_distribution_format
@option_load_example_dags
@option_load_default_connections
@option_all_integration
@option_keep_env_variables
@option_max_time
@option_mount_sources
@option_mysql_version
@option_no_db_cleanup
@option_platform_single
@option_postgres_version
@option_project_name
@option_providers_constraints_location
@option_providers_constraints_mode_ci
@option_providers_constraints_reference
@option_providers_skip_constraints
@option_python
@option_restart
@option_run_db_tests_only
@option_skip_db_tests
@option_skip_environment_initialization
@option_skip_image_upgrade_check
@option_test_type
@option_warn_image_upgrade_needed
@option_standalone_dag_processor
@option_upgrade_boto
@option_upgrade_sqlalchemy
@option_use_airflow_version
@option_allow_pre_releases
@option_use_distributions_from_dist
@option_use_uv
@option_uv_http_timeout
@option_verbose
def shell(
    airflow_constraints_location: str,
    airflow_constraints_mode: str,
    airflow_constraints_reference: str,
    airflow_extras: str,
    auth_manager: str,
    backend: str,
    builder: str,
    celery_broker: str,
    celery_flower: bool,
    clean_airflow_installation: bool,
    db_reset: bool,
    downgrade_sqlalchemy: bool,
    downgrade_pendulum: bool,
    docker_host: str | None,
    executor: str,
    extra_args: tuple,
    excluded_providers: str,
    force_build: bool,
    force_lowest_dependencies: bool,
    forward_credentials: bool,
    github_repository: str,
    include_mypy_volume: bool,
    install_selected_providers: str,
    install_airflow_with_constraints: bool,
    install_airflow_python_client: bool,
    integration: tuple[str, ...],
    keep_env_variables: bool,
    load_example_dags: bool,
    load_default_connections: bool,
    max_time: int | None,
    mount_sources: str,
    mysql_version: str,
    no_db_cleanup: bool,
    distribution_format: str,
    platform: str | None,
    postgres_version: str,
    project_name: str,
    providers_constraints_location: str,
    providers_constraints_mode: str,
    providers_constraints_reference: str,
    providers_skip_constraints: bool,
    python: str,
    quiet: bool,
    restart: bool,
    run_db_tests_only: bool,
    skip_environment_initialization: bool,
    skip_db_tests: bool,
    skip_image_upgrade_check: bool,
    standalone_dag_processor: bool,
    start_api_server_with_examples: bool,
    test_type: str | None,
    tty: str,
    upgrade_boto: bool,
    upgrade_sqlalchemy: bool,
    use_airflow_version: str | None,
    allow_pre_releases: bool,
    use_distributions_from_dist: bool,
    use_uv: bool,
    uv_http_timeout: int,
    verbose_commands: bool,
    warn_image_upgrade_needed: bool,
):
    """Enter breeze environment. this is the default command use when no other is selected."""
    if get_verbose() or get_dry_run() and not quiet:
        get_console().print("\n[success]Welcome to breeze.py[/]\n")
        get_console().print(f"\n[success]Root of Airflow Sources = {AIRFLOW_ROOT_PATH}[/]\n")
    if max_time:
        TimerThread(max_time=max_time).start()
        set_forced_answer("yes")
    airflow_constraints_reference = _determine_constraint_branch_used(
        airflow_constraints_reference, use_airflow_version
    )
    platform = get_normalized_platform(platform)
    shell_params = ShellParams(
        airflow_constraints_location=airflow_constraints_location,
        airflow_constraints_mode=airflow_constraints_mode,
        airflow_constraints_reference=airflow_constraints_reference,
        airflow_extras=airflow_extras,
        allow_pre_releases=allow_pre_releases,
        auth_manager=auth_manager,
        backend=backend,
        builder=builder,
        celery_broker=celery_broker,
        celery_flower=celery_flower,
        clean_airflow_installation=clean_airflow_installation,
        db_reset=db_reset,
        downgrade_sqlalchemy=downgrade_sqlalchemy,
        downgrade_pendulum=downgrade_pendulum,
        docker_host=docker_host,
        excluded_providers=excluded_providers,
        executor=executor,
        extra_args=extra_args if not max_time else ["exit"],
        force_build=force_build,
        force_lowest_dependencies=force_lowest_dependencies,
        forward_credentials=forward_credentials,
        github_repository=github_repository,
        include_mypy_volume=include_mypy_volume,
        install_airflow_with_constraints=install_airflow_with_constraints,
        install_airflow_python_client=install_airflow_python_client,
        install_selected_providers=install_selected_providers,
        integration=integration,
        keep_env_variables=keep_env_variables,
        load_example_dags=load_example_dags,
        load_default_connections=load_default_connections,
        mount_sources=mount_sources,
        mysql_version=mysql_version,
        no_db_cleanup=no_db_cleanup,
        distribution_format=distribution_format,
        platform=platform,
        postgres_version=postgres_version,
        project_name=project_name,
        providers_constraints_location=providers_constraints_location,
        providers_constraints_mode=providers_constraints_mode,
        providers_constraints_reference=providers_constraints_reference,
        providers_skip_constraints=providers_skip_constraints,
        python=python,
        quiet=quiet,
        restart=restart,
        run_db_tests_only=run_db_tests_only,
        skip_db_tests=skip_db_tests,
        skip_image_upgrade_check=skip_image_upgrade_check,
        skip_environment_initialization=skip_environment_initialization,
        standalone_dag_processor=standalone_dag_processor,
        start_api_server_with_examples=start_api_server_with_examples,
        test_type=test_type,
        tty=tty,
        upgrade_boto=upgrade_boto,
        upgrade_sqlalchemy=upgrade_sqlalchemy,
        use_airflow_version=use_airflow_version,
        use_distributions_from_dist=use_distributions_from_dist,
        use_uv=use_uv,
        uv_http_timeout=uv_http_timeout,
        verbose_commands=verbose_commands,
        warn_image_upgrade_needed=warn_image_upgrade_needed,
    )
    perform_environment_checks(quiet=shell_params.quiet)
    rebuild_or_pull_ci_image_if_needed(command_params=shell_params)
    result = enter_shell(shell_params=shell_params)
    fix_ownership_using_docker()
    sys.exit(result.returncode)


option_executor_start_airflow = click.option(
    "--executor",
    type=click.Choice(START_AIRFLOW_ALLOWED_EXECUTORS, case_sensitive=False),
    help="Specify the executor to use with start-airflow (defaults to LocalExecutor "
    "or CeleryExecutor depending on the integration used).",
)


@main.command(name="start-airflow")
@click.option(
    "--skip-assets-compilation",
    help="Skips compilation of assets when starting airflow even if the content of www changed "
    "(mutually exclusive with --dev-mode).",
    is_flag=True,
)
@click.option(
    "--dev-mode",
    help="Starts api-server in dev mode (assets are always recompiled in this case when starting) "
    "(mutually exclusive with --skip-assets-compilation and --use-airflow-version).",
    is_flag=True,
)
@click.option(
    "--create-all-roles",
    help="Creates all user roles for testing with FabAuthManager (viewer, user, op, admin). "
    "SimpleAuthManager always has all roles available.",
    is_flag=True,
)
@click.argument("extra-args", nargs=-1, type=click.UNPROCESSED)
@option_airflow_constraints_location
@option_airflow_constraints_mode_ci
@option_airflow_constraints_reference
@option_airflow_extras
@option_auth_manager
@option_answer
@option_backend
@option_builder
@option_clean_airflow_installation
@option_celery_broker
@option_celery_flower
@option_db_reset
@option_debug_components
@option_debugger
@option_docker_host
@option_dry_run
@option_executor_start_airflow
@option_force_build
@option_forward_credentials
@option_github_repository
@option_installation_distribution_format
@option_install_selected_providers
@option_install_airflow_with_constraints_default_true
@option_all_integration
@option_load_default_connections
@option_load_example_dags
@option_mount_sources
@option_mysql_version
@option_platform_single
@option_postgres_version
@option_project_name
@option_providers_constraints_location
@option_providers_constraints_mode_ci
@option_providers_constraints_reference
@option_providers_skip_constraints
@option_python
@option_restart
@option_standalone_dag_processor
@option_use_mprocs
@option_use_uv
@option_uv_http_timeout
@option_use_airflow_version
@option_allow_pre_releases
@option_use_distributions_from_dist
@option_verbose
def start_airflow(
    airflow_constraints_mode: str,
    airflow_constraints_location: str,
    airflow_constraints_reference: str,
    airflow_extras: str,
    install_airflow_with_constraints: bool,
    allow_pre_releases: bool,
    auth_manager: str,
    backend: str,
    builder: str,
    celery_broker: str,
    celery_flower: bool,
    clean_airflow_installation: bool,
    db_reset: bool,
    debug_components: tuple[str, ...],
    debugger: str,
    dev_mode: bool,
    create_all_roles: bool,
    docker_host: str | None,
    executor: str | None,
    extra_args: tuple,
    force_build: bool,
    forward_credentials: bool,
    github_repository: str,
    integration: tuple[str, ...],
    install_selected_providers: str,
    load_default_connections: bool,
    load_example_dags: bool,
    mount_sources: str,
    mysql_version: str,
    distribution_format: str,
    platform: str | None,
    postgres_version: str,
    project_name: str,
    providers_constraints_location: str,
    providers_constraints_mode: str,
    providers_constraints_reference: str,
    providers_skip_constraints: bool,
    python: str,
    restart: bool,
    skip_assets_compilation: bool,
    standalone_dag_processor: bool,
    use_mprocs: bool,
    use_airflow_version: str | None,
    use_distributions_from_dist: bool,
    use_uv: bool,
    uv_http_timeout: int,
):
    """
    Enter breeze environment and starts all Airflow components in the tmux or mprocs session.
    Compile assets if contents of www directory changed.
    """
    if dev_mode and skip_assets_compilation:
        get_console().print(
            "[warning]You cannot skip asset compilation in dev mode! Assets will be compiled!"
        )
        skip_assets_compilation = True

    if dev_mode and use_airflow_version:
        get_console().print(
            "[error]You cannot set Airflow version in dev mode! Consider switching to the respective "
            "version branch if you need to use --dev-mode on a different Airflow version! \nExiting!!"
        )

        sys.exit(1)

    # Automatically enable file polling for hot reloading under WSL
    if dev_mode and is_wsl():
        os.environ["CHOKIDAR_USEPOLLING"] = "true"
        get_console().print(
            "[info]Detected WSL environment. Automatically enabled CHOKIDAR_USEPOLLING for hot reloading."
        )

    perform_environment_checks(quiet=False)
    if use_airflow_version is None and not skip_assets_compilation:
        assert_prek_installed()
        # Now with the /ui project, lets only do a static build of /www and focus on the /ui
        run_compile_ui_assets(dev=dev_mode, run_in_background=True, force_clean=False)
    airflow_constraints_reference = _determine_constraint_branch_used(
        airflow_constraints_reference, use_airflow_version
    )

    if not executor:
        if CELERY_INTEGRATION in integration:
            # Default to a celery executor if that's the integration being used
            executor = ALLOWED_CELERY_EXECUTORS[0]
        else:
            # Otherwise default to LocalExecutor
            executor = START_AIRFLOW_DEFAULT_ALLOWED_EXECUTOR

    get_console().print(f"[info]Airflow will be using: {executor} to execute the tasks.")

    platform = get_normalized_platform(platform)
    shell_params = ShellParams(
        airflow_constraints_location=airflow_constraints_location,
        airflow_constraints_mode=airflow_constraints_mode,
        airflow_constraints_reference=airflow_constraints_reference,
        airflow_extras=airflow_extras,
        allow_pre_releases=allow_pre_releases,
        auth_manager=auth_manager,
        backend=backend,
        builder=builder,
        celery_broker=celery_broker,
        celery_flower=celery_flower,
        clean_airflow_installation=clean_airflow_installation,
        debug_components=debug_components,
        debugger=debugger,
        db_reset=db_reset,
        dev_mode=dev_mode,
        create_all_roles=create_all_roles,
        docker_host=docker_host,
        executor=executor,
        extra_args=extra_args,
        force_build=force_build,
        forward_credentials=forward_credentials,
        github_repository=github_repository,
        integration=integration,
        install_selected_providers=install_selected_providers,
        install_airflow_with_constraints=install_airflow_with_constraints,
        load_default_connections=load_default_connections,
        load_example_dags=load_example_dags,
        mount_sources=mount_sources,
        mysql_version=mysql_version,
        distribution_format=distribution_format,
        platform=platform,
        postgres_version=postgres_version,
        project_name=project_name,
        providers_constraints_location=providers_constraints_location,
        providers_constraints_mode=providers_constraints_mode,
        providers_constraints_reference=providers_constraints_reference,
        providers_skip_constraints=providers_skip_constraints,
        python=python,
        restart=restart,
        standalone_dag_processor=standalone_dag_processor,
        start_airflow=True,
        use_airflow_version=use_airflow_version,
        use_distributions_from_dist=use_distributions_from_dist,
        use_mprocs=use_mprocs,
        use_uv=use_uv,
        uv_http_timeout=uv_http_timeout,
    )
    rebuild_or_pull_ci_image_if_needed(command_params=shell_params)
    result = enter_shell(shell_params=shell_params)
    fix_ownership_using_docker()
    if CELERY_INTEGRATION in integration and executor not in ALLOWED_CELERY_EXECUTORS:
        get_console().print(
            "[warning]A non-Celery executor was used with start-airflow in combination with the Celery "
            "integration, this will lead to some processes failing to start (e.g.  celery worker)\n"
        )

    sys.exit(result.returncode)


@main.command(name="build-docs")
@option_builder
@click.option(
    "--clean-build",
    is_flag=True,
    help="Cleans the build directory before building the documentation and removes all inventory "
    "cache (including external inventories).",
)
@click.option(
    "--refresh-airflow-inventories",
    is_flag=True,
    help="When set, only airflow package inventories will be refreshed, regardless "
    "if they are already downloaded. With `--clean-build` - everything is cleaned..",
)
@click.option("-d", "--docs-only", help="Only build documentation.", is_flag=True)
@click.option(
    "--include-commits", help="Include commits in the documentation.", is_flag=True, envvar="INCLUDE_COMMITS"
)
@option_dry_run
@option_github_repository
@option_include_not_ready_providers
@option_include_removed_providers
@click.option(
    "--one-pass-only",
    help="Builds documentation in one pass only. This is useful for debugging sphinx errors.",
    is_flag=True,
)
@click.option(
    "--package-filter",
    help="Filter(s) to use more than one can be specified. You can use glob pattern matching the "
    "full package name, for example `apache-airflow-providers-*`. Useful when you want to select"
    "several similarly named packages together.",
    type=str,
    multiple=True,
)
@click.option(
    "--distributions-list",
    envvar="DISTRIBUTIONS_LIST",
    type=str,
    help="Optional, contains space separated list of package ids that are processed for documentation "
    "building, and document publishing. It is an easier alternative to adding individual packages as"
    " arguments to every command. This overrides the packages passed as arguments.",
)
@click.option("-s", "--spellcheck-only", help="Only run spell checking.", is_flag=True)
@option_verbose
@option_answer
@argument_doc_packages
def build_docs(
    builder: str,
    clean_build: bool,
    refresh_airflow_inventories: bool,
    docs_only: bool,
    github_repository: str,
    include_not_ready_providers: bool,
    include_removed_providers: bool,
    include_commits: bool,
    one_pass_only: bool,
    package_filter: tuple[str, ...],
    distributions_list: str,
    spellcheck_only: bool,
    doc_packages: tuple[str, ...],
):
    """
    Build documents.
    """
    perform_environment_checks()
    fix_ownership_using_docker()
    cleanup_python_generated_files()
    build_params = BuildCiParams(
        github_repository=github_repository,
        python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
        builder=builder,
    )
    rebuild_or_pull_ci_image_if_needed(command_params=build_params)
    if clean_build:
        directories_to_clean = ["_build", "_doctrees", "_inventory_cache", "apis"]
    else:
        directories_to_clean = ["apis"]
    generated_path = AIRFLOW_ROOT_PATH / "generated"
    for dir_name in directories_to_clean:
        get_console().print("Removing all generated dirs.")
        for directory in generated_path.rglob(dir_name):
            get_console().print(f"[info]Removing {directory}")
            shutil.rmtree(directory, ignore_errors=True)
    if refresh_airflow_inventories and not clean_build:
        get_console().print("Removing airflow inventories.")
        package_globs = ["helm-chart", "docker-stack", "apache-airflow*"]
        for package_glob in package_globs:
            for directory in (generated_path / "_inventory_cache").rglob(package_glob):
                get_console().print(f"[info]Removing {directory}")
                shutil.rmtree(directory, ignore_errors=True)

    docs_list_as_tuple: tuple[str, ...] = ()
    if distributions_list and len(distributions_list):
        get_console().print(
            f"\n[info]Populating provider list from DISTRIBUTIONS_LIST env as {distributions_list}"
        )
        # Override doc_packages with values from DISTRIBUTIONS_LIST
        docs_list_as_tuple = tuple(distributions_list.split(" "))
    if doc_packages and docs_list_as_tuple:
        get_console().print(
            f"[warning]Both package arguments and --distributions-list / DISTRIBUTIONS_LIST passed. "
            f"Overriding to {docs_list_as_tuple}"
        )
    doc_packages = docs_list_as_tuple or doc_packages
    doc_builder = DocBuildParams(
        package_filter=package_filter,
        docs_only=docs_only,
        spellcheck_only=spellcheck_only,
        one_pass_only=one_pass_only,
        include_commits=include_commits,
        short_doc_packages=expand_all_provider_distributions(
            short_doc_packages=doc_packages,
            include_removed=include_removed_providers,
            include_not_ready=include_not_ready_providers,
        ),
    )
    cmd = "/opt/airflow/scripts/in_container/run_docs_build.sh " + " ".join(
        [shlex.quote(arg) for arg in doc_builder.args_doc_builder]
    )
    shell_params = ShellParams(
        github_repository=github_repository,
        python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
        mount_sources=MOUNT_ALL,
    )
    result = execute_command_in_shell(shell_params, project_name="docs", command=cmd)
    fix_ownership_using_docker()
    if result.returncode == 0:
        get_console().print(
            "Run ./docs/start_doc_server.sh for a lighter resource option and view "
            "the built docs at http://localhost:8000"
        )
    sys.exit(result.returncode)


@main.command(
    name="compile-ui-assets",
    help="Compiles ui assets.",
)
@click.option(
    "--dev",
    help="Run development version of assets compilation - it will not quit and automatically "
    "recompile assets on-the-fly when they are changed.",
    is_flag=True,
)
@click.option(
    "--force-clean",
    help="Force cleanup of compile assets before building them.",
    is_flag=True,
)
@option_verbose
@option_dry_run
def compile_ui_assets(dev: bool, force_clean: bool):
    perform_environment_checks()
    assert_prek_installed()
    compile_ui_assets_result = run_compile_ui_assets(
        dev=dev, run_in_background=False, force_clean=force_clean
    )
    if compile_ui_assets_result.returncode != 0:
        get_console().print("[warn]New assets were generated[/]")
    sys.exit(0)


@main.command(name="down", help="Stop running breeze environment.")
@click.option(
    "-p",
    "--preserve-volumes",
    help="Skip removing database volumes when stopping Breeze.",
    is_flag=True,
)
@click.option(
    "-c",
    "--cleanup-mypy-cache",
    help="Additionally cleanup MyPy cache.",
    is_flag=True,
)
@click.option(
    "-b",
    "--cleanup-build-cache",
    help="Additionally cleanup Build (pip/uv) cache.",
    is_flag=True,
)
@option_verbose
@option_dry_run
def down(preserve_volumes: bool, cleanup_mypy_cache: bool, cleanup_build_cache: bool):
    perform_environment_checks()
    shell_params = ShellParams(backend="all", include_mypy_volume=cleanup_mypy_cache)
    bring_compose_project_down(preserve_volumes=preserve_volumes, shell_params=shell_params)
    if cleanup_mypy_cache:
        command_to_execute = ["docker", "volume", "rm", "--force", "mypy-cache-volume"]
        run_command(command_to_execute)
    if cleanup_build_cache:
        command_to_execute = ["docker", "volume", "rm", "--force", "airflow-cache-volume"]
        run_command(command_to_execute)


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
    else:
        get_console().print("[error]No airflow containers are running[/]")
        sys.exit(1)


def stop_exec_on_error(returncode: int):
    get_console().print("\n[error]ERROR in finding the airflow docker-compose process id[/]\n")
    sys.exit(returncode)


def find_airflow_container() -> str | None:
    shell_params = ShellParams()
    check_docker_resources(shell_params.airflow_image_name)
    shell_params.print_badge_info()
    cmd = [
        "docker",
        "compose",
        "--project-name",
        shell_params.project_name,
        "ps",
        "--all",
        "--filter",
        "status=running",
        "airflow",
    ]
    docker_compose_ps_command = run_command(
        cmd, text=True, capture_output=True, check=False, env=shell_params.env_variables_for_docker_commands
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
    container_info = output.strip().splitlines()
    if container_info:
        container_running = container_info[-1].split(" ")[0]
        if container_running.startswith("-"):
            # On docker-compose v1 we get '--------' as output here
            stop_exec_on_error(docker_compose_ps_command.returncode)
        return container_running
    stop_exec_on_error(1)
    return None


@main.command(
    name="generate-migration-file", help="Autogenerate the alembic migration file for the ORM changes."
)
@option_builder
@option_github_repository
@click.option(
    "-m",
    "--message",
    help="Message to use for the migration",
    default="Empty message",
    show_default=True,
)
def autogenerate(
    builder: str,
    github_repository: str,
    message: str,
):
    """Autogenerate the alembic migration file."""
    perform_environment_checks()
    fix_ownership_using_docker()
    build_params = BuildCiParams(
        github_repository=github_repository, python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION, builder=builder
    )
    rebuild_or_pull_ci_image_if_needed(command_params=build_params)
    shell_params = ShellParams(
        github_repository=github_repository,
        python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    )
    cmd = f"/opt/airflow/scripts/in_container/run_generate_migration.sh '{message}'"
    execute_command_in_shell(shell_params, project_name="db", command=cmd)
    fix_ownership_using_docker()


@main.command(name="doctor", help="Auto-healing of breeze")
@option_answer
@option_verbose
@option_dry_run
@click.pass_context
def doctor(ctx):
    shell_params = ShellParams()
    check_docker_resources(shell_params.airflow_image_name)
    shell_params.print_badge_info()

    perform_environment_checks()
    fix_ownership_using_docker(quiet=False)

    given_answer = user_confirm("Are you sure with the removal of temporary Python files and Python cache?")
    if not get_dry_run() and given_answer == Answer.YES:
        cleanup_python_generated_files()

    shell_params = ShellParams(backend="all", include_mypy_volume=True)
    bring_compose_project_down(preserve_volumes=False, shell_params=shell_params)

    given_answer = user_confirm("Are you sure with the removal of mypy cache and build cache dir?")
    if given_answer == Answer.YES:
        get_console().print("\n[info]Cleaning mypy cache...\n")
        command_to_execute = ["docker", "volume", "rm", "--force", "mypy-cache-volume"]
        run_command(command_to_execute)

        get_console().print("\n[info]Cleaning build cache...\n")
        command_to_execute = ["docker", "volume", "rm", "--force", "airflow-cache-volume"]
        run_command(command_to_execute)

        get_console().print("\n[info]Deleting .build cache dir...\n")
        dirpath = Path(".build")
        if not get_dry_run() and dirpath.exists() and dirpath.is_dir():
            shutil.rmtree(dirpath)

    given_answer = user_confirm(
        "Proceed with breeze cleanup to remove all docker volumes, images and networks?"
    )
    if given_answer == Answer.YES:
        get_console().print("\n[info]Executing breeze cleanup...\n")
        ctx.forward(cleanup)
    elif given_answer == Answer.QUIT:
        sys.exit(0)


@main.command(
    name="run",
    help="Run a command in the Breeze environment without entering the interactive shell.",
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
)
@click.argument("command", required=True)
@click.argument("command_args", nargs=-1, type=click.UNPROCESSED)
@option_answer
@option_backend
@option_builder
@option_docker_host
@option_dry_run
@option_force_build
@option_forward_credentials
@option_github_repository
@option_mysql_version
@option_platform_single
@option_postgres_version
@option_project_name
@option_python
@option_skip_image_upgrade_check
@option_tty
@option_use_uv
@option_uv_http_timeout
@option_verbose
def run(
    command: str,
    command_args: tuple,
    backend: str,
    builder: str,
    docker_host: str | None,
    force_build: bool,
    forward_credentials: bool,
    github_repository: str,
    mysql_version: str,
    platform: str | None,
    postgres_version: str,
    project_name: str,
    python: str,
    skip_image_upgrade_check: bool,
    tty: str,
    use_uv: bool,
    uv_http_timeout: int,
):
    """
    Run a command in the Breeze environment without entering the interactive shell.

    This is useful for automated testing, CI workflows, and one-off command execution.
    The command will be executed in a fresh container that is automatically cleaned up.
    Each run uses a unique project name to avoid conflicts with other instances.

    Examples:
        # Run a specific test
        breeze run pytest providers/google/tests/unit/google/cloud/operators/test_dataflow.py -v

        # Check version compatibility
        breeze run python -c "from airflow.providers.google.version_compat import AIRFLOW_V_3_0_PLUS; print(AIRFLOW_V_3_0_PLUS)"

        # Run bash commands
        breeze run bash -c "cd /opt/airflow && python -m pytest providers/google/tests/"

        # Run with different Python version
        breeze run --python 3.11 pytest providers/standard/tests/unit/operators/test_bash.py

        # Run with PostgreSQL backend
        breeze run --backend postgres pytest providers/postgres/tests/
    """
    import uuid

    from airflow_breeze.commands.ci_image_commands import rebuild_or_pull_ci_image_if_needed
    from airflow_breeze.params.shell_params import ShellParams
    from airflow_breeze.utils.ci_group import ci_group
    from airflow_breeze.utils.docker_command_utils import execute_command_in_shell
    from airflow_breeze.utils.platforms import get_normalized_platform

    # Generate a unique project name to avoid conflicts with other running instances
    unique_project_name = f"{project_name}-run-{uuid.uuid4().hex[:8]}"

    # Build the full command string with proper escaping
    import shlex

    if command_args:
        # Use shlex.join to properly escape arguments
        full_command = f"{command} {shlex.join(command_args)}"
    else:
        full_command = command

    platform = get_normalized_platform(platform)

    # Create shell parameters optimized for non-interactive command execution
    shell_params = ShellParams(
        backend=backend,
        builder=builder,
        docker_host=docker_host,
        force_build=force_build,
        forward_credentials=forward_credentials,
        github_repository=github_repository,
        mysql_version=mysql_version,
        platform=platform,
        postgres_version=postgres_version,
        project_name=unique_project_name,
        python=python,
        skip_image_upgrade_check=skip_image_upgrade_check,
        use_uv=use_uv,
        uv_http_timeout=uv_http_timeout,
        # Optimizations for non-interactive execution
        quiet=True,
        skip_environment_initialization=True,
        tty=tty,
        # Set extra_args to empty tuple since we'll pass the command directly
        extra_args=(),
    )

    if get_verbose():
        get_console().print(f"[info]Running command in Breeze: {full_command}[/]")
        get_console().print(f"[info]Using project name: {unique_project_name}[/]")

    # Build or pull the CI image if needed
    rebuild_or_pull_ci_image_if_needed(command_params=shell_params)

    # Execute the command in the shell
    with ci_group(f"Running command: {command}"):
        result = execute_command_in_shell(
            shell_params=shell_params,
            project_name=unique_project_name,
            command=full_command,
            # Always preserve the backend specified by user (or resolved from default)
            preserve_backend=True,
        )

    # Clean up ownership
    from airflow_breeze.utils.docker_command_utils import fix_ownership_using_docker

    fix_ownership_using_docker()

    # Exit with the same code as the command
    sys.exit(result.returncode)
