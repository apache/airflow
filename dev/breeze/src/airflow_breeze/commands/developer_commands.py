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
import re
import shlex
import shutil
import sys
import threading
from collections.abc import Iterable
from signal import SIGTERM
from time import sleep

import click

from airflow_breeze.branch_defaults import DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.commands.ci_image_commands import rebuild_or_pull_ci_image_if_needed
from airflow_breeze.commands.common_options import (
    argument_doc_packages,
    option_airflow_extras,
    option_answer,
    option_backend,
    option_builder,
    option_clean_airflow_installation,
    option_database_isolation,
    option_db_reset,
    option_docker_host,
    option_downgrade_pendulum,
    option_downgrade_sqlalchemy,
    option_dry_run,
    option_excluded_providers,
    option_forward_credentials,
    option_github_repository,
    option_image_tag_for_running,
    option_include_not_ready_providers,
    option_include_removed_providers,
    option_installation_package_format,
    option_integration,
    option_keep_env_variables,
    option_max_time,
    option_mount_sources,
    option_mysql_version,
    option_no_db_cleanup,
    option_postgres_version,
    option_project_name,
    option_python,
    option_run_db_tests_only,
    option_skip_db_tests,
    option_standalone_dag_processor,
    option_upgrade_boto,
    option_use_airflow_version,
    option_use_uv,
    option_uv_http_timeout,
    option_verbose,
)
from airflow_breeze.commands.common_package_installation_options import (
    option_airflow_constraints_location,
    option_airflow_constraints_mode_ci,
    option_airflow_constraints_reference,
    option_airflow_skip_constraints,
    option_install_selected_providers,
    option_providers_constraints_location,
    option_providers_constraints_mode_ci,
    option_providers_constraints_reference,
    option_providers_skip_constraints,
    option_use_packages_from_dist,
)
from airflow_breeze.commands.main_command import main
from airflow_breeze.commands.testing_commands import (
    option_force_lowest_dependencies,
    option_test_type,
)
from airflow_breeze.global_constants import (
    ALLOWED_CELERY_BROKERS,
    ALLOWED_CELERY_EXECUTORS,
    ALLOWED_EXECUTORS,
    ALLOWED_TTY,
    CELERY_INTEGRATION,
    DEFAULT_ALLOWED_EXECUTOR,
    DEFAULT_CELERY_BROKER,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    DOCKER_DEFAULT_PLATFORM,
    SINGLE_PLATFORMS,
    START_AIRFLOW_ALLOWED_EXECUTORS,
    START_AIRFLOW_DEFAULT_ALLOWED_EXECUTOR,
)
from airflow_breeze.params.build_ci_params import BuildCiParams
from airflow_breeze.params.doc_build_params import DocBuildParams
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.pre_commit_ids import PRE_COMMIT_LIST
from airflow_breeze.utils.coertions import one_or_none_set
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.custom_param_types import BetterChoice
from airflow_breeze.utils.docker_command_utils import (
    bring_compose_project_down,
    check_docker_resources,
    enter_shell,
    execute_command_in_shell,
    fix_ownership_using_docker,
    perform_environment_checks,
)
from airflow_breeze.utils.packages import expand_all_provider_packages
from airflow_breeze.utils.path_utils import (
    AIRFLOW_SOURCES_ROOT,
    cleanup_python_generated_files,
)
from airflow_breeze.utils.recording import generating_command_images
from airflow_breeze.utils.run_utils import (
    assert_pre_commit_installed,
    run_command,
    run_compile_ui_assets,
    run_compile_www_assets,
)
from airflow_breeze.utils.shared_options import (
    get_dry_run,
    get_verbose,
    set_forced_answer,
)


def _determine_constraint_branch_used(
    airflow_constraints_reference: str, use_airflow_version: str | None
):
    """
    Determine which constraints reference to use.

    When use-airflow-version is branch or version, we derive the constraints branch from it, unless
    someone specified the constraints branch explicitly.

    :param airflow_constraints_reference: the constraint reference specified (or default)
    :param use_airflow_version: which airflow version we are installing
    :return: the actual constraints reference to use
    """
    if (
        use_airflow_version
        and airflow_constraints_reference == DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
        and re.match(
            r"[0-9]+\.[0-9]+\.[0-9]+[0-9a-z.]*|main|v[0-9]_.*", use_airflow_version
        )
    ):
        get_console().print(
            f"[info]Using constraints for {use_airflow_version} - matching airflow version used."
        )
        return f"constraints-{use_airflow_version}"
    return airflow_constraints_reference


class TimerThread(threading.Thread):
    def __init__(self, max_time: int):
        super().__init__(daemon=True)
        self.max_time = max_time

    def run(self):
        get_console().print(f"[info]Setting timer to fail after {self.max_time} s.")
        sleep(self.max_time)
        get_console().print(
            f"[error]The command took longer than {self.max_time} s. Failing!"
        )
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
option_celery_flower = click.option(
    "--celery-flower", help="Start celery flower", is_flag=True
)
option_executor_shell = click.option(
    "--executor",
    type=click.Choice(ALLOWED_EXECUTORS, case_sensitive=False),
    help="Specify the executor to use with shell command.",
    default=DEFAULT_ALLOWED_EXECUTOR,
    show_default=True,
)
option_force_build = click.option(
    "--force-build",
    help="Force image build no matter if it is determined as needed.",
    is_flag=True,
)
option_include_mypy_volume = click.option(
    "--include-mypy-volume",
    help="Whether to include mounting of the mypy volume (useful for debugging mypy).",
    is_flag=True,
    envvar="INCLUDE_MYPY_VOLUME",
)
option_platform_single = click.option(
    "--platform",
    help="Platform for Airflow image.",
    default=DOCKER_DEFAULT_PLATFORM
    if not generating_command_images()
    else SINGLE_PLATFORMS[0],
    envvar="PLATFORM",
    type=BetterChoice(SINGLE_PLATFORMS),
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

option_install_airflow_with_constraints_default_true = click.option(
    "--install-airflow-with-constraints/--no-install-airflow-with-constraints",
    is_flag=True,
    default=True,
    show_default=True,
    envvar="INSTALL_AIRFLOW_WITH_CONSTRAINTS",
    help="Install airflow in a separate step, with constraints determined from package or airflow version.",
)


@main.command()
@click.argument("extra-args", nargs=-1, type=click.UNPROCESSED)
@click.option(
    "--quiet",
    is_flag=True,
    envvar="QUIET",
    help="Suppress initialization output when starting.",
)
@click.option(
    "--tty",
    envvar="TTY",
    type=BetterChoice(ALLOWED_TTY),
    default=ALLOWED_TTY[0],
    show_default=True,
    help="Whether to allocate pseudo-tty when running docker command"
    " (useful for pre-commit and CI to force-enable it).",
)
@click.option(
    "--verbose-commands",
    help="Show details of commands executed.",
    is_flag=True,
    envvar="VERBOSE_COMMANDS",
)
@option_airflow_constraints_location
@option_airflow_constraints_mode_ci
@option_airflow_constraints_reference
@option_airflow_extras
@option_airflow_skip_constraints
@option_answer
@option_backend
@option_builder
@option_celery_broker
@option_celery_flower
@option_clean_airflow_installation
@option_database_isolation
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
@option_image_tag_for_running
@option_include_mypy_volume
@option_install_airflow_with_constraints_default_true
@option_install_selected_providers
@option_installation_package_format
@option_integration
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
@option_test_type
@option_skip_db_tests
@option_skip_environment_initialization
@option_skip_image_upgrade_check
@option_warn_image_upgrade_needed
@option_standalone_dag_processor
@option_upgrade_boto
@option_use_airflow_version
@option_use_packages_from_dist
@option_use_uv
@option_uv_http_timeout
@option_verbose
def shell(
    airflow_constraints_location: str,
    airflow_constraints_mode: str,
    airflow_constraints_reference: str,
    airflow_extras: str,
    airflow_skip_constraints: bool,
    backend: str,
    builder: str,
    celery_broker: str,
    celery_flower: bool,
    clean_airflow_installation: bool,
    database_isolation: bool,
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
    image_tag: str | None,
    include_mypy_volume: bool,
    install_selected_providers: str,
    install_airflow_with_constraints: bool,
    integration: tuple[str, ...],
    keep_env_variables: bool,
    max_time: int | None,
    mount_sources: str,
    mysql_version: str,
    no_db_cleanup: bool,
    package_format: str,
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
    test_type: str,
    tty: str,
    upgrade_boto: bool,
    use_airflow_version: str | None,
    use_packages_from_dist: bool,
    use_uv: bool,
    uv_http_timeout: int,
    verbose_commands: bool,
    warn_image_upgrade_needed: bool,
):
    """Enter breeze environment. this is the default command use when no other is selected."""
    if get_verbose() or get_dry_run() and not quiet:
        get_console().print("\n[success]Welcome to breeze.py[/]\n")
        get_console().print(
            f"\n[success]Root of Airflow Sources = {AIRFLOW_SOURCES_ROOT}[/]\n"
        )
    if max_time:
        TimerThread(max_time=max_time).start()
        set_forced_answer("yes")
    airflow_constraints_reference = _determine_constraint_branch_used(
        airflow_constraints_reference, use_airflow_version
    )
    shell_params = ShellParams(
        airflow_constraints_location=airflow_constraints_location,
        airflow_constraints_mode=airflow_constraints_mode,
        airflow_constraints_reference=airflow_constraints_reference,
        airflow_extras=airflow_extras,
        airflow_skip_constraints=airflow_skip_constraints,
        backend=backend,
        builder=builder,
        celery_broker=celery_broker,
        celery_flower=celery_flower,
        clean_airflow_installation=clean_airflow_installation,
        database_isolation=database_isolation,
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
        image_tag=image_tag,
        include_mypy_volume=include_mypy_volume,
        install_airflow_with_constraints=install_airflow_with_constraints,
        install_selected_providers=install_selected_providers,
        integration=integration,
        keep_env_variables=keep_env_variables,
        mount_sources=mount_sources,
        mysql_version=mysql_version,
        no_db_cleanup=no_db_cleanup,
        package_format=package_format,
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
        test_type=test_type,
        tty=tty,
        upgrade_boto=upgrade_boto,
        use_airflow_version=use_airflow_version,
        use_packages_from_dist=use_packages_from_dist,
        use_uv=use_uv,
        uv_http_timeout=uv_http_timeout,
        verbose_commands=verbose_commands,
        warn_image_upgrade_needed=warn_image_upgrade_needed,
    )
    rebuild_or_pull_ci_image_if_needed(command_params=shell_params)
    result = enter_shell(shell_params=shell_params)
    fix_ownership_using_docker()
    sys.exit(result.returncode)


option_load_example_dags = click.option(
    "-e",
    "--load-example-dags",
    help="Enable configuration to load example DAGs when starting Airflow.",
    is_flag=True,
    envvar="LOAD_EXAMPLES",
)

option_load_default_connection = click.option(
    "-c",
    "--load-default-connections",
    help="Enable configuration to load default connections when starting Airflow.",
    is_flag=True,
    envvar="LOAD_DEFAULT_CONNECTIONS",
)

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
    help="Starts webserver in dev mode (assets are always recompiled in this case when starting) "
    "(mutually exclusive with --skip-assets-compilation).",
    is_flag=True,
)
@click.argument("extra-args", nargs=-1, type=click.UNPROCESSED)
@option_airflow_constraints_location
@option_airflow_constraints_mode_ci
@option_airflow_constraints_reference
@option_airflow_extras
@option_airflow_skip_constraints
@option_answer
@option_backend
@option_builder
@option_clean_airflow_installation
@option_celery_broker
@option_celery_flower
@option_database_isolation
@option_db_reset
@option_docker_host
@option_dry_run
@option_executor_start_airflow
@option_force_build
@option_forward_credentials
@option_github_repository
@option_image_tag_for_running
@option_installation_package_format
@option_install_selected_providers
@option_integration
@option_load_default_connection
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
@option_use_uv
@option_uv_http_timeout
@option_use_airflow_version
@option_use_packages_from_dist
@option_verbose
def start_airflow(
    airflow_constraints_mode: str,
    airflow_constraints_location: str,
    airflow_constraints_reference: str,
    airflow_extras: str,
    airflow_skip_constraints: bool,
    backend: str,
    builder: str,
    celery_broker: str,
    celery_flower: bool,
    clean_airflow_installation: bool,
    database_isolation: bool,
    db_reset: bool,
    dev_mode: bool,
    docker_host: str | None,
    executor: str | None,
    extra_args: tuple,
    force_build: bool,
    forward_credentials: bool,
    github_repository: str,
    image_tag: str | None,
    integration: tuple[str, ...],
    install_selected_providers: str,
    load_default_connections: bool,
    load_example_dags: bool,
    mount_sources: str,
    mysql_version: str,
    package_format: str,
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
    use_airflow_version: str | None,
    use_packages_from_dist: bool,
    use_uv: bool,
    uv_http_timeout: int,
):
    """
    Enter breeze environment and starts all Airflow components in the tmux session.
    Compile assets if contents of www directory changed.
    """
    if dev_mode and skip_assets_compilation:
        get_console().print(
            "[warning]You cannot skip asset compilation in dev mode! Assets will be compiled!"
        )
        skip_assets_compilation = True
    if use_airflow_version is None and not skip_assets_compilation:
        # Now with the /ui project, lets only do a static build of /www and focus on the /ui
        run_compile_www_assets(dev=False, run_in_background=False, force_clean=False)
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

    shell_params = ShellParams(
        airflow_constraints_location=airflow_constraints_location,
        airflow_constraints_mode=airflow_constraints_mode,
        airflow_constraints_reference=airflow_constraints_reference,
        airflow_extras=airflow_extras,
        airflow_skip_constraints=airflow_skip_constraints,
        backend=backend,
        builder=builder,
        celery_broker=celery_broker,
        celery_flower=celery_flower,
        clean_airflow_installation=clean_airflow_installation,
        database_isolation=database_isolation,
        db_reset=db_reset,
        dev_mode=dev_mode,
        docker_host=docker_host,
        executor=executor,
        extra_args=extra_args,
        force_build=force_build,
        forward_credentials=forward_credentials,
        github_repository=github_repository,
        image_tag=image_tag,
        integration=integration,
        install_selected_providers=install_selected_providers,
        install_airflow_with_constraints=True,
        load_default_connections=load_default_connections,
        load_example_dags=load_example_dags,
        mount_sources=mount_sources,
        mysql_version=mysql_version,
        package_format=package_format,
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
        use_packages_from_dist=use_packages_from_dist,
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
    help="Clean inventories of Inter-Sphinx documentation and generated APIs and sphinx artifacts "
    "before the build - useful for a clean build.",
    is_flag=True,
)
@click.option("-d", "--docs-only", help="Only build documentation.", is_flag=True)
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
    "--package-list",
    envvar="PACKAGE_LIST",
    type=str,
    help="Optional, contains comma-separated list of package ids that are processed for documentation "
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
    docs_only: bool,
    github_repository: str,
    include_not_ready_providers: bool,
    include_removed_providers: bool,
    one_pass_only: bool,
    package_filter: tuple[str, ...],
    package_list: str,
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
        docs_dir = AIRFLOW_SOURCES_ROOT / "docs"
        for dir_name in ["_build", "_doctrees", "_inventory_cache", "_api"]:
            for directory in docs_dir.rglob(dir_name):
                get_console().print(f"[info]Removing {directory}")
                shutil.rmtree(directory, ignore_errors=True)
    docs_list_as_tuple: tuple[str, ...] = ()
    if package_list and len(package_list):
        get_console().print(
            f"\n[info]Populating provider list from PACKAGE_LIST env as {package_list}"
        )
        # Override doc_packages with values from PACKAGE_LIST
        docs_list_as_tuple = tuple(package_list.split(","))
    if doc_packages and docs_list_as_tuple:
        get_console().print(
            f"[warning]Both package arguments and --package-list / PACKAGE_LIST passed. "
            f"Overriding to {docs_list_as_tuple}"
        )
    doc_packages = docs_list_as_tuple or doc_packages
    doc_builder = DocBuildParams(
        package_filter=package_filter,
        docs_only=docs_only,
        spellcheck_only=spellcheck_only,
        one_pass_only=one_pass_only,
        short_doc_packages=expand_all_provider_packages(
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
    )
    result = execute_command_in_shell(shell_params, project_name="docs", command=cmd)
    fix_ownership_using_docker()
    if result.returncode == 0:
        get_console().print(
            "[info]To view the built documentation, you have two options:\n\n"
            "1. Start the webserver in breeze and access the built docs at "
            "http://localhost:28080/docs/\n"
            "2. Alternatively, you can run ./docs/start_doc_server.sh for a lighter resource option and view "
            "the built docs at http://localhost:8000"
        )
    sys.exit(result.returncode)


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
    "type_",
    help="Type(s) of the static checks to run.",
    type=BetterChoice(PRE_COMMIT_LIST),
)
@click.option("-a", "--all-files", help="Run checks on all files.", is_flag=True)
@click.option(
    "-f",
    "--file",
    help="List of files to run the checks on.",
    type=click.Path(),
    multiple=True,
)
@click.option(
    "-s",
    "--show-diff-on-failure",
    help="Show diff for files modified by the checks.",
    is_flag=True,
)
@click.option(
    "-c",
    "--last-commit",
    help="Run checks for all files in last commit. Mutually exclusive with --commit-ref.",
    is_flag=True,
)
@click.option(
    "-m",
    "--only-my-changes",
    help="Run checks for commits belonging to my PR only: for all commits between merge base to `main` "
    "branch and HEAD of your branch.",
    is_flag=True,
)
@click.option(
    "-r",
    "--commit-ref",
    help="Run checks for this commit reference only "
    "(can be any git commit-ish reference). "
    "Mutually exclusive with --last-commit.",
)
@click.option(
    "--initialize-environment",
    help="Initialize environment before running checks.",
    is_flag=True,
)
@click.option(
    "--max-initialization-attempts",
    help="Maximum number of attempts to initialize environment before giving up.",
    show_default=True,
    type=click.IntRange(1, 10),
    default=3,
)
@option_builder
@option_dry_run
@option_force_build
@option_github_repository
@option_image_tag_for_running
@option_skip_image_upgrade_check
@option_verbose
@click.argument("precommit_args", nargs=-1, type=click.UNPROCESSED)
def static_checks(
    all_files: bool,
    builder: str,
    commit_ref: str,
    file: Iterable[str],
    force_build: bool,
    github_repository: str,
    image_tag: str,
    initialize_environment: bool,
    last_commit: bool,
    max_initialization_attempts: int,
    only_my_changes: bool,
    precommit_args: tuple,
    show_diff_on_failure: bool,
    skip_image_upgrade_check: bool,
    type_: str,
):
    assert_pre_commit_installed()
    perform_environment_checks()
    build_params = BuildCiParams(
        builder=builder,
        force_build=force_build,
        image_tag=image_tag,
        github_repository=github_repository,
        # for static checks we do not want to regenerate dependencies before pre-commits are run
        # we want the pre-commit to do it for us (and detect the case the dependencies are updated)
        skip_provider_dependencies_check=True,
    )
    if not skip_image_upgrade_check:
        rebuild_or_pull_ci_image_if_needed(command_params=build_params)

    if initialize_environment:
        get_console().print(
            "[info]Make sure that pre-commit is installed and environment initialized[/]"
        )
        get_console().print(
            f"[info]Trying to install the environments up to {max_initialization_attempts} "
            f"times in case of flakiness[/]"
        )
        return_code = 0
        for attempt in range(1, 1 + max_initialization_attempts):
            get_console().print(
                f"[info]Attempt number {attempt} to install pre-commit environments"
            )
            initialization_result = run_command(
                [sys.executable, "-m", "pre_commit", "install", "--install-hooks"],
                check=False,
                no_output_dump_on_exception=True,
                text=True,
            )
            if initialization_result.returncode == 0:
                break
            get_console().print(f"[warning]Attempt number {attempt} failed - retrying[/]")
            return_code = initialization_result.returncode
        else:
            get_console().print("[error]Could not install pre-commit environments[/]")
            sys.exit(return_code)

    command_to_execute = [sys.executable, "-m", "pre_commit", "run"]
    if not one_or_none_set([last_commit, commit_ref, only_my_changes, all_files]):
        get_console().print(
            "\n[error]You can only specify "
            "one of --last-commit, --commit-ref, --only-my-changes, --all-files[/]\n"
        )
        sys.exit(1)
    if type_:
        command_to_execute.append(type_)
    if only_my_changes:
        merge_base = run_command(
            ["git", "merge-base", "HEAD", "main"],
            capture_output=True,
            check=False,
            text=True,
        ).stdout.strip()
        if not merge_base:
            get_console().print(
                "\n[warning]Could not find merge base between HEAD and main. Running check for all files\n"
            )
            all_files = True
        else:
            get_console().print(
                f"\n[info]Running checks for files changed in the current branch: {merge_base}..HEAD\n"
            )
            command_to_execute.extend(["--from-ref", merge_base, "--to-ref", "HEAD"])
    if all_files:
        command_to_execute.append("--all-files")
    if show_diff_on_failure:
        command_to_execute.append("--show-diff-on-failure")
    if last_commit:
        get_console().print(
            "\n[info]Running checks for last commit in the current branch: HEAD^..HEAD\n"
        )
        command_to_execute.extend(["--from-ref", "HEAD^", "--to-ref", "HEAD"])
    if commit_ref:
        get_console().print(f"\n[info]Running checks for selected commit: {commit_ref}\n")
        command_to_execute.extend(
            ["--from-ref", f"{commit_ref}^", "--to-ref", f"{commit_ref}"]
        )
    if get_verbose() or get_dry_run():
        command_to_execute.append("--verbose")
    if file:
        command_to_execute.append("--files")
        command_to_execute.extend(file)
    if precommit_args:
        command_to_execute.extend(precommit_args)
    skip_checks = os.environ.get("SKIP")
    if skip_checks and skip_checks != "identity":
        get_console().print("\nThis static check run skips those checks:\n")
        get_console().print(skip_checks.split(","))
        get_console().print()
    env = os.environ.copy()
    env["GITHUB_REPOSITORY"] = github_repository
    env["VERBOSE"] = str(get_verbose()).lower()
    static_checks_result = run_command(
        command_to_execute,
        check=False,
        no_output_dump_on_exception=True,
        text=True,
        env=env,
    )
    if not os.environ.get("SKIP_BREEZE_PRE_COMMITS"):
        fix_ownership_using_docker()
    if static_checks_result.returncode != 0:
        if os.environ.get("CI"):
            get_console().print(
                "\n[error]This error means that you have to fix the issues listed above:[/]"
            )
            get_console().print(
                "\n[info]Some of the problems might be fixed automatically via pre-commit[/]"
            )
            get_console().print(
                "\n[info]You can run it locally with: `pre-commit run --all-files` "
                "but it might take quite some time.[/]"
            )
            get_console().print(
                "\n[info]If you use breeze you can also run it faster via: "
                "`breeze static-checks --only-my-changes` but it might produce slightly "
                "different results.[/]"
            )
            get_console().print(
                "\n[info]To run `pre-commit` as part of git workflow, use "
                "`pre-commit install`. This will make pre-commit run as you commit changes[/]\n"
            )
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
@click.option(
    "--force-clean",
    help="Force cleanup of compile assets before building them.",
    is_flag=True,
)
@option_verbose
@option_dry_run
def compile_www_assets(dev: bool, force_clean: bool):
    perform_environment_checks()
    assert_pre_commit_installed()
    compile_www_assets_result = run_compile_www_assets(
        dev=dev, run_in_background=False, force_clean=force_clean
    )
    if compile_www_assets_result.returncode != 0:
        get_console().print("[warn]New assets were generated[/]")
    sys.exit(0)


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
    assert_pre_commit_installed()
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
@option_project_name
@option_verbose
@option_dry_run
def down(preserve_volumes: bool, cleanup_mypy_cache: bool, project_name: str):
    perform_environment_checks()
    shell_params = ShellParams(
        backend="all", include_mypy_volume=cleanup_mypy_cache, project_name=project_name
    )
    bring_compose_project_down(
        preserve_volumes=preserve_volumes, shell_params=shell_params
    )
    if cleanup_mypy_cache:
        command_to_execute = ["docker", "volume", "rm", "--force", "mypy-cache-volume"]
        run_command(command_to_execute)


@main.command(
    name="exec", help="Joins the interactive shell of running airflow container."
)
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


def stop_exec_on_error(returncode: int):
    get_console().print(
        "\n[error]ERROR in finding the airflow docker-compose process id[/]\n"
    )
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
        cmd,
        text=True,
        capture_output=True,
        check=False,
        env=shell_params.env_variables_for_docker_commands,
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
    else:
        stop_exec_on_error(1)
        return None


@main.command(
    name="generate-migration-file",
    help="Autogenerate the alembic migration file for the ORM changes.",
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
        github_repository=github_repository,
        python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
        builder=builder,
    )
    rebuild_or_pull_ci_image_if_needed(command_params=build_params)
    shell_params = ShellParams(
        github_repository=github_repository,
        python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    )
    cmd = f"/opt/airflow/scripts/in_container/run_generate_migration.sh '{message}'"
    execute_command_in_shell(shell_params, project_name="db", command=cmd)
    fix_ownership_using_docker()
