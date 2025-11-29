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

import contextlib
import os
import signal
import sys
from collections.abc import Generator
from datetime import datetime
from functools import cache
from multiprocessing.pool import Pool
from time import sleep

import click
from click import IntRange

from airflow_breeze.commands.ci_image_commands import rebuild_or_pull_ci_image_if_needed
from airflow_breeze.commands.common_options import (
    option_allow_pre_releases,
    option_backend,
    option_clean_airflow_installation,
    option_core_integration,
    option_db_reset,
    option_debug_resources,
    option_downgrade_pendulum,
    option_downgrade_sqlalchemy,
    option_dry_run,
    option_excluded_providers,
    option_force_lowest_dependencies,
    option_forward_credentials,
    option_github_repository,
    option_image_name,
    option_include_success_outputs,
    option_install_airflow_with_constraints,
    option_keep_env_variables,
    option_mount_sources,
    option_mysql_version,
    option_no_db_cleanup,
    option_parallelism,
    option_postgres_version,
    option_providers_integration,
    option_python,
    option_run_db_tests_only,
    option_run_in_parallel,
    option_skip_cleanup,
    option_skip_db_tests,
    option_upgrade_boto,
    option_upgrade_sqlalchemy,
    option_use_airflow_version,
    option_verbose,
)
from airflow_breeze.commands.common_package_installation_options import (
    option_airflow_constraints_reference,
    option_providers_constraints_location,
    option_providers_skip_constraints,
    option_use_distributions_from_dist,
)
from airflow_breeze.commands.release_management_commands import option_distribution_format
from airflow_breeze.global_constants import (
    ALL_TEST_SUITES,
    ALL_TEST_TYPE,
    ALLOWED_TEST_TYPE_CHOICES,
    GroupOfTests,
    all_selective_core_test_types,
    providers_test_type,
)
from airflow_breeze.params.build_prod_params import BuildProdParams
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.custom_param_types import BetterChoice, NotVerifiedBetterChoice
from airflow_breeze.utils.docker_command_utils import (
    fix_ownership_using_docker,
    notify_on_unhealthy_backend_container,
    perform_environment_checks,
    remove_docker_networks,
)
from airflow_breeze.utils.parallel import (
    GenericRegexpProgressMatcher,
    SummarizeAfter,
    check_async_run_results,
    run_with_pool,
)
from airflow_breeze.utils.path_utils import AIRFLOW_CTL_ROOT_PATH, FILES_PATH, cleanup_python_generated_files
from airflow_breeze.utils.run_tests import (
    file_name_from_test_type,
    generate_args_for_pytest,
    run_docker_compose_tests,
)
from airflow_breeze.utils.run_utils import RunCommandResult, run_command
from airflow_breeze.utils.selective_checks import ALL_CI_SELECTIVE_TEST_TYPES

GRACE_CONTAINER_STOP_TIMEOUT = 10  # Timeout in seconds to wait for containers to get killed

LOW_MEMORY_CONDITION = 8 * 1024 * 1024 * 1024  # 8 GB
DEFAULT_TOTAL_TEST_TIMEOUT = 60 * 60  # 60 minutes

logs_already_dumped = False


@click.group(cls=BreezeGroup, name="testing", help="Tools that developers can use to run tests")
def testing_group():
    pass


@testing_group.command(
    name="docker-compose-tests",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_python
@option_image_name
@click.option(
    "--skip-docker-compose-deletion",
    help="Skip deletion of docker-compose instance after the test",
    envvar="SKIP_DOCKER_COMPOSE_DELETION",
    is_flag=True,
)
@option_github_repository
@option_include_success_outputs
@option_verbose
@option_dry_run
@click.argument("extra_pytest_args", nargs=-1, type=click.Path(path_type=str))
def docker_compose_tests(
    python: str,
    image_name: str,
    skip_docker_compose_deletion: bool,
    github_repository: str,
    include_success_outputs: bool,
    extra_pytest_args: tuple,
):
    """Run docker-compose tests."""
    perform_environment_checks()
    if image_name is None:
        build_params = BuildProdParams(python=python, github_repository=github_repository)
        image_name = build_params.airflow_image_name
    get_console().print(f"[info]Running docker-compose with PROD image: {image_name}[/]")
    return_code, info = run_docker_compose_tests(
        image_name=image_name,
        include_success_outputs=include_success_outputs,
        extra_pytest_args=extra_pytest_args,
        skip_docker_compose_deletion=skip_docker_compose_deletion,
    )
    sys.exit(return_code)


TEST_PROGRESS_REGEXP = (
    r"airflow-core/tests/.*|providers/.*/tests/.*|task-sdk/tests/.*|airflow-ctl/tests/.*|.*=====.*"
)
PERCENT_TEST_PROGRESS_REGEXP = r"^tests/.*\[[ \d%]*\].*|^\..*\[[ \d%]*\].*"


def _run_test(
    shell_params: ShellParams,
    extra_pytest_args: tuple,
    python_version: str,
    output: Output | None,
    test_timeout: int,
    output_outside_the_group: bool = False,
    skip_docker_compose_down: bool = False,
) -> tuple[int, str]:
    if "[" in shell_params.test_type and not shell_params.test_type.startswith("Providers"):
        get_console(output=output).print(
            "[error]Only 'Providers' test type can specify actual tests with \\[\\][/]"
        )
        sys.exit(1)
    compose_project_name, project_name = _get_project_names(shell_params)
    env = shell_params.env_variables_for_docker_commands
    down_cmd = [
        "docker",
        "compose",
        "--project-name",
        compose_project_name,
        "down",
        "--remove-orphans",
        "--volumes",
    ]
    run_command(down_cmd, output=output, check=False, env=env)
    run_cmd = [
        "docker",
        "compose",
        "--project-name",
        compose_project_name,
        "run",
        "-T",
        "--service-ports",
        "--rm",
        "airflow",
    ]
    pytest_args = generate_args_for_pytest(
        test_group=shell_params.test_group,
        test_type=shell_params.test_type,
        test_timeout=test_timeout,
        skip_db_tests=shell_params.skip_db_tests,
        run_db_tests_only=shell_params.run_db_tests_only,
        backend=shell_params.backend,
        use_xdist=shell_params.use_xdist,
        enable_coverage=shell_params.enable_coverage,
        collect_only=shell_params.collect_only,
        parallelism=shell_params.parallelism,
        python_version=python_version,
        parallel_test_types_list=shell_params.parallel_test_types_list,
        keep_env_variables=shell_params.keep_env_variables,
        no_db_cleanup=shell_params.no_db_cleanup,
        integration=shell_params.integration,
    )
    pytest_args.extend(extra_pytest_args)
    # Skip "FOLDER" in case "--ignore=FOLDER" is passed as an argument
    # Which might be the case if we are ignoring some providers during compatibility checks
    pytest_args_before_skip = pytest_args
    pytest_args = [arg for arg in pytest_args if f"--ignore={arg}" not in pytest_args]
    # Double check: If no test is leftover we can skip running the test
    if pytest_args_before_skip != pytest_args and pytest_args[0].startswith("--"):
        return 0, f"Skipped test, no tests needed: {shell_params.test_type}"
    run_cmd.extend(pytest_args)
    try:
        remove_docker_networks(networks=[f"{compose_project_name}_default"])
        result = run_command(
            run_cmd,
            output=output,
            check=False,
            output_outside_the_group=output_outside_the_group,
            env=env,
        )
        if result.returncode != 0:
            notify_on_unhealthy_backend_container(
                project_name=project_name, backend=shell_params.backend, output=output
            )
        if os.environ.get("CI") == "true" and result.returncode != 0:
            get_console(output=output).print(f"[error]Test failed with {result.returncode}.[/]")
            _dump_container_logs(output=output, shell_params=shell_params)
    finally:
        if not skip_docker_compose_down:
            run_command(
                [
                    "docker",
                    "compose",
                    "--project-name",
                    compose_project_name,
                    "rm",
                    "--stop",
                    "--force",
                    "-v",
                ],
                output=output,
                check=False,
                env=env,
                verbose_override=False,
                quiet=True,
            )
            remove_docker_networks(networks=[f"{compose_project_name}_default"])
    return result.returncode, f"Test: {shell_params.test_type}"


def _get_project_names(shell_params: ShellParams) -> tuple[str, str]:
    """Return compose project name and project name."""
    project_name = file_name_from_test_type(shell_params.test_type)
    if shell_params.test_type == ALL_TEST_TYPE:
        compose_project_name = "airflow-test"
    else:
        compose_project_name = f"airflow-test-{project_name}"
    return compose_project_name, project_name


@cache  # Note: using functools.cache to avoid multiple dumps in the same run
def _dump_container_logs(output: Output | None, shell_params: ShellParams):
    get_console().print("[warning]Dumping container logs[/]")
    ps_result = run_command(
        ["docker", "ps", "--all", "--format", "{{.Names}}"],
        check=True,
        capture_output=True,
        text=True,
    )
    container_ids = ps_result.stdout.splitlines()
    get_console(output=output).print("[warning]Wait 10 seconds for logs to find their way to stderr.\n")
    sleep(10)
    compose_project_name, project_name = _get_project_names(shell_params)
    get_console(output=output).print(
        f"[warning]Dumping container logs: {container_ids} for compose project {compose_project_name} (cp.\n"
    )
    date_str = datetime.now().strftime("%Y_%d_%m_%H_%M_%S")
    for container_id in container_ids:
        if compose_project_name not in container_id:
            continue
        dump_path = FILES_PATH / f"container_logs_{container_id}_{date_str}.log"
        get_console(output=output).print(f"[info]Dumping container log {container_id} to {dump_path}\n")
        with open(dump_path, "w") as outfile:
            run_command(
                ["docker", "logs", "--details", "--timestamps", container_id],
                check=False,
                stdout=outfile,
            )


def _run_tests_in_pool(
    debug_resources: bool,
    extra_pytest_args: tuple,
    include_success_outputs: bool,
    parallelism: int,
    shell_params: ShellParams,
    skip_cleanup: bool,
    skip_docker_compose_down: bool,
    test_timeout: int,
    tests_to_run: list[str],
    handler: TimeoutHandler,
):
    if not tests_to_run:
        return
    # this should be hard-coded as we want to have very specific sequence of tests
    # Heaviest tests go first and lightest tests go last. This way we can maximise parallelism as the
    # lightest tests will continue to complete and new light tests will get added while the heavy
    # tests are still running. We are only adding here test types that take more than 2 minutes to run
    # on a fast machine in parallel
    sorting_order = [
        "Providers[standard]",
        "Providers[amazon]",
        "Providers[google]",
        "API",
        "Other",
        "WWW",
        "Core",
        "CLI",
        "Serialization",
        "Always",
        "Providers[celery]",
    ]
    sort_key = {item: i for i, item in enumerate(sorting_order)}
    # Put the test types in the order we want them to run
    tests_to_run = sorted(tests_to_run, key=lambda x: (sort_key.get(x, len(sorting_order)), x))
    escaped_tests = [test.replace("[", "\\[") for test in tests_to_run]
    with ci_group(f"Testing {' '.join(escaped_tests)}"):
        all_params = [f"{test_type}" for test_type in tests_to_run]
        with run_with_pool(
            parallelism=parallelism,
            all_params=all_params,
            debug_resources=debug_resources,
            progress_matcher=GenericRegexpProgressMatcher(
                regexp=TEST_PROGRESS_REGEXP,
                regexp_for_joined_line=PERCENT_TEST_PROGRESS_REGEXP,
                lines_to_search=400,
            ),
        ) as (pool, outputs):
            handler.set_pool(pool)
            results = [
                pool.apply_async(
                    _run_test,
                    kwds={
                        "shell_params": shell_params.clone_with_test(test_type=test_type),
                        "extra_pytest_args": extra_pytest_args,
                        "python_version": shell_params.python,
                        "output": outputs[index],
                        "test_timeout": test_timeout,
                        "skip_docker_compose_down": skip_docker_compose_down,
                    },
                )
                for index, test_type in enumerate(tests_to_run)
            ]
    escaped_tests = [test.replace("[", "\\[") for test in tests_to_run]
    check_async_run_results(
        results=results,
        success_message=f"Tests {' '.join(escaped_tests)} completed successfully",
        outputs=outputs,
        include_success_outputs=include_success_outputs,
        skip_cleanup=skip_cleanup,
        summarize_on_ci=SummarizeAfter.FAILURE,
        summary_start_regexp=r".*= FAILURES.*|.*= ERRORS.*",
        terminated_on_timeout=handler.terminated_on_timeout_output_list[0],
    )


def pull_images_for_docker_compose(shell_params: ShellParams):
    get_console().print("Pulling images once before parallel run\n")
    env = shell_params.env_variables_for_docker_commands
    pull_cmd = [
        "docker",
        "compose",
        "pull",
    ]
    run_command(pull_cmd, output=None, check=False, env=env)


def run_tests_in_parallel(
    shell_params: ShellParams,
    extra_pytest_args: tuple,
    test_timeout: int,
    include_success_outputs: bool,
    debug_resources: bool,
    parallelism: int,
    skip_cleanup: bool,
    skip_docker_compose_down: bool,
    handler: TimeoutHandler,
) -> None:
    get_console().print("\n[info]Summary of the tests to run\n")
    get_console().print(f"[info]Running tests in parallel with parallelism={parallelism}")
    get_console().print(f"[info]Extra pytest args: {extra_pytest_args}")
    get_console().print(f"[info]Test timeout: {test_timeout}")
    get_console().print(f"[info]Include success outputs: {include_success_outputs}")
    get_console().print(f"[info]Debug resources: {debug_resources}")
    get_console().print(f"[info]Skip cleanup: {skip_cleanup}")
    get_console().print(f"[info]Skip docker-compose down: {skip_docker_compose_down}")
    get_console().print("[info]Shell params:")
    get_console().print(shell_params.__dict__)
    pull_images_for_docker_compose(shell_params)
    _run_tests_in_pool(
        tests_to_run=shell_params.parallel_test_types_list,
        parallelism=parallelism,
        shell_params=shell_params,
        extra_pytest_args=extra_pytest_args,
        test_timeout=test_timeout,
        include_success_outputs=include_success_outputs,
        debug_resources=debug_resources,
        skip_cleanup=skip_cleanup,
        skip_docker_compose_down=skip_docker_compose_down,
        handler=handler,
    )


def _verify_parallelism_parameters(
    excluded_parallel_test_types: str, run_db_tests_only: bool, run_in_parallel: bool, use_xdist: bool
):
    if excluded_parallel_test_types and not (run_in_parallel or use_xdist):
        get_console().print(
            "\n[error]You can only specify --excluded-parallel-test-types when --run-in-parallel or "
            "--use-xdist are set[/]\n"
        )
        sys.exit(1)
    if use_xdist and run_in_parallel:
        get_console().print("\n[error]You can only specify one of --use-xdist, --run-in-parallel[/]\n")
        sys.exit(1)
    if use_xdist and run_db_tests_only:
        get_console().print("\n[error]You can only specify one of --use-xdist, --run-db-tests-only[/]\n")
        sys.exit(1)


option_collect_only = click.option(
    "--collect-only",
    help="Collect tests only, do not run them.",
    is_flag=True,
    envvar="COLLECT_ONLY",
)
option_enable_coverage = click.option(
    "--enable-coverage",
    help="Enable coverage capturing for tests in the form of XML files",
    is_flag=True,
    envvar="ENABLE_COVERAGE",
)
option_excluded_parallel_core_test_types = click.option(
    "--excluded-parallel-test-types",
    help="Space separated list of core test types that will be excluded from parallel tes runs.",
    default="",
    show_default=True,
    envvar="EXCLUDED_PARALLEL_TEST_TYPES",
    type=NotVerifiedBetterChoice(all_selective_core_test_types()),
)
option_parallel_core_test_types = click.option(
    "--parallel-test-types",
    help="Space separated list of core test types used for testing in parallel.",
    default=ALL_CI_SELECTIVE_TEST_TYPES,
    show_default=True,
    envvar="PARALLEL_TEST_TYPES",
    type=NotVerifiedBetterChoice(all_selective_core_test_types()),
)
option_excluded_parallel_providers_test_types = click.option(
    "--excluded-parallel-test-types",
    help="Space separated list of provider test types that will be excluded from parallel tes runs. You can "
    "for example `Providers[airbyte,http]`.",
    default="",
    envvar="EXCLUDED_PARALLEL_TEST_TYPES",
    type=str,
)
option_parallel_providers_test_types = click.option(
    "--parallel-test-types",
    help="Space separated list of provider test types used for testing in parallel. You can also optionally "
    "specify tests of which providers should be run: `Providers[airbyte,http]`.",
    default=providers_test_type()[0],
    envvar="PARALLEL_TEST_TYPES",
    type=str,
)
option_skip_docker_compose_down = click.option(
    "--skip-docker-compose-down",
    help="Skips running docker-compose down after tests",
    is_flag=True,
    envvar="SKIP_DOCKER_COMPOSE_DOWN",
)
option_skip_providers = click.option(
    "--skip-providers",
    help="Space-separated list of provider ids to skip when running tests",
    type=str,
    default="",
    envvar="SKIP_PROVIDERS",
)
option_test_timeout = click.option(
    "--test-timeout",
    help="Test timeout in seconds. Set the pytest setup, execution and teardown timeouts to this value",
    default=60,
    envvar="TEST_TIMEOUT",
    type=IntRange(min=0),
    show_default=True,
)
option_test_type_core_group = click.option(
    "--test-type",
    help="Type of tests to run for core test group",
    default=ALL_TEST_TYPE,
    envvar="TEST_TYPE",
    show_default=True,
    type=BetterChoice(ALLOWED_TEST_TYPE_CHOICES[GroupOfTests.CORE]),
)
option_test_type_providers_group = click.option(
    "--test-type",
    help="Type of test to run. You can also optionally specify tests of which providers "
    "should be run: `Providers[airbyte,http]` or "
    "excluded from the full test suite: `Providers[-amazon,google]`",
    default=ALL_TEST_TYPE,
    envvar="TEST_TYPE",
    show_default=True,
    type=NotVerifiedBetterChoice(ALLOWED_TEST_TYPE_CHOICES[GroupOfTests.PROVIDERS]),
)
option_test_type = click.option(
    "--test-type",
    help="Type for shell tests to run - used when forcing "
    "lowest dependencies to determine which distribution to force lowest dependencies for",
    default=ALL_TEST_TYPE,
    envvar="TEST_TYPE",
    show_default=True,
    type=NotVerifiedBetterChoice([*ALL_TEST_SUITES.keys(), *all_selective_core_test_types()]),
)
option_test_type_helm = click.option(
    "--test-type",
    help="Type of helm tests to run",
    default=ALL_TEST_TYPE,
    envvar="TEST_TYPE",
    show_default=True,
    type=BetterChoice(ALLOWED_TEST_TYPE_CHOICES[GroupOfTests.HELM]),
)
option_test_type_task_sdk_group = click.option(
    "--test-type",
    help="Type of test to run for task SDK",
    default=ALL_TEST_TYPE,
    envvar="TEST_TYPE",
    show_default=True,
    type=BetterChoice(ALLOWED_TEST_TYPE_CHOICES[GroupOfTests.TASK_SDK]),
)
option_test_type_airflow_ctl_group = click.option(
    "--test-type",
    help="Type of CTL tests to run",
    default=ALL_TEST_TYPE,
    envvar="TEST_TYPE",
    show_default=True,
    type=BetterChoice(ALLOWED_TEST_TYPE_CHOICES[GroupOfTests.CTL]),
)
option_use_xdist = click.option(
    "--use-xdist",
    help="Use xdist plugin for pytest",
    is_flag=True,
    envvar="USE_XDIST",
)
option_force_sa_warnings = click.option(
    "--force-sa-warnings/--no-force-sa-warnings",
    help="Enable `sqlalchemy.exc.MovedIn20Warning` during the tests runs.",
    is_flag=True,
    default=True,
    show_default=True,
    envvar="SQLALCHEMY_WARN_20",
)
option_total_test_timeout = click.option(
    "--total-test-timeout",
    help="Total test timeout in seconds. This is the maximum time parallel tests will run. If there is "
    "an underlying pytest command that hangs, the process will be stop with system exit after "
    "that time. This should give a chance to upload logs as artifacts on CI.",
    default=DEFAULT_TOTAL_TEST_TIMEOUT,
    type=int,
    envvar="TOTAL_TEST_TIMEOUT",
)
option_skip_docker_compose_deletion = click.option(
    "--skip-docker-compose-deletion",
    help="Skip deletion of docker-compose instance after the test",
    envvar="SKIP_DOCKER_COMPOSE_DELETION",
    is_flag=True,
)


@testing_group.command(
    name="core-tests",
    help="Run all (default) or specified core unit tests.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_airflow_constraints_reference
@option_backend
@option_collect_only
@option_clean_airflow_installation
@option_db_reset
@option_debug_resources
@option_downgrade_pendulum
@option_downgrade_sqlalchemy
@option_dry_run
@option_enable_coverage
@option_excluded_parallel_core_test_types
@option_force_sa_warnings
@option_force_lowest_dependencies
@option_forward_credentials
@option_github_repository
@option_include_success_outputs
@option_install_airflow_with_constraints
@option_keep_env_variables
@option_mount_sources
@option_mysql_version
@option_no_db_cleanup
@option_distribution_format
@option_parallel_core_test_types
@option_parallelism
@option_postgres_version
@option_python
@option_run_db_tests_only
@option_run_in_parallel
@option_skip_cleanup
@option_skip_db_tests
@option_skip_docker_compose_down
@option_test_timeout
@option_test_type_core_group
@option_total_test_timeout
@option_upgrade_boto
@option_upgrade_sqlalchemy
@option_use_airflow_version
@option_allow_pre_releases
@option_use_distributions_from_dist
@option_use_xdist
@option_verbose
@click.argument("extra_pytest_args", nargs=-1, type=click.Path(path_type=str))
def core_tests(**kwargs):
    _run_test_command(
        test_group=GroupOfTests.CORE,
        integration=(),
        excluded_providers="",
        providers_skip_constraints=False,
        providers_constraints_location="",
        skip_providers="",
        **kwargs,
    )


@testing_group.command(
    name="providers-tests",
    help="Run all (default) or specified Providers unit tests.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_airflow_constraints_reference
@option_backend
@option_collect_only
@option_clean_airflow_installation
@option_db_reset
@option_debug_resources
@option_downgrade_pendulum
@option_downgrade_sqlalchemy
@option_dry_run
@option_enable_coverage
@option_excluded_providers
@option_excluded_parallel_providers_test_types
@option_force_sa_warnings
@option_force_lowest_dependencies
@option_forward_credentials
@option_github_repository
@option_include_success_outputs
@option_install_airflow_with_constraints
@option_keep_env_variables
@option_mount_sources
@option_mysql_version
@option_no_db_cleanup
@option_distribution_format
@option_parallel_providers_test_types
@option_parallelism
@option_postgres_version
@option_providers_constraints_location
@option_providers_skip_constraints
@option_python
@option_run_db_tests_only
@option_run_in_parallel
@option_skip_cleanup
@option_skip_db_tests
@option_skip_docker_compose_down
@option_skip_providers
@option_test_timeout
@option_test_type_providers_group
@option_total_test_timeout
@option_upgrade_boto
@option_upgrade_sqlalchemy
@option_use_airflow_version
@option_allow_pre_releases
@option_use_distributions_from_dist
@option_use_xdist
@option_verbose
@click.argument("extra_pytest_args", nargs=-1, type=click.Path(path_type=str))
def providers_tests(**kwargs):
    _run_test_command(test_group=GroupOfTests.PROVIDERS, integration=(), **kwargs)


@testing_group.command(
    name="task-sdk-tests",
    help="Run task-sdk tests - all task SDK tests are non-DB bound tests.",
    context_settings=dict(
        ignore_unknown_options=False,
        allow_extra_args=False,
    ),
)
@option_collect_only
@option_dry_run
@option_enable_coverage
@option_force_sa_warnings
@option_forward_credentials
@option_github_repository
@option_keep_env_variables
@option_mount_sources
@option_python
@option_skip_docker_compose_down
@option_test_timeout
@option_verbose
@click.argument("extra_pytest_args", nargs=-1, type=click.Path(path_type=str))
def task_sdk_tests(**kwargs):
    """Run task SDK tests."""
    _run_test_command(
        test_group=GroupOfTests.TASK_SDK,
        allow_pre_releases=False,
        airflow_constraints_reference="constraints-main",
        backend="none",
        clean_airflow_installation=False,
        debug_resources=False,
        downgrade_pendulum=False,
        downgrade_sqlalchemy=False,
        db_reset=False,
        include_success_outputs=False,
        integration=(),
        install_airflow_with_constraints=False,
        run_db_tests_only=False,
        run_in_parallel=False,
        skip_db_tests=True,
        use_xdist=True,
        excluded_parallel_test_types="",
        excluded_providers="",
        force_lowest_dependencies=False,
        no_db_cleanup=True,
        parallel_test_types="",
        parallelism=0,
        distribution_format="wheel",
        providers_constraints_location="",
        providers_skip_constraints=False,
        skip_cleanup=False,
        skip_providers="",
        test_type=ALL_TEST_TYPE,
        total_test_timeout=DEFAULT_TOTAL_TEST_TIMEOUT,
        upgrade_boto=False,
        upgrade_sqlalchemy=False,
        use_airflow_version=None,
        use_distributions_from_dist=False,
        **kwargs,
    )


@testing_group.command(
    name="task-sdk-integration-tests",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_python
@option_image_name
@option_skip_docker_compose_deletion
@option_github_repository
@option_include_success_outputs
@option_verbose
@option_dry_run
@click.option(
    "--task-sdk-version",
    help="Version of Task SDK to test",
    default="1.1.0",
    show_default=True,
    envvar="TASK_SDK_VERSION",
)
@click.argument("extra_pytest_args", nargs=-1, type=click.Path(path_type=str))
def task_sdk_integration_tests(
    python: str,
    image_name: str,
    skip_docker_compose_deletion: bool,
    github_repository: str,
    include_success_outputs: bool,
    task_sdk_version: str,
    extra_pytest_args: tuple,
):
    """Run task SDK integration tests."""
    perform_environment_checks()
    if image_name is None:
        build_params = BuildProdParams(python=python, github_repository=github_repository)
        image_name = build_params.airflow_image_name

    # Export the TASK_SDK_VERSION environment variable for the test
    import os

    os.environ["TASK_SDK_VERSION"] = task_sdk_version

    get_console().print(f"[info]Running task SDK integration tests with PROD image: {image_name}[/]")
    get_console().print(f"[info]Using Task SDK version: {task_sdk_version}[/]")
    return_code, info = run_docker_compose_tests(
        image_name=image_name,
        include_success_outputs=include_success_outputs,
        extra_pytest_args=extra_pytest_args,
        skip_docker_compose_deletion=skip_docker_compose_deletion,
        test_type="task-sdk-integration",
    )
    sys.exit(return_code)


@testing_group.command(
    name="airflow-ctl-integration-tests",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_python
@option_image_name
@option_skip_docker_compose_deletion
@option_github_repository
@option_include_success_outputs
@option_verbose
@option_dry_run
@click.option(
    "--airflow-ctl-version",
    help="Version of airflowctl to test",
    default="1.0.0",
    show_default=True,
    envvar="AIRFLOW_CTL_VERSION",
)
@click.argument("extra_pytest_args", nargs=-1, type=click.Path(path_type=str))
def airflowctl_integration_tests(
    python: str,
    image_name: str | None,
    skip_docker_compose_deletion: bool,
    github_repository: str,
    include_success_outputs: bool,
    airflow_ctl_version: str,
    extra_pytest_args: tuple,
):
    """Run airflowctl integration tests."""
    # Export the AIRFLOW_CTL_VERSION environment variable for the test
    import os

    perform_environment_checks()

    os.environ["AIRFLOW_CTL_VERSION"] = airflow_ctl_version
    image_name = image_name or os.environ.get("DOCKER_IMAGE")

    if image_name is None:
        build_params = BuildProdParams(python=python, github_repository=github_repository)
        image_name = build_params.airflow_image_name

    get_console().print(f"[info]Running airflowctl integration tests with PROD image: {image_name}[/]")
    get_console().print(f"[info]Using airflowctl version: {airflow_ctl_version}[/]")
    return_code, info = run_docker_compose_tests(
        image_name=image_name,
        include_success_outputs=include_success_outputs,
        extra_pytest_args=extra_pytest_args,
        skip_docker_compose_deletion=skip_docker_compose_deletion,
        test_type="airflow-ctl-integration",
    )
    sys.exit(return_code)


@testing_group.command(
    name="airflow-ctl-tests",
    help="Run airflow-ctl tests - all airflowctl tests are non-DB bound tests.",
    context_settings=dict(
        ignore_unknown_options=False,
        allow_extra_args=False,
    ),
)
@option_parallelism
@option_python
@option_dry_run
@option_verbose
@click.argument("extra_pytest_args", nargs=-1, type=click.Path(path_type=str))
def airflow_ctl_tests(python: str, parallelism: int, extra_pytest_args: tuple):
    parallelism_args = ["-n", str(parallelism)] if parallelism > 1 else []
    test_command = [
        "uv",
        "run",
        "--python",
        python,
        "pytest",
        "--color=yes",
        *parallelism_args,
        *extra_pytest_args,
    ]
    result = run_command(test_command, cwd=AIRFLOW_CTL_ROOT_PATH, check=False)
    if result.returncode != 0:
        get_console().print(
            f"[error]airflowctl tests failed with return code {result.returncode}.[/]\n"
            f"Command: {' '.join(test_command)}\n"
        )
        sys.exit(result.returncode)


@testing_group.command(
    name="core-integration-tests",
    help="Run the specified integration tests.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_backend
@option_collect_only
@option_db_reset
@option_dry_run
@option_enable_coverage
@option_force_sa_warnings
@option_forward_credentials
@option_github_repository
@option_core_integration
@option_keep_env_variables
@option_mount_sources
@option_mysql_version
@option_no_db_cleanup
@option_postgres_version
@option_python
@option_skip_docker_compose_down
@option_test_timeout
@option_verbose
@click.argument("extra_pytest_args", nargs=-1, type=click.Path(path_type=str))
def core_integration_tests(
    backend: str,
    collect_only: bool,
    db_reset: bool,
    enable_coverage: bool,
    extra_pytest_args: tuple,
    force_sa_warnings: bool,
    forward_credentials: bool,
    github_repository: str,
    keep_env_variables: bool,
    integration: tuple,
    mount_sources: str,
    mysql_version: str,
    no_db_cleanup: bool,
    postgres_version: str,
    python: str,
    skip_docker_compose_down: bool,
    test_timeout: int,
):
    shell_params = ShellParams(
        test_group=GroupOfTests.INTEGRATION_CORE,
        backend=backend,
        collect_only=collect_only,
        enable_coverage=enable_coverage,
        forward_credentials=forward_credentials,
        forward_ports=False,
        github_repository=github_repository,
        integration=integration,
        keep_env_variables=keep_env_variables,
        mount_sources=mount_sources,
        mysql_version=mysql_version,
        no_db_cleanup=no_db_cleanup,
        postgres_version=postgres_version,
        python=python,
        test_type="All",
        force_sa_warnings=force_sa_warnings,
        run_tests=True,
        db_reset=db_reset,
    )
    fix_ownership_using_docker()
    cleanup_python_generated_files()
    perform_environment_checks()
    returncode, _ = _run_test(
        shell_params=shell_params,
        extra_pytest_args=extra_pytest_args,
        python_version=python,
        output=None,
        test_timeout=test_timeout,
        output_outside_the_group=True,
        skip_docker_compose_down=skip_docker_compose_down,
    )
    sys.exit(returncode)


@testing_group.command(
    name="providers-integration-tests",
    help="Run the specified integration tests.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_backend
@option_collect_only
@option_db_reset
@option_dry_run
@option_enable_coverage
@option_force_sa_warnings
@option_forward_credentials
@option_github_repository
@option_providers_integration
@option_keep_env_variables
@option_mount_sources
@option_mysql_version
@option_no_db_cleanup
@option_postgres_version
@option_python
@option_skip_docker_compose_down
@option_test_timeout
@option_verbose
@click.argument("extra_pytest_args", nargs=-1, type=click.Path(path_type=str))
def integration_providers_tests(
    backend: str,
    collect_only: bool,
    db_reset: bool,
    enable_coverage: bool,
    extra_pytest_args: tuple,
    force_sa_warnings: bool,
    forward_credentials: bool,
    github_repository: str,
    integration: tuple,
    keep_env_variables: bool,
    mount_sources: str,
    mysql_version: str,
    no_db_cleanup: bool,
    postgres_version: str,
    python: str,
    skip_docker_compose_down: bool,
    test_timeout: int,
):
    shell_params = ShellParams(
        test_group=GroupOfTests.INTEGRATION_PROVIDERS,
        backend=backend,
        collect_only=collect_only,
        enable_coverage=enable_coverage,
        forward_credentials=forward_credentials,
        forward_ports=False,
        github_repository=github_repository,
        integration=integration,
        keep_env_variables=keep_env_variables,
        mount_sources=mount_sources,
        mysql_version=mysql_version,
        no_db_cleanup=no_db_cleanup,
        postgres_version=postgres_version,
        python=python,
        test_type="All",
        force_sa_warnings=force_sa_warnings,
        run_tests=True,
        db_reset=db_reset,
    )
    fix_ownership_using_docker()
    cleanup_python_generated_files()
    perform_environment_checks()
    returncode, _ = _run_test(
        shell_params=shell_params,
        extra_pytest_args=extra_pytest_args,
        python_version=python,
        output=None,
        test_timeout=test_timeout,
        output_outside_the_group=True,
        skip_docker_compose_down=skip_docker_compose_down,
    )
    sys.exit(returncode)


@testing_group.command(
    name="system-tests",
    help="Run the specified system tests.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_backend
@option_collect_only
@option_db_reset
@option_dry_run
@option_enable_coverage
@option_force_sa_warnings
@option_forward_credentials
@option_github_repository
@option_keep_env_variables
@option_mount_sources
@option_mysql_version
@option_no_db_cleanup
@option_use_airflow_version
@option_allow_pre_releases
@option_airflow_constraints_reference
@option_clean_airflow_installation
@option_force_lowest_dependencies
@option_install_airflow_with_constraints
@option_distribution_format
@option_use_distributions_from_dist
@option_postgres_version
@option_python
@option_skip_docker_compose_down
@option_test_timeout
@option_verbose
@click.argument("extra_pytest_args", nargs=-1, type=click.Path(path_type=str))
def system_tests(
    backend: str,
    collect_only: bool,
    db_reset: bool,
    enable_coverage: bool,
    extra_pytest_args: tuple,
    force_sa_warnings: bool,
    forward_credentials: bool,
    github_repository: str,
    keep_env_variables: bool,
    mount_sources: str,
    mysql_version: str,
    no_db_cleanup: bool,
    postgres_version: str,
    python: str,
    skip_docker_compose_down: bool,
    test_timeout: int,
    use_airflow_version: str,
    allow_pre_releases: bool,
    airflow_constraints_reference: str,
    clean_airflow_installation: bool,
    force_lowest_dependencies: bool,
    install_airflow_with_constraints: bool,
    distribution_format: str,
    use_distributions_from_dist: bool,
):
    shell_params = ShellParams(
        test_group=GroupOfTests.SYSTEM,
        backend=backend,
        collect_only=collect_only,
        enable_coverage=enable_coverage,
        forward_credentials=forward_credentials,
        forward_ports=True,
        github_repository=github_repository,
        integration=(),
        keep_env_variables=keep_env_variables,
        mount_sources=mount_sources,
        mysql_version=mysql_version,
        no_db_cleanup=no_db_cleanup,
        postgres_version=postgres_version,
        python=python,
        test_type="None",
        force_sa_warnings=force_sa_warnings,
        run_tests=True,
        db_reset=db_reset,
        use_airflow_version=use_airflow_version,
        allow_pre_releases=allow_pre_releases,
        airflow_constraints_reference=airflow_constraints_reference,
        clean_airflow_installation=clean_airflow_installation,
        force_lowest_dependencies=force_lowest_dependencies,
        install_airflow_with_constraints=install_airflow_with_constraints,
        distribution_format=distribution_format,
        use_distributions_from_dist=use_distributions_from_dist,
    )
    fix_ownership_using_docker()
    cleanup_python_generated_files()
    perform_environment_checks()
    returncode, _ = _run_test(
        shell_params=shell_params,
        extra_pytest_args=extra_pytest_args,
        python_version=python,
        output=None,
        test_timeout=test_timeout,
        output_outside_the_group=True,
        skip_docker_compose_down=skip_docker_compose_down,
    )
    sys.exit(returncode)


@testing_group.command(
    name="helm-tests",
    help="Run Helm chart tests.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_mount_sources
@option_github_repository
@option_test_timeout
@option_parallelism
@option_test_type_helm
@option_use_xdist
@option_verbose
@option_dry_run
@click.argument("extra_pytest_args", nargs=-1, type=click.Path(path_type=str))
def helm_tests(
    extra_pytest_args: tuple,
    mount_sources: str,
    github_repository: str,
    test_timeout: int,
    test_type: str,
    parallelism: int,
    use_xdist: bool,
):
    shell_params = ShellParams(
        mount_sources=mount_sources,
        github_repository=github_repository,
        run_tests=True,
        test_type=test_type,
    )
    env = shell_params.env_variables_for_docker_commands
    perform_environment_checks()
    fix_ownership_using_docker()
    cleanup_python_generated_files()
    pytest_args = generate_args_for_pytest(
        test_group=GroupOfTests.HELM,
        test_type=test_type,
        test_timeout=test_timeout,
        skip_db_tests=False,
        run_db_tests_only=False,
        backend="none",
        use_xdist=use_xdist,
        enable_coverage=False,
        collect_only=False,
        parallelism=parallelism,
        parallel_test_types_list=[],
        python_version=shell_params.python,
        keep_env_variables=False,
        no_db_cleanup=False,
    )
    cmd = ["docker", "compose", "run", "--service-ports", "--rm", "airflow", *pytest_args, *extra_pytest_args]
    result = run_command(cmd, check=False, env=env, output_outside_the_group=True)
    fix_ownership_using_docker()
    sys.exit(result.returncode)


@testing_group.command(
    name="python-api-client-tests",
    help="Run python api client tests.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_backend
@option_collect_only
@option_db_reset
@option_no_db_cleanup
@option_enable_coverage
@option_force_sa_warnings
@option_forward_credentials
@option_github_repository
@option_keep_env_variables
@option_mysql_version
@option_postgres_version
@option_python
@option_skip_docker_compose_down
@option_test_timeout
@option_dry_run
@option_verbose
@click.argument("extra_pytest_args", nargs=-1, type=click.Path(path_type=str))
def python_api_client_tests(
    backend: str,
    collect_only: bool,
    db_reset: bool,
    no_db_cleanup: bool,
    enable_coverage: bool,
    force_sa_warnings: bool,
    forward_credentials: bool,
    github_repository: str,
    keep_env_variables: bool,
    mysql_version: str,
    postgres_version: str,
    python: str,
    skip_docker_compose_down: bool,
    test_timeout: int,
    extra_pytest_args: tuple,
):
    shell_params = ShellParams(
        test_group=GroupOfTests.PYTHON_API_CLIENT,
        backend=backend,
        collect_only=collect_only,
        enable_coverage=enable_coverage,
        forward_credentials=forward_credentials,
        forward_ports=False,
        github_repository=github_repository,
        integration=(),
        keep_env_variables=keep_env_variables,
        mysql_version=mysql_version,
        postgres_version=postgres_version,
        python=python,
        test_type="python-api-client",
        force_sa_warnings=force_sa_warnings,
        run_tests=True,
        db_reset=db_reset,
        no_db_cleanup=no_db_cleanup,
        install_airflow_python_client=True,
        start_api_server_with_examples=True,
    )
    rebuild_or_pull_ci_image_if_needed(command_params=shell_params)
    fix_ownership_using_docker()
    cleanup_python_generated_files()
    perform_environment_checks()
    returncode, _ = _run_test(
        shell_params=shell_params,
        extra_pytest_args=extra_pytest_args,
        python_version=python,
        output=None,
        test_timeout=test_timeout,
        output_outside_the_group=True,
        skip_docker_compose_down=skip_docker_compose_down,
    )
    sys.exit(returncode)


class TimeoutHandler:
    def __init__(self, shell_params: ShellParams, terminated_on_timeout_output_list: list[bool]):
        # Initialize the timeout handler with shell parameters and a list to track terminated outputs
        # The terminated_on_timeout_output_list list is used to signal to the outside world that the
        # output has been terminate by setting the first element to True when the timeout is reached.
        self.shell_params = shell_params
        self.pool: Pool | None = None
        self.terminated_on_timeout_output_list = terminated_on_timeout_output_list
        self.terminated_on_timeout_output_list[0] = False

    def set_pool(self, pool: Pool):
        self.pool = pool

    @staticmethod
    def _send_signal_to_containers(list_of_containers: list[str], signal_number: str):
        run_command(
            ["docker", "kill", "--signal", signal_number, *list_of_containers],
            check=True,
            capture_output=False,
            text=True,
        )

    @staticmethod
    def _get_running_containers() -> RunCommandResult:
        return run_command(
            ["docker", "ps", "-q"],
            check=True,
            capture_output=True,
            text=True,
        )

    @staticmethod
    def _print_all_containers():
        run_command(
            ["docker", "ps"],
            check=True,
        )

    def timeout_method(self, signum, frame):
        get_console().print("[warning]Timeout reached.[/]")
        if self.pool:
            get_console().print("[warning]Terminating the pool[/]")
            self.pool.close()
            self.pool.terminate()
            # No join here. The pool is joined already in the main function
        get_console().print("[warning]Stopping all running containers[/]:")
        self._print_all_containers()
        if os.environ.get("CI") == "true":
            _dump_container_logs(output=None, shell_params=self.shell_params)
        list_of_containers = self._get_running_containers().stdout.splitlines()
        get_console().print("[warning]Attempting to send TERM signal to all remaining containers:")
        get_console().print(list_of_containers)
        self._send_signal_to_containers(list_of_containers, "SIGTERM")
        get_console().print(f"[warning]Waiting {GRACE_CONTAINER_STOP_TIMEOUT} seconds for containers to stop")
        sleep(GRACE_CONTAINER_STOP_TIMEOUT)
        containers_left = self._get_running_containers().stdout.splitlines()
        if containers_left:
            get_console().print("[warning]Some containers are still running. Killing them with SIGKILL:")
            get_console().print(containers_left)
            self._send_signal_to_containers(list_of_containers, "SIGKILL")
            get_console().print(
                f"[warning]Waiting {GRACE_CONTAINER_STOP_TIMEOUT} seconds for containers to stop"
            )
            sleep(GRACE_CONTAINER_STOP_TIMEOUT)
            containers_left = self._get_running_containers().stdout.splitlines()
            if containers_left:
                get_console().print(
                    "[error]Some containers are still running. Marking stuff as terminated anyway."
                )
                get_console().print(containers_left)
        get_console().print(
            "[warning]All containers stopped. Marking the whole run as terminated on timeout[/]"
        )
        self.terminated_on_timeout_output_list[0] = True


@contextlib.contextmanager
def run_with_timeout(timeout: int, shell_params: ShellParams) -> Generator[TimeoutHandler, None, None]:
    timeout_handler = TimeoutHandler(shell_params=shell_params, terminated_on_timeout_output_list=[False])
    signal.signal(signal.SIGALRM, lambda signum, frame: timeout_handler.timeout_method(signum, frame))
    signal.alarm(timeout)
    try:
        yield timeout_handler
    finally:
        signal.alarm(0)


def _run_test_command(
    *,
    test_group: GroupOfTests,
    airflow_constraints_reference: str,
    allow_pre_releases: bool,
    backend: str,
    collect_only: bool,
    clean_airflow_installation: bool,
    db_reset: bool,
    debug_resources: bool,
    downgrade_sqlalchemy: bool,
    downgrade_pendulum: bool,
    enable_coverage: bool,
    excluded_parallel_test_types: str,
    excluded_providers: str,
    extra_pytest_args: tuple,
    force_sa_warnings: bool,
    forward_credentials: bool,
    force_lowest_dependencies: bool,
    github_repository: str,
    include_success_outputs: bool,
    install_airflow_with_constraints: bool,
    integration: tuple[str, ...],
    keep_env_variables: bool,
    mount_sources: str,
    no_db_cleanup: bool,
    parallel_test_types: str,
    parallelism: int,
    distribution_format: str,
    providers_constraints_location: str,
    providers_skip_constraints: bool,
    python: str,
    run_db_tests_only: bool,
    run_in_parallel: bool,
    skip_cleanup: bool,
    skip_db_tests: bool,
    skip_docker_compose_down: bool,
    skip_providers: str,
    test_timeout: int,
    test_type: str,
    total_test_timeout: int,
    upgrade_boto: bool,
    upgrade_sqlalchemy: bool,
    use_airflow_version: str | None,
    use_distributions_from_dist: bool,
    use_xdist: bool,
    mysql_version: str = "",
    postgres_version: str = "",
):
    _verify_parallelism_parameters(
        excluded_parallel_test_types, run_db_tests_only, run_in_parallel, use_xdist
    )
    test_list = parallel_test_types.split(" ")
    excluded_test_list = excluded_parallel_test_types.split(" ")
    if excluded_test_list:
        test_list = [test for test in test_list if test not in excluded_test_list]
    shell_params = ShellParams(
        airflow_constraints_reference=airflow_constraints_reference,
        allow_pre_releases=allow_pre_releases,
        backend=backend,
        collect_only=collect_only,
        clean_airflow_installation=clean_airflow_installation,
        downgrade_sqlalchemy=downgrade_sqlalchemy,
        downgrade_pendulum=downgrade_pendulum,
        enable_coverage=enable_coverage,
        excluded_providers=excluded_providers,
        force_sa_warnings=force_sa_warnings,
        force_lowest_dependencies=force_lowest_dependencies,
        forward_credentials=forward_credentials,
        forward_ports=False,
        github_repository=github_repository,
        integration=integration,
        install_airflow_with_constraints=install_airflow_with_constraints,
        keep_env_variables=keep_env_variables,
        mount_sources=mount_sources,
        mysql_version=mysql_version,
        no_db_cleanup=no_db_cleanup,
        distribution_format=distribution_format,
        parallel_test_types_list=test_list,
        parallelism=parallelism,
        postgres_version=postgres_version,
        providers_constraints_location=providers_constraints_location,
        providers_skip_constraints=providers_skip_constraints,
        python=python,
        run_db_tests_only=run_db_tests_only,
        skip_db_tests=skip_db_tests,
        test_type=test_type,
        test_group=test_group,
        upgrade_boto=upgrade_boto,
        upgrade_sqlalchemy=upgrade_sqlalchemy,
        use_airflow_version=use_airflow_version,
        use_distributions_from_dist=use_distributions_from_dist,
        use_xdist=use_xdist,
        run_tests=True,
        db_reset=db_reset if not skip_db_tests else False,
    )
    rebuild_or_pull_ci_image_if_needed(command_params=shell_params)
    fix_ownership_using_docker()
    cleanup_python_generated_files()
    perform_environment_checks()
    if skip_providers:
        ignored_path_list = [
            *[
                f"--ignore=providers/{provider_id.replace('.', '/')}/tests"
                for provider_id in skip_providers.split(" ")
            ],
        ]
        extra_pytest_args = (*extra_pytest_args, *ignored_path_list)
    if run_in_parallel:
        if test_type != ALL_TEST_TYPE:
            get_console().print(
                "[error]You should not specify --test-type when --run-in-parallel is set[/]. "
                f"Your test type = {test_type}\n"
            )
            sys.exit(1)
        with run_with_timeout(total_test_timeout, shell_params=shell_params) as handler:
            run_tests_in_parallel(
                shell_params=shell_params,
                extra_pytest_args=extra_pytest_args,
                test_timeout=test_timeout,
                include_success_outputs=include_success_outputs,
                parallelism=parallelism,
                skip_cleanup=skip_cleanup,
                debug_resources=debug_resources,
                skip_docker_compose_down=skip_docker_compose_down,
                handler=handler,
            )
    else:
        if shell_params.test_type == ALL_TEST_TYPE:
            if any(["tests/" in arg and not arg.startswith("-") for arg in extra_pytest_args]):
                shell_params.test_type = "None"
                shell_params.parallel_test_types_list = []
        returncode, _ = _run_test(
            shell_params=shell_params,
            extra_pytest_args=extra_pytest_args,
            python_version=python,
            output=None,
            test_timeout=test_timeout,
            output_outside_the_group=True,
            skip_docker_compose_down=skip_docker_compose_down,
        )
        sys.exit(returncode)
