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
from datetime import datetime

import click
from click import IntRange

from airflow_breeze.global_constants import ALLOWED_TEST_TYPE_CHOICES, all_selective_test_types
from airflow_breeze.params.build_prod_params import BuildProdParams
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.common_options import (
    option_backend,
    option_db_reset,
    option_debug_resources,
    option_dry_run,
    option_github_repository,
    option_image_name,
    option_image_tag_for_running,
    option_include_success_outputs,
    option_integration,
    option_mount_sources,
    option_mssql_version,
    option_mysql_version,
    option_parallelism,
    option_postgres_version,
    option_python,
    option_run_in_parallel,
    option_skip_cleanup,
    option_verbose,
)
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.custom_param_types import NotVerifiedBetterChoice
from airflow_breeze.utils.docker_command_utils import (
    DOCKER_COMPOSE_COMMAND,
    get_env_variables_for_docker_commands,
    perform_environment_checks,
)
from airflow_breeze.utils.parallel import (
    GenericRegexpProgressMatcher,
    SummarizeAfter,
    bytes2human,
    check_async_run_results,
    run_with_pool,
)
from airflow_breeze.utils.path_utils import FILES_DIR, cleanup_python_generated_files
from airflow_breeze.utils.run_tests import run_docker_compose_tests
from airflow_breeze.utils.run_utils import get_filesystem_type, run_command

LOW_MEMORY_CONDITION = 8 * 1024 * 1024 * 1024


@click.group(cls=BreezeGroup, name="testing", help="Tools that developers can use to run tests")
def testing():
    pass


@testing.command(
    name="docker-compose-tests",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_python
@option_image_tag_for_running
@option_image_name
@option_github_repository
@option_verbose
@option_dry_run
@click.argument("extra_pytest_args", nargs=-1, type=click.UNPROCESSED)
def docker_compose_tests(
    python: str,
    image_name: str,
    image_tag: str | None,
    github_repository: str,
    extra_pytest_args: tuple,
):
    """Run docker-compose tests."""
    if image_name is None:
        build_params = BuildProdParams(
            python=python, image_tag=image_tag, github_repository=github_repository
        )
        image_name = build_params.airflow_image_name_with_tag
    get_console().print(f"[info]Running docker-compose with PROD image: {image_name}[/]")
    return_code, info = run_docker_compose_tests(
        image_name=image_name,
        extra_pytest_args=extra_pytest_args,
    )
    sys.exit(return_code)


TEST_PROGRESS_REGEXP = r"tests/.*|.*=====.*"
PERCENT_TEST_PROGRESS_REGEXP = r"^tests/.*\[[ \d%]*\].*"


def _run_test(
    exec_shell_params: ShellParams,
    extra_pytest_args: tuple,
    db_reset: bool,
    output: Output | None,
    test_timeout: int,
    output_outside_the_group: bool = False,
) -> tuple[int, str]:
    env_variables = get_env_variables_for_docker_commands(exec_shell_params)
    env_variables["RUN_TESTS"] = "true"
    if test_timeout:
        env_variables["TEST_TIMEOUT"] = str(test_timeout)
    if db_reset:
        env_variables["DB_RESET"] = "true"
    perform_environment_checks()
    env_variables["TEST_TYPE"] = exec_shell_params.test_type
    if "[" in exec_shell_params.test_type and not exec_shell_params.test_type.startswith("Providers"):
        get_console(output=output).print(
            "[error]Only 'Providers' test type can specify actual tests with \\[\\][/]"
        )
        sys.exit(1)
    project_name = _file_name_from_test_type(exec_shell_params.test_type)
    down_cmd = [
        *DOCKER_COMPOSE_COMMAND,
        "--project-name",
        f"airflow-test-{project_name}",
        "down",
        "--remove-orphans",
    ]
    run_command(down_cmd, env=env_variables, output=output, check=False)
    run_cmd = [
        *DOCKER_COMPOSE_COMMAND,
        "--project-name",
        f"airflow-test-{project_name}",
        "run",
        "-T",
        "--service-ports",
        "--rm",
        "airflow",
    ]
    run_cmd.extend(list(extra_pytest_args))
    try:
        result = run_command(
            run_cmd,
            env=env_variables,
            output=output,
            check=False,
            output_outside_the_group=output_outside_the_group,
        )
        if os.environ.get("CI") == "true" and result.returncode != 0:
            ps_result = run_command(
                ["docker", "ps", "--all", "--format", "{{.Names}}"],
                check=True,
                capture_output=True,
                text=True,
            )
            container_ids = ps_result.stdout.splitlines()
            get_console(output=output).print(
                f"[info]Error {ps_result.returncode}. Dumping containers: {container_ids}."
            )
            date_str = datetime.now().strftime("%Y_%d_%m_%H_%M_%S")
            for container_id in container_ids:
                dump_path = FILES_DIR / f"container_logs_{container_id}_{date_str}.log"
                get_console(output=output).print(f"[info]Dumping container {container_id} to {dump_path}")
                with open(dump_path, "w") as outfile:
                    run_command(["docker", "logs", container_id], check=False, stdout=outfile)
    finally:
        run_command(
            [
                *DOCKER_COMPOSE_COMMAND,
                "--project-name",
                f"airflow-test-{project_name}",
                "rm",
                "--stop",
                "--force",
                "-v",
            ],
            env=env_variables,
            output=output,
            check=False,
            verbose_override=False,
        )
    return result.returncode, f"Test: {exec_shell_params.test_type}"


def _file_name_from_test_type(test_type):
    return test_type.lower().replace("[", "_").replace("]", "").replace(",", "_")[:30]


def _run_tests_in_pool(
    tests_to_run: list[str],
    parallelism: int,
    exec_shell_params: ShellParams,
    extra_pytest_args: tuple,
    test_timeout: int,
    db_reset: bool,
    include_success_outputs: bool,
    debug_resources: bool,
    skip_cleanup: bool,
):
    with ci_group(f"Testing {' '.join(tests_to_run)}"):
        all_params = [f"Test {test_type}" for test_type in tests_to_run]
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
            results = [
                pool.apply_async(
                    _run_test,
                    kwds={
                        "exec_shell_params": exec_shell_params.clone_with_test(test_type=test_type),
                        "extra_pytest_args": extra_pytest_args,
                        "db_reset": db_reset,
                        "output": outputs[index],
                        "test_timeout": test_timeout,
                    },
                )
                for index, test_type in enumerate(tests_to_run)
            ]
    check_async_run_results(
        results=results,
        success=f"Tests {' '.join(tests_to_run)} completed successfully",
        outputs=outputs,
        include_success_outputs=include_success_outputs,
        skip_cleanup=skip_cleanup,
        summarize_on_ci=SummarizeAfter.FAILURE,
        summary_start_regexp=r".*= FAILURES.*|.*= ERRORS.*",
    )


def run_tests_in_parallel(
    exec_shell_params: ShellParams,
    test_types_list: list[str],
    extra_pytest_args: tuple,
    db_reset: bool,
    full_tests_needed: bool,
    test_timeout: int,
    include_success_outputs: bool,
    debug_resources: bool,
    parallelism: int,
    skip_cleanup: bool,
) -> None:
    import psutil

    memory_available = psutil.virtual_memory()
    if memory_available.available < LOW_MEMORY_CONDITION and exec_shell_params.backend in ["mssql", "mysql"]:
        # Run heavy tests sequentially
        heavy_test_types_to_run = {"Core", "Providers"} & set(test_types_list)
        if heavy_test_types_to_run:
            # some of those are requested
            get_console().print(
                f"[warning]Running {heavy_test_types_to_run} tests sequentially"
                f"for {exec_shell_params.backend}"
                f" backend due to low memory available: {bytes2human(memory_available.available)}"
            )
            tests_to_run_sequentially = []
            for heavy_test_type in heavy_test_types_to_run:
                for test_type in test_types_list:
                    if test_type.startswith(heavy_test_type):
                        test_types_list.remove(test_type)
                        tests_to_run_sequentially.append(test_type)
            _run_tests_in_pool(
                tests_to_run=tests_to_run_sequentially,
                parallelism=1,
                exec_shell_params=exec_shell_params,
                extra_pytest_args=extra_pytest_args,
                test_timeout=test_timeout,
                db_reset=db_reset,
                include_success_outputs=include_success_outputs,
                debug_resources=debug_resources,
                skip_cleanup=skip_cleanup,
            )
    _run_tests_in_pool(
        tests_to_run=test_types_list,
        parallelism=parallelism,
        exec_shell_params=exec_shell_params,
        extra_pytest_args=extra_pytest_args,
        test_timeout=test_timeout,
        db_reset=db_reset,
        include_success_outputs=include_success_outputs,
        debug_resources=debug_resources,
        skip_cleanup=skip_cleanup,
    )


@testing.command(
    name="tests",
    help="Run the specified unit test targets.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_python
@option_backend
@option_postgres_version
@option_mysql_version
@option_mssql_version
@option_integration
@option_image_tag_for_running
@option_mount_sources
@click.option(
    "--test-type",
    help="Type of test to run. Note that with Providers, you can also specify which provider "
    'tests should be run - for example --test-type "Providers[airbyte,http]"',
    default="All",
    type=NotVerifiedBetterChoice(ALLOWED_TEST_TYPE_CHOICES),
)
@click.option(
    "--test-timeout",
    help="Test timeout. Set the pytest setup, execution and teardown timeouts to this value",
    default=60,
    type=IntRange(min=0),
    show_default=True,
)
@option_db_reset
@option_run_in_parallel
@option_parallelism
@option_skip_cleanup
@option_debug_resources
@option_include_success_outputs
@click.option(
    "--test-types",
    help="Space separated list of test types used for testing in parallel.",
    default=" ".join(all_selective_test_types()),
    show_default=True,
    envvar="TEST_TYPES",
)
@click.option(
    "--full-tests-needed",
    help="Whether full set of tests is run.",
    is_flag=True,
    envvar="FULL_TESTS_NEEDED",
)
@option_verbose
@option_dry_run
@click.argument("extra_pytest_args", nargs=-1, type=click.UNPROCESSED)
def tests(
    python: str,
    backend: str,
    postgres_version: str,
    mysql_version: str,
    mssql_version: str,
    integration: tuple,
    test_type: str,
    test_timeout: int,
    db_reset: bool,
    image_tag: str | None,
    run_in_parallel: bool,
    parallelism: int,
    skip_cleanup: bool,
    debug_resources: bool,
    include_success_outputs: bool,
    test_types: str,
    full_tests_needed: bool,
    mount_sources: str,
    extra_pytest_args: tuple,
):
    docker_filesystem = get_filesystem_type("/var/lib/docker")
    get_console().print(f"Docker filesystem: {docker_filesystem}")
    exec_shell_params = ShellParams(
        python=python,
        backend=backend,
        integration=integration,
        postgres_version=postgres_version,
        mysql_version=mysql_version,
        mssql_version=mssql_version,
        image_tag=image_tag,
        mount_sources=mount_sources,
        forward_ports=False,
        test_type=test_type,
    )
    cleanup_python_generated_files()
    if run_in_parallel:
        run_tests_in_parallel(
            exec_shell_params=exec_shell_params,
            test_types_list=test_types.split(" "),
            extra_pytest_args=extra_pytest_args,
            db_reset=db_reset,
            # Allow to pass information on whether to use full tests in the parallel execution mode
            # or not - this will allow to skip some heavy tests on more resource-heavy configurations
            # in case full tests are not required, some of those will be skipped
            full_tests_needed=full_tests_needed,
            test_timeout=test_timeout,
            include_success_outputs=include_success_outputs,
            parallelism=parallelism,
            skip_cleanup=skip_cleanup,
            debug_resources=debug_resources,
        )
    else:
        returncode, _ = _run_test(
            exec_shell_params=exec_shell_params,
            extra_pytest_args=extra_pytest_args,
            db_reset=db_reset,
            output=None,
            test_timeout=test_timeout,
        )
        sys.exit(returncode)


@testing.command(
    name="integration-tests",
    help="Run the specified integratio tests.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_python
@option_backend
@option_postgres_version
@option_mysql_version
@option_mssql_version
@option_image_tag_for_running
@option_mount_sources
@option_integration
@click.option(
    "--test-timeout",
    help="Test timeout. Set the pytest setup, execution and teardown timeouts to this value",
    default=60,
    type=IntRange(min=0),
    show_default=True,
)
@option_db_reset
@option_verbose
@option_dry_run
@click.argument("extra_pytest_args", nargs=-1, type=click.UNPROCESSED)
def integration_tests(
    python: str,
    backend: str,
    postgres_version: str,
    mysql_version: str,
    mssql_version: str,
    integration: tuple,
    test_timeout: int,
    db_reset: bool,
    image_tag: str | None,
    mount_sources: str,
    extra_pytest_args: tuple,
):
    docker_filesystem = get_filesystem_type("/var/lib/docker")
    get_console().print(f"Docker filesystem: {docker_filesystem}")
    exec_shell_params = ShellParams(
        python=python,
        backend=backend,
        integration=integration,
        postgres_version=postgres_version,
        mysql_version=mysql_version,
        mssql_version=mssql_version,
        image_tag=image_tag,
        mount_sources=mount_sources,
        forward_ports=False,
        test_type="Integration",
    )
    cleanup_python_generated_files()
    returncode, _ = _run_test(
        exec_shell_params=exec_shell_params,
        extra_pytest_args=extra_pytest_args,
        db_reset=db_reset,
        output=None,
        test_timeout=test_timeout,
        output_outside_the_group=True,
    )
    sys.exit(returncode)


@testing.command(
    name="helm-tests",
    help="Run Helm chart tests.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_image_tag_for_running
@option_mount_sources
@option_github_repository
@option_verbose
@option_dry_run
@click.argument("extra_pytest_args", nargs=-1, type=click.UNPROCESSED)
def helm_tests(
    extra_pytest_args: tuple,
    image_tag: str | None,
    mount_sources: str,
    github_repository: str,
):
    exec_shell_params = ShellParams(
        image_tag=image_tag,
        mount_sources=mount_sources,
        github_repository=github_repository,
    )
    env_variables = get_env_variables_for_docker_commands(exec_shell_params)
    env_variables["RUN_TESTS"] = "true"
    env_variables["TEST_TYPE"] = "Helm"
    perform_environment_checks()
    cleanup_python_generated_files()
    cmd = [*DOCKER_COMPOSE_COMMAND, "run", "--service-ports", "--rm", "airflow"]
    cmd.extend(list(extra_pytest_args))
    result = run_command(cmd, env=env_variables, check=False, output_outside_the_group=True)
    sys.exit(result.returncode)
