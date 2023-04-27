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

from airflow_breeze.commands.ci_image_commands import rebuild_or_pull_ci_image_if_needed
from airflow_breeze.global_constants import (
    ALLOWED_HELM_TEST_PACKAGES,
    ALLOWED_TEST_TYPE_CHOICES,
    all_selective_test_types,
)
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
from airflow_breeze.utils.custom_param_types import BetterChoice, NotVerifiedBetterChoice
from airflow_breeze.utils.docker_command_utils import (
    DOCKER_COMPOSE_COMMAND,
    get_env_variables_for_docker_commands,
    perform_environment_checks,
    remove_docker_networks,
)
from airflow_breeze.utils.parallel import (
    GenericRegexpProgressMatcher,
    SummarizeAfter,
    check_async_run_results,
    run_with_pool,
)
from airflow_breeze.utils.path_utils import FILES_DIR, cleanup_python_generated_files
from airflow_breeze.utils.run_tests import file_name_from_test_type, run_docker_compose_tests
from airflow_breeze.utils.run_utils import get_filesystem_type, run_command
from airflow_breeze.utils.suspended_providers import get_suspended_providers_folders

LOW_MEMORY_CONDITION = 8 * 1024 * 1024 * 1024


@click.group(cls=BreezeGroup, name="testing", help="Tools that developers can use to run tests")
def group_for_testing():
    pass


@group_for_testing.command(
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
    env_variables["TEST_TYPE"] = exec_shell_params.test_type
    env_variables["COLLECT_ONLY"] = str(exec_shell_params.collect_only).lower()
    env_variables["REMOVE_ARM_PACKAGES"] = str(exec_shell_params.remove_arm_packages).lower()
    env_variables["SKIP_PROVIDER_TESTS"] = str(exec_shell_params.skip_provider_tests).lower()
    env_variables["SUSPENDED_PROVIDERS_FOLDERS"] = " ".join(get_suspended_providers_folders()).strip()
    if "[" in exec_shell_params.test_type and not exec_shell_params.test_type.startswith("Providers"):
        get_console(output=output).print(
            "[error]Only 'Providers' test type can specify actual tests with \\[\\][/]"
        )
        sys.exit(1)
    project_name = file_name_from_test_type(exec_shell_params.test_type)
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
        remove_docker_networks(networks=[f"airflow-test-{project_name}_default"])
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
        remove_docker_networks(networks=[f"airflow-test-{project_name}_default"])
    return result.returncode, f"Test: {exec_shell_params.test_type}"


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
    escaped_tests = [test.replace("[", "\\[") for test in tests_to_run]
    check_async_run_results(
        results=results,
        success=f"Tests {' '.join(escaped_tests)} completed successfully",
        outputs=outputs,
        include_success_outputs=include_success_outputs,
        skip_cleanup=skip_cleanup,
        summarize_on_ci=SummarizeAfter.FAILURE,
        summary_start_regexp=r".*= FAILURES.*|.*= ERRORS.*",
    )


def run_tests_in_parallel(
    exec_shell_params: ShellParams,
    parallel_test_types_list: list[str],
    extra_pytest_args: tuple,
    db_reset: bool,
    test_timeout: int,
    include_success_outputs: bool,
    debug_resources: bool,
    parallelism: int,
    skip_cleanup: bool,
) -> None:
    _run_tests_in_pool(
        tests_to_run=parallel_test_types_list,
        parallelism=parallelism,
        exec_shell_params=exec_shell_params,
        extra_pytest_args=extra_pytest_args,
        test_timeout=test_timeout,
        db_reset=db_reset,
        include_success_outputs=include_success_outputs,
        debug_resources=debug_resources,
        skip_cleanup=skip_cleanup,
    )


@group_for_testing.command(
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
    help="Type of test to run. With Providers, you can specify tests of which providers "
    "should be run: `Providers[airbyte,http]` or "
    "excluded from the full test suite: `Providers[-amazon,google]`",
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
    "--parallel-test-types",
    help="Space separated list of test types used for testing in parallel.",
    default=" ".join(all_selective_test_types()) + " PlainAsserts",
    show_default=True,
    envvar="PARALLEL_TEST_TYPES",
)
@click.option(
    "--upgrade-boto",
    help="Remove aiobotocore and upgrade botocore and boto to the latest version.",
    is_flag=True,
    envvar="UPGRADE_BOTO",
)
@click.option(
    "--collect-only",
    help="Collect tests only, do not run them.",
    is_flag=True,
    envvar="COLLECT_ONLY",
)
@click.option(
    "--remove-arm-packages",
    help="Removes arm packages from the image to test if ARM collection works",
    is_flag=True,
    envvar="REMOVE_ARM_PACKAGES",
)
@option_verbose
@option_dry_run
@option_github_repository
@click.argument("extra_pytest_args", nargs=-1, type=click.UNPROCESSED)
def command_for_tests(
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
    parallel_test_types: str,
    mount_sources: str,
    extra_pytest_args: tuple,
    upgrade_boto: bool,
    collect_only: bool,
    remove_arm_packages: bool,
    github_repository: str,
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
        upgrade_boto=upgrade_boto,
        collect_only=collect_only,
        remove_arm_packages=remove_arm_packages,
        github_repository=github_repository,
    )
    rebuild_or_pull_ci_image_if_needed(command_params=exec_shell_params)
    cleanup_python_generated_files()
    perform_environment_checks()
    if run_in_parallel:
        test_list = parallel_test_types.split(" ")
        run_tests_in_parallel(
            exec_shell_params=exec_shell_params,
            parallel_test_types_list=test_list,
            extra_pytest_args=extra_pytest_args,
            db_reset=db_reset,
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


@group_for_testing.command(
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
@option_github_repository
@click.option(
    "--test-timeout",
    help="Test timeout. Set the pytest setup, execution and teardown timeouts to this value",
    default=60,
    type=IntRange(min=0),
    show_default=True,
)
@click.option(
    "--skip-provider-tests",
    help="Skip provider tests",
    is_flag=True,
    envvar="SKIP_PROVIDER_TESTS",
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
    github_repository: str,
    test_timeout: int,
    skip_provider_tests: bool,
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
        skip_provider_tests=skip_provider_tests,
        github_repository=github_repository,
    )
    cleanup_python_generated_files()
    perform_environment_checks()
    returncode, _ = _run_test(
        exec_shell_params=exec_shell_params,
        extra_pytest_args=extra_pytest_args,
        db_reset=db_reset,
        output=None,
        test_timeout=test_timeout,
        output_outside_the_group=True,
    )
    sys.exit(returncode)


@group_for_testing.command(
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
@click.option(
    "--helm-test-package",
    help="Package to tests",
    default="all",
    type=BetterChoice(ALLOWED_HELM_TEST_PACKAGES),
)
@click.argument("extra_pytest_args", nargs=-1, type=click.UNPROCESSED)
def helm_tests(
    extra_pytest_args: tuple,
    image_tag: str | None,
    mount_sources: str,
    helm_test_package: str,
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
    if helm_test_package != "all":
        env_variables["HELM_TEST_PACKAGE"] = helm_test_package
    perform_environment_checks()
    cleanup_python_generated_files()
    cmd = [*DOCKER_COMPOSE_COMMAND, "run", "--service-ports", "--rm", "airflow"]
    cmd.extend(list(extra_pytest_args))
    result = run_command(cmd, env=env_variables, check=False, output_outside_the_group=True)
    sys.exit(result.returncode)
