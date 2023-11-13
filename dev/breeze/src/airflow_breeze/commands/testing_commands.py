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
from time import sleep

import click
from click import IntRange

from airflow_breeze.commands.ci_image_commands import rebuild_or_pull_ci_image_if_needed
from airflow_breeze.global_constants import (
    ALLOWED_HELM_TEST_PACKAGES,
)
from airflow_breeze.params.build_prod_params import BuildProdParams
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.common_options import (
    option_backend,
    option_collect_only,
    option_db_reset,
    option_debug_resources,
    option_downgrade_sqlalchemy,
    option_dry_run,
    option_enable_coverage,
    option_excluded_parallel_test_types,
    option_github_repository,
    option_image_name,
    option_image_tag_for_running,
    option_include_success_outputs,
    option_integration,
    option_mount_sources,
    option_mssql_version,
    option_mysql_version,
    option_parallel_test_types,
    option_parallelism,
    option_postgres_version,
    option_python,
    option_remove_arm_packages,
    option_run_db_tests_only,
    option_run_in_parallel,
    option_skip_cleanup,
    option_skip_db_tests,
    option_skip_docker_compose_down,
    option_skip_provider_tests,
    option_test_timeout,
    option_test_type,
    option_upgrade_boto,
    option_use_airflow_version,
    option_use_xdist,
    option_verbose,
)
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.custom_param_types import BetterChoice
from airflow_breeze.utils.docker_command_utils import (
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
from airflow_breeze.utils.run_tests import (
    file_name_from_test_type,
    generate_args_for_pytest,
    run_docker_compose_tests,
)
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
@click.option(
    "--skip-docker-compose-deletion",
    help="Skip deletion of docker-compose instance after the test",
    envvar="SKIP_DOCKER_COMPOSE_DELETION",
    is_flag=True,
)
@option_github_repository
@option_verbose
@option_dry_run
@click.argument("extra_pytest_args", nargs=-1, type=click.UNPROCESSED)
def docker_compose_tests(
    python: str,
    image_name: str,
    image_tag: str | None,
    skip_docker_compose_deletion: bool,
    github_repository: str,
    extra_pytest_args: tuple,
):
    """Run docker-compose tests."""
    perform_environment_checks()
    if image_name is None:
        build_params = BuildProdParams(
            python=python, image_tag=image_tag, github_repository=github_repository
        )
        image_name = build_params.airflow_image_name_with_tag
    get_console().print(f"[info]Running docker-compose with PROD image: {image_name}[/]")
    return_code, info = run_docker_compose_tests(
        image_name=image_name,
        extra_pytest_args=extra_pytest_args,
        skip_docker_compose_deletion=skip_docker_compose_deletion,
    )
    sys.exit(return_code)


TEST_PROGRESS_REGEXP = r"tests/.*|.*=====.*"
PERCENT_TEST_PROGRESS_REGEXP = r"^tests/.*\[[ \d%]*\].*|^\..*\[[ \d%]*\].*"


def _run_test(
    exec_shell_params: ShellParams,
    extra_pytest_args: tuple,
    db_reset: bool,
    output: Output | None,
    test_timeout: int,
    output_outside_the_group: bool = False,
    skip_docker_compose_down: bool = False,
) -> tuple[int, str]:
    env_variables = get_env_variables_for_docker_commands(exec_shell_params)
    env_variables["RUN_TESTS"] = "true"
    if db_reset:
        env_variables["DB_RESET"] = "true"
    env_variables["TEST_TYPE"] = exec_shell_params.test_type
    env_variables["COLLECT_ONLY"] = str(exec_shell_params.collect_only).lower()
    env_variables["REMOVE_ARM_PACKAGES"] = str(exec_shell_params.remove_arm_packages).lower()
    env_variables["SUSPENDED_PROVIDERS_FOLDERS"] = " ".join(get_suspended_providers_folders()).strip()
    if "[" in exec_shell_params.test_type and not exec_shell_params.test_type.startswith("Providers"):
        get_console(output=output).print(
            "[error]Only 'Providers' test type can specify actual tests with \\[\\][/]"
        )
        sys.exit(1)
    project_name = file_name_from_test_type(exec_shell_params.test_type)
    compose_project_name = f"airflow-test-{project_name}"
    # This is needed for Docker-compose 1 compatibility
    env_variables["COMPOSE_PROJECT_NAME"] = compose_project_name
    down_cmd = [
        "docker",
        "compose",
        "--project-name",
        compose_project_name,
        "down",
        "--remove-orphans",
        "--volumes",
    ]
    run_command(down_cmd, env=env_variables, output=output, check=False)
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
    run_cmd.extend(
        generate_args_for_pytest(
            test_type=exec_shell_params.test_type,
            test_timeout=test_timeout,
            skip_provider_tests=exec_shell_params.skip_provider_tests,
            skip_db_tests=exec_shell_params.skip_db_tests,
            run_db_tests_only=exec_shell_params.run_db_tests_only,
            backend=exec_shell_params.backend,
            use_xdist=exec_shell_params.use_xdist,
            enable_coverage=exec_shell_params.enable_coverage,
            collect_only=exec_shell_params.collect_only,
            parallelism=exec_shell_params.parallelism,
            parallel_test_types_list=exec_shell_params.parallel_test_types_list,
            helm_test_package=None,
        )
    )
    run_cmd.extend(list(extra_pytest_args))
    try:
        remove_docker_networks(networks=[f"{compose_project_name}_default"])
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
            get_console(output=output).print("[info]Wait 10 seconds for logs to find their way to stderr.\n")
            sleep(10)
            get_console(output=output).print(
                f"[info]Error {result.returncode}. Dumping containers: {container_ids} for {project_name}.\n"
            )
            date_str = datetime.now().strftime("%Y_%d_%m_%H_%M_%S")
            for container_id in container_ids:
                if compose_project_name not in container_id:
                    continue
                dump_path = FILES_DIR / f"container_logs_{container_id}_{date_str}.log"
                get_console(output=output).print(f"[info]Dumping container {container_id} to {dump_path}\n")
                with open(dump_path, "w") as outfile:
                    run_command(
                        ["docker", "logs", "--details", "--timestamps", container_id],
                        check=False,
                        stdout=outfile,
                    )
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
                env=env_variables,
                output=output,
                check=False,
                verbose_override=False,
            )
            remove_docker_networks(networks=[f"{compose_project_name}_default"])
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
    skip_docker_compose_down: bool,
):
    if not tests_to_run:
        return
    # this should be hard-coded as we want to have very specific sequence of tests
    # Heaviest tests go first and lightest tests go last. This way we can maximise parallelism as the
    # lightest tests will continue to complete and new light tests will get added while the heavy
    # tests are still running. We are only adding here test types that take more than 2 minutes to run
    # on a fast machine in parallel
    sorting_order = [
        "Providers",
        "Providers[-amazon,google]",
        "Core",
        "WWW",
        "CLI",
        "Other",
        "Serialization",
        "Always",
        "PythonVenv",
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
            results = [
                pool.apply_async(
                    _run_test,
                    kwds={
                        "exec_shell_params": exec_shell_params.clone_with_test(test_type=test_type),
                        "extra_pytest_args": extra_pytest_args,
                        "db_reset": db_reset,
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
        success=f"Tests {' '.join(escaped_tests)} completed successfully",
        outputs=outputs,
        include_success_outputs=include_success_outputs,
        skip_cleanup=skip_cleanup,
        summarize_on_ci=SummarizeAfter.FAILURE,
        summary_start_regexp=r".*= FAILURES.*|.*= ERRORS.*",
    )


def run_tests_in_parallel(
    exec_shell_params: ShellParams,
    extra_pytest_args: tuple,
    db_reset: bool,
    test_timeout: int,
    include_success_outputs: bool,
    debug_resources: bool,
    parallelism: int,
    skip_cleanup: bool,
    skio_docker_compose_down: bool,
) -> None:
    _run_tests_in_pool(
        tests_to_run=exec_shell_params.parallel_test_types_list,
        parallelism=parallelism,
        exec_shell_params=exec_shell_params,
        extra_pytest_args=extra_pytest_args,
        test_timeout=test_timeout,
        db_reset=db_reset,
        include_success_outputs=include_success_outputs,
        debug_resources=debug_resources,
        skip_cleanup=skip_cleanup,
        skip_docker_compose_down=skio_docker_compose_down,
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


@group_for_testing.command(
    name="tests",
    help="Run the specified unit tests. This is a low level testing command that allows you to run "
    "various kind of tests subset with a number of options. You can also use dedicated commands such"
    "us db_tests, non_db_tests, integration_tests for more opinionated test suite execution.",
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
@option_use_airflow_version
@option_mount_sources
@option_test_type
@option_test_timeout
@option_run_db_tests_only
@option_skip_db_tests
@option_db_reset
@option_run_in_parallel
@option_parallelism
@option_skip_cleanup
@option_debug_resources
@option_include_success_outputs
@option_parallel_test_types
@option_excluded_parallel_test_types
@option_upgrade_boto
@option_downgrade_sqlalchemy
@option_collect_only
@option_remove_arm_packages
@option_skip_docker_compose_down
@option_use_xdist
@option_skip_provider_tests
@option_enable_coverage
@option_verbose
@option_dry_run
@option_github_repository
@click.argument("extra_pytest_args", nargs=-1, type=click.UNPROCESSED)
def command_for_tests(**kwargs):
    _run_test_command(**kwargs)


@group_for_testing.command(
    name="db-tests",
    help="Run all (default) or specified DB-bound unit tests. This is a dedicated command that only runs "
    "DB tests and it runs them in parallel via splitting tests by test types into separate "
    "containers with separate database started for each container.",
    context_settings=dict(
        ignore_unknown_options=False,
        allow_extra_args=False,
    ),
)
@option_python
@option_backend
@option_postgres_version
@option_mysql_version
@option_mssql_version
@option_image_tag_for_running
@option_use_airflow_version
@option_mount_sources
@option_test_timeout
@option_parallelism
@option_skip_cleanup
@option_debug_resources
@option_include_success_outputs
@option_parallel_test_types
@option_excluded_parallel_test_types
@option_upgrade_boto
@option_downgrade_sqlalchemy
@option_collect_only
@option_remove_arm_packages
@option_skip_docker_compose_down
@option_skip_provider_tests
@option_enable_coverage
@option_verbose
@option_dry_run
@option_github_repository
def command_for_db_tests(**kwargs):
    _run_test_command(
        integration=(),
        run_in_parallel=True,
        use_xdist=False,
        skip_db_tests=False,
        run_db_tests_only=True,
        test_type="Default",
        db_reset=True,
        extra_pytest_args=(),
        **kwargs,
    )


@group_for_testing.command(
    name="non-db-tests",
    help="Run all (default) or specified Non-DB unit tests. This is a dedicated command that only"
    "runs Non-DB tests and it runs them in parallel via pytest-xdist in single container, "
    "with `none` backend set.",
    context_settings=dict(
        ignore_unknown_options=False,
        allow_extra_args=False,
    ),
)
@option_python
@option_image_tag_for_running
@option_use_airflow_version
@option_mount_sources
@option_test_timeout
@option_parallelism
@option_skip_cleanup
@option_debug_resources
@option_include_success_outputs
@option_parallel_test_types
@option_excluded_parallel_test_types
@option_upgrade_boto
@option_downgrade_sqlalchemy
@option_collect_only
@option_remove_arm_packages
@option_skip_docker_compose_down
@option_skip_provider_tests
@option_enable_coverage
@option_verbose
@option_dry_run
@option_github_repository
def command_for_non_db_tests(**kwargs):
    _run_test_command(
        integration=(),
        run_in_parallel=False,
        use_xdist=True,
        skip_db_tests=True,
        run_db_tests_only=False,
        test_type="Default",
        db_reset=False,
        backend="none",
        extra_pytest_args=(),
        **kwargs,
    )


def _run_test_command(
    *,
    backend: str,
    collect_only: bool,
    db_reset: bool,
    debug_resources: bool,
    downgrade_sqlalchemy: bool,
    enable_coverage: bool,
    excluded_parallel_test_types: str,
    extra_pytest_args: tuple,
    github_repository: str,
    image_tag: str | None,
    include_success_outputs: bool,
    integration: tuple[str, ...],
    mount_sources: str,
    parallel_test_types: str,
    parallelism: int,
    python: str,
    remove_arm_packages: bool,
    run_db_tests_only: bool,
    run_in_parallel: bool,
    skip_cleanup: bool,
    skip_db_tests: bool,
    skip_docker_compose_down: bool,
    skip_provider_tests: bool,
    test_timeout: int,
    test_type: str,
    upgrade_boto: bool,
    use_airflow_version: str | None,
    use_xdist: bool,
    mssql_version: str = "",
    mysql_version: str = "",
    postgres_version: str = "",
):
    docker_filesystem = get_filesystem_type("/var/lib/docker")
    get_console().print(f"Docker filesystem: {docker_filesystem}")
    _verify_parallelism_parameters(
        excluded_parallel_test_types, run_db_tests_only, run_in_parallel, use_xdist
    )
    test_list = parallel_test_types.split(" ")
    excluded_test_list = excluded_parallel_test_types.split(" ")
    if excluded_test_list:
        test_list = [test for test in test_list if test not in excluded_test_list]
    if skip_provider_tests or "Providers" in excluded_test_list:
        test_list = [test for test in test_list if not test.startswith("Providers")]
    exec_shell_params = ShellParams(
        python=python,
        backend=backend,
        integration=integration,
        postgres_version=postgres_version,
        mysql_version=mysql_version,
        mssql_version=mssql_version,
        image_tag=image_tag,
        use_airflow_version=use_airflow_version,
        mount_sources=mount_sources,
        forward_ports=False,
        test_type=test_type,
        upgrade_boto=upgrade_boto,
        downgrade_sqlalchemy=downgrade_sqlalchemy,
        collect_only=collect_only,
        remove_arm_packages=remove_arm_packages,
        github_repository=github_repository,
        run_db_tests_only=run_db_tests_only,
        skip_db_tests=skip_db_tests,
        use_xdist=use_xdist,
        enable_coverage=enable_coverage,
        parallelism=parallelism,
        skip_provider_tests=skip_provider_tests,
        parallel_test_types_list=test_list,
    )
    rebuild_or_pull_ci_image_if_needed(command_params=exec_shell_params)
    cleanup_python_generated_files()
    perform_environment_checks()
    if run_in_parallel:
        if test_type != "Default":
            get_console().print(
                "[error]You should not specify --test-type when --run-in-parallel is set[/]. "
                f"Your test type = {test_type}\n"
            )
            sys.exit(1)
        run_tests_in_parallel(
            exec_shell_params=exec_shell_params,
            extra_pytest_args=extra_pytest_args,
            db_reset=db_reset,
            test_timeout=test_timeout,
            include_success_outputs=include_success_outputs,
            parallelism=parallelism,
            skip_cleanup=skip_cleanup,
            debug_resources=debug_resources,
            skio_docker_compose_down=skip_docker_compose_down,
        )
    else:
        if exec_shell_params.test_type == "Default":
            if any([arg.startswith("tests") for arg in extra_pytest_args]):
                # in case some tests are specified as parameters, do not pass "tests" as default
                exec_shell_params.test_type = "None"
                exec_shell_params.parallel_test_types_list = []
            else:
                exec_shell_params.test_type = "All"
        returncode, _ = _run_test(
            exec_shell_params=exec_shell_params,
            extra_pytest_args=extra_pytest_args,
            db_reset=db_reset,
            output=None,
            test_timeout=test_timeout,
            output_outside_the_group=True,
            skip_docker_compose_down=skip_docker_compose_down,
        )
        sys.exit(returncode)


@group_for_testing.command(
    name="integration-tests",
    help="Run the specified integration tests.",
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
@option_enable_coverage
@option_github_repository
@click.option(
    "--test-timeout",
    help="Test timeout. Set the pytest setup, execution and teardown timeouts to this value",
    default=60,
    type=IntRange(min=0),
    show_default=True,
)
@option_skip_provider_tests
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
    enable_coverage: bool,
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
        enable_coverage=enable_coverage,
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
@option_test_timeout
@option_parallelism
@option_use_xdist
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
    test_timeout: int,
    parallelism: int,
    use_xdist: bool,
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
    pytest_args = generate_args_for_pytest(
        test_type="Helm",
        test_timeout=test_timeout,
        skip_provider_tests=True,
        skip_db_tests=False,
        run_db_tests_only=False,
        backend="none",
        use_xdist=use_xdist,
        enable_coverage=False,
        collect_only=False,
        parallelism=parallelism,
        parallel_test_types_list=[],
        helm_test_package=helm_test_package,
    )
    cmd = ["docker", "compose", "run", "--service-ports", "--rm", "airflow", *pytest_args, *extra_pytest_args]
    result = run_command(cmd, env=env_variables, check=False, output_outside_the_group=True)
    sys.exit(result.returncode)
