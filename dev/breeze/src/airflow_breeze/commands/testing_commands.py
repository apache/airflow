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
from airflow_breeze.commands.common_options import (
    option_backend,
    option_clean_airflow_installation,
    option_database_isolation,
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
    option_image_tag_for_running,
    option_include_success_outputs,
    option_integration,
    option_keep_env_variables,
    option_mount_sources,
    option_mysql_version,
    option_no_db_cleanup,
    option_parallelism,
    option_postgres_version,
    option_python,
    option_run_db_tests_only,
    option_run_in_parallel,
    option_skip_cleanup,
    option_skip_db_tests,
    option_upgrade_boto,
    option_use_airflow_version,
    option_verbose,
)
from airflow_breeze.commands.common_package_installation_options import (
    option_airflow_constraints_reference,
    option_install_airflow_with_constraints,
    option_providers_constraints_location,
    option_providers_skip_constraints,
    option_use_packages_from_dist,
)
from airflow_breeze.commands.release_management_commands import option_package_format
from airflow_breeze.global_constants import (
    ALLOWED_HELM_TEST_PACKAGES,
    ALLOWED_PARALLEL_TEST_TYPE_CHOICES,
    ALLOWED_TEST_TYPE_CHOICES,
)
from airflow_breeze.params.build_prod_params import BuildProdParams
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.custom_param_types import BetterChoice, NotVerifiedBetterChoice
from airflow_breeze.utils.docker_command_utils import (
    fix_ownership_using_docker,
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
from airflow_breeze.utils.selective_checks import ALL_CI_SELECTIVE_TEST_TYPES

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
    project_name = file_name_from_test_type(shell_params.test_type)
    compose_project_name = f"airflow-test-{project_name}"
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
        test_type=shell_params.test_type,
        test_timeout=test_timeout,
        skip_provider_tests=shell_params.skip_provider_tests,
        skip_db_tests=shell_params.skip_db_tests,
        run_db_tests_only=shell_params.run_db_tests_only,
        backend=shell_params.backend,
        use_xdist=shell_params.use_xdist,
        enable_coverage=shell_params.enable_coverage,
        collect_only=shell_params.collect_only,
        parallelism=shell_params.parallelism,
        python_version=python_version,
        parallel_test_types_list=shell_params.parallel_test_types_list,
        helm_test_package=None,
        keep_env_variables=shell_params.keep_env_variables,
        no_db_cleanup=shell_params.no_db_cleanup,
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
                output=output,
                check=False,
                env=env,
                verbose_override=False,
            )
            remove_docker_networks(networks=[f"{compose_project_name}_default"])
    return result.returncode, f"Test: {shell_params.test_type}"


def _run_tests_in_pool(
    tests_to_run: list[str],
    parallelism: int,
    shell_params: ShellParams,
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
        "Other",
        "Core",
        "PythonVenv",
        "WWW",
        "CLI",
        "Serialization",
        "Always",
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
        success=f"Tests {' '.join(escaped_tests)} completed successfully",
        outputs=outputs,
        include_success_outputs=include_success_outputs,
        skip_cleanup=skip_cleanup,
        summarize_on_ci=SummarizeAfter.FAILURE,
        summary_start_regexp=r".*= FAILURES.*|.*= ERRORS.*",
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
    db_reset: bool,
    test_timeout: int,
    include_success_outputs: bool,
    debug_resources: bool,
    parallelism: int,
    skip_cleanup: bool,
    skip_docker_compose_down: bool,
) -> None:
    get_console().print("\n[info]Summary of the tests to run\n")
    get_console().print(f"[info]Running tests in parallel with parallelism={parallelism}")
    get_console().print(f"[info]Extra pytest args: {extra_pytest_args}")
    get_console().print(f"[info]DB reset: {db_reset}")
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
        db_reset=db_reset,
        include_success_outputs=include_success_outputs,
        debug_resources=debug_resources,
        skip_cleanup=skip_cleanup,
        skip_docker_compose_down=skip_docker_compose_down,
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
option_excluded_parallel_test_types = click.option(
    "--excluded-parallel-test-types",
    help="Space separated list of test types that will be excluded from parallel tes runs.",
    default="",
    show_default=True,
    envvar="EXCLUDED_PARALLEL_TEST_TYPES",
    type=NotVerifiedBetterChoice(ALLOWED_PARALLEL_TEST_TYPE_CHOICES),
)
option_parallel_test_types = click.option(
    "--parallel-test-types",
    help="Space separated list of test types used for testing in parallel",
    default=ALL_CI_SELECTIVE_TEST_TYPES,
    show_default=True,
    envvar="PARALLEL_TEST_TYPES",
    type=NotVerifiedBetterChoice(ALLOWED_PARALLEL_TEST_TYPE_CHOICES),
)
option_skip_docker_compose_down = click.option(
    "--skip-docker-compose-down",
    help="Skips running docker-compose down after tests",
    is_flag=True,
    envvar="SKIP_DOCKER_COMPOSE_DOWN",
)
option_skip_provider_tests = click.option(
    "--skip-provider-tests",
    help="Skip provider tests",
    is_flag=True,
    envvar="SKIP_PROVIDER_TESTS",
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
option_test_type = click.option(
    "--test-type",
    help="Type of test to run. With Providers, you can specify tests of which providers "
    "should be run: `Providers[airbyte,http]` or "
    "excluded from the full test suite: `Providers[-amazon,google]`",
    default="Default",
    envvar="TEST_TYPE",
    show_default=True,
    type=NotVerifiedBetterChoice(ALLOWED_TEST_TYPE_CHOICES),
)
option_use_xdist = click.option(
    "--use-xdist",
    help="Use xdist plugin for pytest",
    is_flag=True,
    envvar="USE_XDIST",
)
option_remove_arm_packages = click.option(
    "--remove-arm-packages",
    help="Removes arm packages from the image to test if ARM collection works",
    is_flag=True,
    envvar="REMOVE_ARM_PACKAGES",
)
option_force_sa_warnings = click.option(
    "--force-sa-warnings/--no-force-sa-warnings",
    help="Enable `sqlalchemy.exc.MovedIn20Warning` during the tests runs.",
    is_flag=True,
    default=True,
    show_default=True,
    envvar="SQLALCHEMY_WARN_20",
)


@group_for_testing.command(
    name="tests",
    help="Run the specified unit tests. This is a low level testing command that allows you to run "
    "various kind of tests subset with a number of options. You can also use dedicated commands such "
    "as db_tests, non_db_tests, integration_tests for more opinionated test suite execution.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_airflow_constraints_reference
@option_backend
@option_collect_only
@option_clean_airflow_installation
@option_database_isolation
@option_db_reset
@option_debug_resources
@option_downgrade_pendulum
@option_downgrade_sqlalchemy
@option_dry_run
@option_enable_coverage
@option_excluded_providers
@option_excluded_parallel_test_types
@option_force_sa_warnings
@option_force_lowest_dependencies
@option_forward_credentials
@option_github_repository
@option_image_tag_for_running
@option_include_success_outputs
@option_integration
@option_install_airflow_with_constraints
@option_keep_env_variables
@option_mount_sources
@option_mysql_version
@option_no_db_cleanup
@option_package_format
@option_parallel_test_types
@option_parallelism
@option_postgres_version
@option_providers_constraints_location
@option_providers_skip_constraints
@option_python
@option_remove_arm_packages
@option_run_db_tests_only
@option_run_in_parallel
@option_skip_cleanup
@option_skip_db_tests
@option_skip_docker_compose_down
@option_skip_provider_tests
@option_skip_providers
@option_test_timeout
@option_test_type
@option_upgrade_boto
@option_use_airflow_version
@option_use_packages_from_dist
@option_use_xdist
@option_verbose
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
@option_airflow_constraints_reference
@option_backend
@option_collect_only
@option_clean_airflow_installation
@option_database_isolation
@option_debug_resources
@option_downgrade_pendulum
@option_downgrade_sqlalchemy
@option_dry_run
@option_enable_coverage
@option_excluded_parallel_test_types
@option_excluded_providers
@option_forward_credentials
@option_force_lowest_dependencies
@option_github_repository
@option_image_tag_for_running
@option_include_success_outputs
@option_install_airflow_with_constraints
@option_keep_env_variables
@option_mount_sources
@option_mysql_version
@option_no_db_cleanup
@option_package_format
@option_parallel_test_types
@option_parallelism
@option_postgres_version
@option_providers_constraints_location
@option_providers_skip_constraints
@option_python
@option_remove_arm_packages
@option_skip_cleanup
@option_skip_docker_compose_down
@option_skip_provider_tests
@option_skip_providers
@option_test_timeout
@option_upgrade_boto
@option_use_airflow_version
@option_use_packages_from_dist
@option_force_sa_warnings
@option_verbose
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
    help="Run all (default) or specified Non-DB unit tests. This is a dedicated command that only "
    "runs Non-DB tests and it runs them in parallel via pytest-xdist in single container, "
    "with `none` backend set.",
    context_settings=dict(
        ignore_unknown_options=False,
        allow_extra_args=False,
    ),
)
@option_airflow_constraints_reference
@option_collect_only
@option_clean_airflow_installation
@option_debug_resources
@option_downgrade_sqlalchemy
@option_downgrade_pendulum
@option_dry_run
@option_enable_coverage
@option_excluded_parallel_test_types
@option_excluded_providers
@option_forward_credentials
@option_force_lowest_dependencies
@option_github_repository
@option_image_tag_for_running
@option_include_success_outputs
@option_install_airflow_with_constraints
@option_keep_env_variables
@option_mount_sources
@option_no_db_cleanup
@option_package_format
@option_parallel_test_types
@option_parallelism
@option_providers_constraints_location
@option_providers_skip_constraints
@option_python
@option_remove_arm_packages
@option_skip_cleanup
@option_skip_docker_compose_down
@option_skip_provider_tests
@option_skip_providers
@option_test_timeout
@option_upgrade_boto
@option_use_airflow_version
@option_use_packages_from_dist
@option_force_sa_warnings
@option_verbose
def command_for_non_db_tests(**kwargs):
    _run_test_command(
        backend="none",
        database_isolation=False,
        db_reset=False,
        extra_pytest_args=(),
        integration=(),
        run_db_tests_only=False,
        run_in_parallel=False,
        skip_db_tests=True,
        test_type="Default",
        use_xdist=True,
        **kwargs,
    )


def _run_test_command(
    *,
    airflow_constraints_reference: str,
    backend: str,
    collect_only: bool,
    clean_airflow_installation: bool,
    db_reset: bool,
    database_isolation: bool,
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
    image_tag: str | None,
    include_success_outputs: bool,
    install_airflow_with_constraints: bool,
    integration: tuple[str, ...],
    keep_env_variables: bool,
    mount_sources: str,
    no_db_cleanup: bool,
    parallel_test_types: str,
    parallelism: int,
    package_format: str,
    providers_constraints_location: str,
    providers_skip_constraints: bool,
    python: str,
    remove_arm_packages: bool,
    run_db_tests_only: bool,
    run_in_parallel: bool,
    skip_cleanup: bool,
    skip_db_tests: bool,
    skip_docker_compose_down: bool,
    skip_provider_tests: bool,
    skip_providers: str,
    test_timeout: int,
    test_type: str,
    upgrade_boto: bool,
    use_airflow_version: str | None,
    use_packages_from_dist: bool,
    use_xdist: bool,
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
    shell_params = ShellParams(
        airflow_constraints_reference=airflow_constraints_reference,
        backend=backend,
        collect_only=collect_only,
        clean_airflow_installation=clean_airflow_installation,
        database_isolation=database_isolation,
        downgrade_sqlalchemy=downgrade_sqlalchemy,
        downgrade_pendulum=downgrade_pendulum,
        enable_coverage=enable_coverage,
        excluded_providers=excluded_providers,
        force_sa_warnings=force_sa_warnings,
        force_lowest_dependencies=force_lowest_dependencies,
        forward_credentials=forward_credentials,
        forward_ports=False,
        github_repository=github_repository,
        image_tag=image_tag,
        integration=integration,
        install_airflow_with_constraints=install_airflow_with_constraints,
        keep_env_variables=keep_env_variables,
        mount_sources=mount_sources,
        mysql_version=mysql_version,
        no_db_cleanup=no_db_cleanup,
        package_format=package_format,
        parallel_test_types_list=test_list,
        parallelism=parallelism,
        postgres_version=postgres_version,
        providers_constraints_location=providers_constraints_location,
        providers_skip_constraints=providers_skip_constraints,
        python=python,
        remove_arm_packages=remove_arm_packages,
        run_db_tests_only=run_db_tests_only,
        skip_db_tests=skip_db_tests,
        skip_provider_tests=skip_provider_tests,
        test_type=test_type,
        upgrade_boto=upgrade_boto,
        use_airflow_version=use_airflow_version,
        use_packages_from_dist=use_packages_from_dist,
        use_xdist=use_xdist,
        run_tests=True,
        db_reset=db_reset,
    )
    rebuild_or_pull_ci_image_if_needed(command_params=shell_params)
    fix_ownership_using_docker()
    cleanup_python_generated_files()
    perform_environment_checks()
    if skip_providers:
        ignored_path_list = [
            f"--ignore=providers/tests/{provider_id.replace('.','/')}"
            for provider_id in skip_providers.split(" ")
        ]
        extra_pytest_args = (*extra_pytest_args, *ignored_path_list)
    if run_in_parallel:
        if test_type != "Default":
            get_console().print(
                "[error]You should not specify --test-type when --run-in-parallel is set[/]. "
                f"Your test type = {test_type}\n"
            )
            sys.exit(1)
        run_tests_in_parallel(
            shell_params=shell_params,
            extra_pytest_args=extra_pytest_args,
            db_reset=db_reset,
            test_timeout=test_timeout,
            include_success_outputs=include_success_outputs,
            parallelism=parallelism,
            skip_cleanup=skip_cleanup,
            debug_resources=debug_resources,
            skip_docker_compose_down=skip_docker_compose_down,
        )
    else:
        if shell_params.test_type == "Default":
            if any([arg.startswith("tests") for arg in extra_pytest_args]):
                # in case some tests are specified as parameters, do not pass "tests" as default
                shell_params.test_type = "None"
                shell_params.parallel_test_types_list = []
            else:
                shell_params.test_type = "All"
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


@group_for_testing.command(
    name="integration-tests",
    help="Run the specified integration tests.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_backend
@option_db_reset
@option_dry_run
@option_enable_coverage
@option_forward_credentials
@option_github_repository
@option_image_tag_for_running
@option_integration
@option_mount_sources
@option_mysql_version
@option_postgres_version
@option_python
@option_skip_provider_tests
@option_test_timeout
@option_force_sa_warnings
@option_verbose
@click.argument("extra_pytest_args", nargs=-1, type=click.UNPROCESSED)
def integration_tests(
    backend: str,
    db_reset: bool,
    enable_coverage: bool,
    extra_pytest_args: tuple,
    forward_credentials: bool,
    github_repository: str,
    image_tag: str | None,
    integration: tuple,
    mount_sources: str,
    mysql_version: str,
    postgres_version: str,
    python: str,
    skip_provider_tests: bool,
    force_sa_warnings: bool,
    test_timeout: int,
):
    docker_filesystem = get_filesystem_type("/var/lib/docker")
    get_console().print(f"Docker filesystem: {docker_filesystem}")
    shell_params = ShellParams(
        backend=backend,
        enable_coverage=enable_coverage,
        forward_credentials=forward_credentials,
        forward_ports=False,
        github_repository=github_repository,
        image_tag=image_tag,
        integration=integration,
        mount_sources=mount_sources,
        mysql_version=mysql_version,
        postgres_version=postgres_version,
        python=python,
        skip_provider_tests=skip_provider_tests,
        test_type="Integration",
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
    if helm_test_package == "all":
        helm_test_package = ""
    shell_params = ShellParams(
        image_tag=image_tag,
        mount_sources=mount_sources,
        github_repository=github_repository,
        run_tests=True,
        test_type="Helm",
        helm_test_package=helm_test_package,
    )
    env = shell_params.env_variables_for_docker_commands
    perform_environment_checks()
    fix_ownership_using_docker()
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
        python_version=shell_params.python,
        helm_test_package=helm_test_package,
        keep_env_variables=False,
        no_db_cleanup=False,
    )
    cmd = ["docker", "compose", "run", "--service-ports", "--rm", "airflow", *pytest_args, *extra_pytest_args]
    result = run_command(cmd, check=False, env=env, output_outside_the_group=True)
    fix_ownership_using_docker()
    sys.exit(result.returncode)
