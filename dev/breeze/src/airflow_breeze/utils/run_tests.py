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
import sys
from itertools import chain
from pathlib import Path

from airflow_breeze.global_constants import (
    ALL_TEST_SUITES,
    ALL_TEST_TYPE,
    NONE_TEST_TYPE,
    GroupOfTests,
    SelectiveCoreTestType,
    all_helm_test_packages,
)
from airflow_breeze.utils.confirm import Answer, confirm_action
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.packages import get_excluded_provider_folders, get_suspended_provider_folders
from airflow_breeze.utils.path_utils import (
    AIRFLOW_PROVIDERS_ROOT_PATH,
    AIRFLOW_ROOT_PATH,
)
from airflow_breeze.utils.run_utils import run_command

DOCKER_TESTS_ROOT_PATH = AIRFLOW_ROOT_PATH / "docker-tests"
DOCKER_TESTS_TESTS_MODULE_PATH = DOCKER_TESTS_ROOT_PATH / "tests" / "docker_tests"
DOCKER_TESTS_REQUIREMENTS = DOCKER_TESTS_ROOT_PATH / "requirements.txt"

TASK_SDK_INTEGRATION_TESTS_ROOT_PATH = AIRFLOW_ROOT_PATH / "task-sdk-integration-tests"
TASK_SDK_TESTS_TESTS_MODULE_PATH = TASK_SDK_INTEGRATION_TESTS_ROOT_PATH / "tests" / "task_sdk_tests"
TASK_SDK_TESTS_REQUIREMENTS = TASK_SDK_INTEGRATION_TESTS_ROOT_PATH / "requirements.txt"

AIRFLOW_E2E_TESTS_ROOT_PATH = AIRFLOW_ROOT_PATH / "airflow-e2e-tests"

AIRFLOW_CTL_TESTS_ROOT_PATH = AIRFLOW_ROOT_PATH / "airflow-ctl-tests"

IGNORE_DB_INIT_FOR_TEST_GROUPS = [
    GroupOfTests.HELM,
    GroupOfTests.PYTHON_API_CLIENT,
    GroupOfTests.SYSTEM,
]

IGNORE_WARNING_OUTPUT_FOR_TEST_GROUPS = [
    GroupOfTests.HELM,
    GroupOfTests.PYTHON_API_CLIENT,
]


def verify_an_image(
    image_name: str,
    image_type: str,
    output: Output | None,
    slim_image: bool,
    extra_pytest_args: tuple[str, ...],
) -> tuple[int, str]:
    command_result = run_command(
        ["docker", "inspect", image_name],
        check=False,
        output=output,
    )
    if command_result.returncode != 0:
        get_console(output=output).print(
            f"[error]Error when inspecting {image_type} image: {command_result.returncode}[/]"
        )
        return command_result.returncode, f"Testing {image_type} python {image_name}"
    pytest_args = ("-n", str(os.cpu_count()), "--color=yes")
    if image_type == "PROD":
        test_path = DOCKER_TESTS_TESTS_MODULE_PATH / "test_prod_image.py"
    else:
        test_path = DOCKER_TESTS_TESTS_MODULE_PATH / "test_ci_image.py"
    env = os.environ.copy()
    env["DOCKER_IMAGE"] = image_name
    if slim_image:
        env["TEST_SLIM_IMAGE"] = "true"
    command_result = run_command(
        ["uv", "run", "--isolated", "pytest", test_path.as_posix(), *pytest_args, *extra_pytest_args],
        env=env,
        output=output,
        check=False,
        cwd=DOCKER_TESTS_ROOT_PATH,
    )
    return command_result.returncode, f"Testing {image_type} python {image_name}"


def run_docker_compose_tests(
    image_name: str,
    python_version: str,
    extra_pytest_args: tuple,
    skip_docker_compose_deletion: bool,
    skip_mounting_local_volumes: bool,
    include_success_outputs: bool,
    test_type: str = "docker-compose",
    skip_image_check: bool = False,
    test_mode: str = "basic",
) -> tuple[int, str]:
    if not skip_image_check:
        command_result = run_command(
            ["docker", "inspect", image_name], check=False, capture_output=True, text=True
        )
        if command_result.returncode != 0:
            get_console().print(f"[error]Error when inspecting PROD image: {command_result.returncode}[/]")
            get_console().print(command_result.stderr or "", highlight=False)
            if "no such object" in command_result.stderr:
                get_console().print(
                    f"The image {image_name} does not exist locally. "
                    f"It should be build before running docker-compose tests."
                )
                answer = confirm_action(
                    "Should we build it now? No will",
                    default_answer=Answer.YES,
                    quit_allowed=False,
                    timeout=20,
                )
                if answer:
                    get_console().print(f"[info]Building image {image_name}[/]")
                    run_command(["breeze", "prod-image", "build", "--python", python_version], check=True)

                else:
                    get_console().print(
                        f"[error]Cannot run docker-compose tests without the image {image_name} present locally.[/]"
                    )
                    get_console().print(
                        f"\nPlease build the image, using\n\n"
                        f"[special]breeze prod-image build --python {python_version}[/]\n\n"
                        f"and try again.\n"
                    )
                    sys.exit(1)

    if test_type == "task-sdk-integration":
        test_path = Path("tests") / "task_sdk_tests"
        cwd = TASK_SDK_INTEGRATION_TESTS_ROOT_PATH.as_posix()
    elif test_type == "airflow-e2e-tests":
        test_path = Path("tests") / "airflow_e2e_tests" / f"{test_mode}_tests"
        cwd = AIRFLOW_E2E_TESTS_ROOT_PATH.as_posix()
    elif test_type == "airflow-ctl-integration":
        test_path = Path("tests") / "airflowctl_tests" / "test_airflowctl_commands.py"
        cwd = AIRFLOW_CTL_TESTS_ROOT_PATH.as_posix()
    else:
        test_path = Path("tests") / "docker_tests" / "test_docker_compose_quick_start.py"
        cwd = DOCKER_TESTS_ROOT_PATH.as_posix()

    all_tests = [test_path.as_posix()]
    # Always with color and -s to see print outputs as they come
    pytest_args = ["--color=yes", "-s"]
    if not any(pytest_arg.startswith("tests/") for pytest_arg in extra_pytest_args):
        # Only add all tests when no tests were specified on the command line
        pytest_args.extend(all_tests)
    else:
        pytest_args.extend(extra_pytest_args)

    env = os.environ.copy()
    env["DOCKER_IMAGE"] = image_name
    env["E2E_TEST_MODE"] = test_mode
    if skip_docker_compose_deletion:
        env["SKIP_DOCKER_COMPOSE_DELETION"] = "true"
    if skip_mounting_local_volumes:
        env["SKIP_MOUNTING_LOCAL_VOLUMES"] = "true"
    if include_success_outputs:
        env["INCLUDE_SUCCESS_OUTPUTS"] = "true"
    env["AIRFLOW_UID"] = str(os.getuid())
    command_result = run_command(
        ["uv", "run", "pytest", *pytest_args],
        env=env,
        check=False,
        cwd=cwd,
    )
    return command_result.returncode, f"Testing {test_type} python with {image_name}"


def file_name_from_test_type(test_type: str):
    test_type_no_brackets = test_type.lower().replace("[", "_").replace("]", "")
    return re.sub("[,.]", "_", test_type_no_brackets)[:30]


def test_paths(test_type: str, backend: str) -> tuple[str, str, str]:
    file_friendly_test_type = file_name_from_test_type(test_type)
    random_suffix = os.urandom(4).hex()
    result_log_file = f"/files/test_result-{file_friendly_test_type}-{backend}.xml"
    warnings_file = f"/files/warnings-{file_friendly_test_type}-{backend}.txt"
    coverage_file = f"/files/coverage-{file_friendly_test_type}-{backend}-{random_suffix}.xml"
    return result_log_file, warnings_file, coverage_file


def get_ignore_switches_for_provider(provider_folders: list[str]) -> list[str]:
    return [f"--ignore=providers/{providers}/tests" for providers in provider_folders]


def get_test_folders(provider_folders: list[str]) -> list[str]:
    return [f"providers/{providers}/tests" for providers in provider_folders]


def get_suspended_provider_ignore_args() -> list[str]:
    suspended_folders = get_suspended_provider_folders()
    return get_ignore_switches_for_provider(suspended_folders)


def get_excluded_provider_ignore_args(python_version: str) -> list[str]:
    excluded_folders = get_excluded_provider_folders(python_version)
    return get_ignore_switches_for_provider(excluded_folders)


def get_suspended_test_provider_folders() -> list[str]:
    suspended_folders = get_suspended_provider_folders()
    return get_test_folders(suspended_folders)


def get_excluded_test_provider_folders(python_version: str) -> list[str]:
    excluded_folders = get_excluded_provider_folders(python_version)
    return get_test_folders(excluded_folders)


TEST_TYPE_CORE_MAP_TO_PYTEST_ARGS: dict[str, list[str]] = {
    "Always": ["airflow-core/tests/unit/always"],
    "API": ["airflow-core/tests/unit/api", "airflow-core/tests/unit/api_fastapi"],
    "CLI": ["airflow-core/tests/unit/cli"],
    "Core": [
        "airflow-core/tests/unit/core",
        "airflow-core/tests/unit/executors",
        "airflow-core/tests/unit/jobs",
        "airflow-core/tests/unit/models",
        "airflow-core/tests/unit/ti_deps",
        "airflow-core/tests/unit/utils",
    ],
    "Integration": ["airflow-core/tests/integration"],
    "Serialization": [
        "airflow-core/tests/unit/serialization",
    ],
    "TaskSDK": ["task-sdk/tests"],
    "CTL": ["airflow-ctl/tests"],
    "OpenAPI": ["clients/python"],
}

ALL_PROVIDER_TEST_FOLDERS: list[str] = sorted(
    [path.relative_to(AIRFLOW_ROOT_PATH).as_posix() for path in AIRFLOW_ROOT_PATH.glob("providers/*/tests/")]
    + [
        path.relative_to(AIRFLOW_ROOT_PATH).as_posix()
        for path in AIRFLOW_ROOT_PATH.glob("providers/*/*/tests/")
    ]
)
ALL_PROVIDER_INTEGRATION_TEST_FOLDERS: list[str] = sorted(
    [
        path.relative_to(AIRFLOW_ROOT_PATH).as_posix()
        for path in AIRFLOW_ROOT_PATH.glob("providers/*/tests/integration/")
    ]
    + [
        path.relative_to(AIRFLOW_ROOT_PATH).as_posix()
        for path in AIRFLOW_ROOT_PATH.glob("providers/*/*/tests/integration/")
    ]
)


TEST_GROUP_TO_TEST_FOLDERS: dict[GroupOfTests, list[str]] = {
    GroupOfTests.CORE: ["airflow-core/tests/unit/"],
    GroupOfTests.PROVIDERS: ALL_PROVIDER_TEST_FOLDERS,
    GroupOfTests.TASK_SDK: ["task-sdk/tests"],
    GroupOfTests.CTL: ["airflow-ctl/tests"],
    GroupOfTests.HELM: ["helm-tests"],
    GroupOfTests.INTEGRATION_CORE: ["airflow-core/tests/integration"],
    GroupOfTests.INTEGRATION_PROVIDERS: ALL_PROVIDER_INTEGRATION_TEST_FOLDERS,
    GroupOfTests.PYTHON_API_CLIENT: ["clients/python"],
}


# Those directories are already ignored in pyproject.toml. We want to exclude them here as well.
NO_RECURSE_DIRS = [
    "airflow-core/tests/unit/_internals",
    "airflow-core/tests/unit/dags_with_system_exit",
    "airflow-core/tests/unit/dags_corrupted",
    "airflow-core/tests/unit/dags",
    "providers/google/tests/system/google/cloud/dataproc/resources",
    "providers/google/tests/system/google/cloud/gcs/resources",
]


def find_all_other_tests() -> list[str]:
    all_named_test_folders = list(chain.from_iterable(TEST_TYPE_CORE_MAP_TO_PYTEST_ARGS.values()))
    all_named_test_folders.extend(TEST_GROUP_TO_TEST_FOLDERS[GroupOfTests.PROVIDERS])
    all_named_test_folders.extend(TEST_GROUP_TO_TEST_FOLDERS[GroupOfTests.TASK_SDK])
    all_named_test_folders.extend(TEST_GROUP_TO_TEST_FOLDERS[GroupOfTests.CTL])
    all_named_test_folders.extend(TEST_GROUP_TO_TEST_FOLDERS[GroupOfTests.HELM])
    all_named_test_folders.extend(TEST_GROUP_TO_TEST_FOLDERS[GroupOfTests.INTEGRATION_CORE])
    all_named_test_folders.extend(TEST_GROUP_TO_TEST_FOLDERS[GroupOfTests.INTEGRATION_PROVIDERS])
    all_named_test_folders.append("airflow-core/tests/system")
    all_named_test_folders.append("providers/tests/system")
    all_named_test_folders.extend(NO_RECURSE_DIRS)

    all_current_test_folders = [
        str(path.relative_to(AIRFLOW_ROOT_PATH))
        for path in AIRFLOW_ROOT_PATH.glob("airflow-core/tests/unit/*")
        if path.is_dir() and path.name != "__pycache__"
    ]
    for named_test_folder in all_named_test_folders:
        if named_test_folder in all_current_test_folders:
            all_current_test_folders.remove(named_test_folder)
    return sorted(all_current_test_folders)


PROVIDERS_PREFIX = "Providers"
PROVIDERS_LIST_PREFIX = "Providers["
PROVIDERS_LIST_EXCLUDE_PREFIX = "Providers[-"


def convert_test_type_to_pytest_args(
    *,
    test_group: GroupOfTests,
    test_type: str,
    integration: tuple | None = None,
) -> list[str]:
    if test_type == "None":
        return []
    if test_type in ALL_TEST_SUITES:
        all_paths = [
            *TEST_GROUP_TO_TEST_FOLDERS[test_group],
            *ALL_TEST_SUITES[test_type],
        ]

        if integration and test_group == GroupOfTests.INTEGRATION_PROVIDERS:
            filtered_paths = [
                path
                for path in all_paths
                if any(path.endswith(f"{value}/tests/integration") for value in integration)
            ]

            return filtered_paths
        return all_paths

    if test_group == GroupOfTests.SYSTEM and test_type != NONE_TEST_TYPE:
        get_console().print(f"[error]Only {NONE_TEST_TYPE} should be allowed as test type[/]")
        sys.exit(1)
    if test_group == GroupOfTests.HELM:
        if test_type not in all_helm_test_packages():
            get_console().print(f"[error]Unknown helm test type: {test_type}[/]")
            sys.exit(1)
        helm_folder = TEST_GROUP_TO_TEST_FOLDERS[test_group][0]
        if test_type and test_type != ALL_TEST_TYPE:
            return [f"{helm_folder}/tests/helm_tests/{test_type}"]
        return [helm_folder]
    if test_type == SelectiveCoreTestType.OTHER.value and test_group == GroupOfTests.CORE:
        return find_all_other_tests()
    if test_group in [
        GroupOfTests.INTEGRATION_CORE,
        GroupOfTests.INTEGRATION_PROVIDERS,
    ]:
        if test_type != ALL_TEST_TYPE:
            get_console().print(f"[error]Unknown test type for {test_group}: {test_type}[/]")
            sys.exit(1)
    if test_group == GroupOfTests.PROVIDERS:
        if test_type.startswith(PROVIDERS_LIST_EXCLUDE_PREFIX):
            excluded_provider_list = test_type[len(PROVIDERS_LIST_EXCLUDE_PREFIX) : -1].split(",")
            providers_with_exclusions = TEST_GROUP_TO_TEST_FOLDERS[GroupOfTests.PROVIDERS].copy()
            for excluded_provider in excluded_provider_list:
                provider_test_to_exclude = f"providers/{excluded_provider.replace('.', '/')}/tests"
                if provider_test_to_exclude in providers_with_exclusions:
                    get_console().print(
                        f"[info]Removing {provider_test_to_exclude} from {providers_with_exclusions}[/]"
                    )
                    providers_with_exclusions.remove(provider_test_to_exclude)
            return providers_with_exclusions
        if test_type.startswith(PROVIDERS_LIST_PREFIX):
            provider_list = test_type[len(PROVIDERS_LIST_PREFIX) : -1].split(",")
            providers_to_test = []
            for provider in provider_list:
                provider_path = (
                    AIRFLOW_PROVIDERS_ROOT_PATH.joinpath(provider.replace(".", "/")).relative_to(
                        AIRFLOW_ROOT_PATH
                    )
                    / "tests"
                )
                if provider_path.is_dir():
                    providers_to_test.append(provider_path.as_posix())
                else:
                    get_console().print(
                        f"[error] {provider_path} does not exist for {provider} "
                        "- which means that this provider has no tests. This is bad idea. "
                        "Please add it (all providers should have at least a package in tests)."
                    )
                    sys.exit(1)
            return providers_to_test
        if not test_type.startswith(PROVIDERS_PREFIX):
            get_console().print(f"[error]Unknown test type for {GroupOfTests.PROVIDERS}: {test_type}[/]")
            sys.exit(1)
        return TEST_GROUP_TO_TEST_FOLDERS[test_group]
    if test_group == GroupOfTests.PYTHON_API_CLIENT:
        return TEST_GROUP_TO_TEST_FOLDERS[test_group]
    if test_group != GroupOfTests.CORE:
        get_console().print(f"[error]Only {GroupOfTests.CORE} should be allowed here[/]")
    test_dirs = TEST_TYPE_CORE_MAP_TO_PYTEST_ARGS.get(test_type)
    if test_dirs:
        return test_dirs.copy()
    get_console().print(f"[error]Unknown test type: {test_type}[/]")
    sys.exit(1)


def generate_args_for_pytest(
    *,
    test_group: GroupOfTests,
    test_type: str,
    test_timeout: int,
    skip_db_tests: bool,
    run_db_tests_only: bool,
    backend: str,
    use_xdist: bool,
    enable_coverage: bool,
    collect_only: bool,
    parallelism: int,
    parallel_test_types_list: list[str],
    python_version: str,
    keep_env_variables: bool,
    no_db_cleanup: bool,
    integration: tuple | None = None,
):
    result_log_file, warnings_file, coverage_file = test_paths(test_type, backend)
    if skip_db_tests and parallel_test_types_list:
        args = convert_parallel_types_to_folders(
            test_group=test_group,
            parallel_test_types_list=parallel_test_types_list,
        )
    else:
        args = convert_test_type_to_pytest_args(
            test_group=test_group,
            test_type=test_type,
            integration=integration,
        )
    args.extend(
        [
            "--verbosity=0",
            "--strict-markers",
            "--durations=100",
            "--maxfail=50",
            "--color=yes",
            f"--junitxml={result_log_file}",
            # timeouts in seconds for individual tests
            "--timeouts-order=moi",
            f"--setup-timeout={test_timeout}",
            f"--execution-timeout={test_timeout}",
            f"--teardown-timeout={test_timeout}",
            "--disable-warnings",
            # Only display summary for non-expected cases
            #
            # f - failed
            # E - error
            # X - xpassed (passed even if expected to fail)
            #
            # The following cases are not displayed:
            # x - xfailed (expected to fail and failed)
            # p - passed
            # P - passed with output
            #
            "-rfEX",
        ]
    )
    if skip_db_tests:
        args.append("--skip-db-tests")
    if run_db_tests_only:
        args.append("--run-db-tests-only")
    if test_group not in [GroupOfTests.SYSTEM]:
        args.append("--ignore-glob=*/tests/system/*")
    if test_group != GroupOfTests.INTEGRATION_CORE:
        for group_folder in TEST_GROUP_TO_TEST_FOLDERS[GroupOfTests.INTEGRATION_CORE]:
            args.append(f"--ignore-glob={group_folder}/*")
    if test_group != GroupOfTests.INTEGRATION_PROVIDERS:
        for group_folder in TEST_GROUP_TO_TEST_FOLDERS[GroupOfTests.INTEGRATION_PROVIDERS]:
            args.append(f"--ignore-glob={group_folder}/*")
    if test_group not in IGNORE_WARNING_OUTPUT_FOR_TEST_GROUPS:
        args.append(f"--warning-output-path={warnings_file}")
        for group_folder in TEST_GROUP_TO_TEST_FOLDERS[GroupOfTests.HELM]:
            args.append(f"--ignore={group_folder}")
    if test_group not in IGNORE_DB_INIT_FOR_TEST_GROUPS:
        args.append("--with-db-init")
    if test_group == GroupOfTests.SYSTEM:
        # System tests will be inited when the api server is started
        args.append("--without-db-init")
    if test_group == GroupOfTests.PYTHON_API_CLIENT:
        args.append("--ignore-glob=clients/python/tmp/*")
    args.extend(get_suspended_provider_ignore_args())
    args.extend(get_excluded_provider_ignore_args(python_version))
    suspended_test_folders = get_suspended_test_provider_folders()
    if suspended_test_folders:
        get_console().print(f"[info]Suspended test folders to remove: {suspended_test_folders}[/]")
        for suspended_test_folder in suspended_test_folders:
            if suspended_test_folder in args:
                get_console().print(f"[warning]Removing {suspended_test_folder}[/]")
                args.remove(suspended_test_folder)
    excluded_test_folders = get_excluded_test_provider_folders(python_version)
    if excluded_test_folders:
        get_console().print(f"[info]Excluded test folders to remove: {excluded_test_folders}[/]")
        for excluded_test_folder in excluded_test_folders:
            if excluded_test_folder in args:
                get_console().print(f"[warning]Removing {excluded_test_folder}[/]")
                args.remove(excluded_test_folder)
    if use_xdist:
        args.extend(["-n", str(parallelism) if parallelism else "auto"])
    # We have to disable coverage for Python 3.12 because of the issue with coverage that takes too long, despite
    # Using experimental support for Python 3.12 PEP 669. The coverage.py is not yet fully compatible with the
    # full scope of PEP-669. That will be fully done when https://github.com/nedbat/coveragepy/issues/1746 is
    # resolve for now we are disabling coverage for Python 3.12, and it causes slower execution and occasional
    # timeouts
    if enable_coverage and python_version not in ["3.12", "3.13"]:
        args.extend(
            [
                "--cov=airflow",
                "--cov-config=pyproject.toml",
                f"--cov-report=xml:{coverage_file}",
            ]
        )
    else:
        args.append("--no-cov")
    if collect_only:
        args.extend(
            [
                "--collect-only",
                "-qqqq",
                "--disable-warnings",
            ]
        )
    if keep_env_variables:
        args.append("--keep-env-variables")
    if no_db_cleanup:
        args.append("--no-db-cleanup")
    return args


def convert_parallel_types_to_folders(test_group: GroupOfTests, parallel_test_types_list: list[str]):
    args = []
    for _test_type in parallel_test_types_list:
        args.extend(
            convert_test_type_to_pytest_args(
                test_group=test_group,
                test_type=_test_type,
            )
        )
    all_test_prefixes: list[str] = []
    # leave only folders, strip --pytest-args that exclude some folders with `-` prefix
    for group_folders in TEST_GROUP_TO_TEST_FOLDERS.values():
        for group_folder in group_folders:
            all_test_prefixes.append(group_folder)
    return [arg for arg in args if any(arg.startswith(prefix) for prefix in all_test_prefixes)]
