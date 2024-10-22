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
from subprocess import DEVNULL

from airflow_breeze.global_constants import PIP_VERSION
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.packages import get_excluded_provider_folders, get_suspended_provider_folders
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT, TESTS_PROVIDERS_ROOT
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.virtualenv_utils import create_temp_venv

DOCKER_TESTS_ROOT = AIRFLOW_SOURCES_ROOT / "docker_tests"
DOCKER_TESTS_REQUIREMENTS = DOCKER_TESTS_ROOT / "requirements.txt"
COMMON_TESTS_PYTEST_PLUGIN = "tests_common.pytest_plugin"


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
        test_path = DOCKER_TESTS_ROOT / "test_prod_image.py"
    else:
        test_path = DOCKER_TESTS_ROOT / "test_ci_image.py"
    env = os.environ.copy()
    env["DOCKER_IMAGE"] = image_name
    if slim_image:
        env["TEST_SLIM_IMAGE"] = "true"
    with create_temp_venv(pip_version=PIP_VERSION, requirements_file=DOCKER_TESTS_REQUIREMENTS) as py_exe:
        command_result = run_command(
            [py_exe, "-m", "pytest", str(test_path), *pytest_args, *extra_pytest_args],
            env=env,
            output=output,
            check=False,
        )
    return command_result.returncode, f"Testing {image_type} python {image_name}"


def run_docker_compose_tests(
    image_name: str,
    extra_pytest_args: tuple,
    skip_docker_compose_deletion: bool,
) -> tuple[int, str]:
    command_result = run_command(["docker", "inspect", image_name], check=False, stdout=DEVNULL)
    if command_result.returncode != 0:
        get_console().print(f"[error]Error when inspecting PROD image: {command_result.returncode}[/]")
        return command_result.returncode, f"Testing docker-compose python with {image_name}"
    pytest_args = ("--color=yes",)
    test_path = DOCKER_TESTS_ROOT / "test_docker_compose_quick_start.py"
    env = os.environ.copy()
    env["DOCKER_IMAGE"] = image_name
    if skip_docker_compose_deletion:
        env["SKIP_DOCKER_COMPOSE_DELETION"] = "true"
    with create_temp_venv(pip_version=PIP_VERSION, requirements_file=DOCKER_TESTS_REQUIREMENTS) as py_exe:
        command_result = run_command(
            [py_exe, "-m", "pytest", str(test_path), *pytest_args, *extra_pytest_args],
            env=env,
            check=False,
        )
    return command_result.returncode, f"Testing docker-compose python with {image_name}"


def file_name_from_test_type(test_type: str):
    test_type_no_brackets = test_type.lower().replace("[", "_").replace("]", "")
    return re.sub("[,.]", "_", test_type_no_brackets)[:30]


def test_paths(test_type: str, backend: str, helm_test_package: str | None) -> tuple[str, str, str]:
    file_friendly_test_type = file_name_from_test_type(test_type)
    extra_package = f"-{helm_test_package}" if helm_test_package else ""
    random_suffix = os.urandom(4).hex()
    result_log_file = f"/files/test_result-{file_friendly_test_type}{extra_package}-{backend}.xml"
    warnings_file = f"/files/warnings-{file_friendly_test_type}{extra_package}-{backend}.txt"
    coverage_file = f"/files/coverage-{file_friendly_test_type}-{backend}-{random_suffix}.xml"
    return result_log_file, warnings_file, coverage_file


def get_ignore_switches_for_provider(provider_folders: list[str]) -> list[str]:
    args = []
    for providers in provider_folders:
        args.extend(
            [
                f"--ignore=providers/tests/{providers}",
                f"--ignore=providers/tests/system/{providers}",
                f"--ignore=providers/tests/integration/{providers}",
            ]
        )
    return args


def get_suspended_provider_args() -> list[str]:
    suspended_folders = get_suspended_provider_folders()
    return get_ignore_switches_for_provider(suspended_folders)


def get_excluded_provider_args(python_version: str) -> list[str]:
    excluded_folders = get_excluded_provider_folders(python_version)
    return get_ignore_switches_for_provider(excluded_folders)


TEST_TYPE_MAP_TO_PYTEST_ARGS: dict[str, list[str]] = {
    "Always": ["tests/always"],
    "API": ["tests/api", "tests/api_connexion", "tests/api_internal", "tests/api_fastapi"],
    "BranchPythonVenv": [
        "tests/operators/test_python.py::TestBranchPythonVirtualenvOperator",
    ],
    "BranchExternalPython": [
        "tests/operators/test_python.py::TestBranchExternalPythonOperator",
    ],
    "CLI": ["tests/cli"],
    "Core": [
        "tests/core",
        "tests/executors",
        "tests/jobs",
        "tests/models",
        "tests/ti_deps",
        "tests/utils",
    ],
    "ExternalPython": [
        "tests/operators/test_python.py::TestExternalPythonOperator",
    ],
    "Integration": ["tests/integration"],
    # Operators test type excludes Virtualenv/External tests - they have their own test types
    "Operators": ["tests/operators", "--exclude-virtualenv-operator", "--exclude-external-python-operator"],
    # this one is mysteriously failing dill serialization. It could be removed once
    # https://github.com/pytest-dev/pytest/issues/10845 is fixed
    "PlainAsserts": [
        "tests/operators/test_python.py::TestPythonVirtualenvOperator::test_airflow_context",
        "--assert=plain",
    ],
    "Providers": ["providers/tests"],
    "PythonVenv": [
        "tests/operators/test_python.py::TestPythonVirtualenvOperator",
    ],
    "Serialization": [
        "tests/serialization",
    ],
    "System": ["tests/system"],
    "TaskSDK": ["task_sdk/tests"],
    "WWW": [
        "tests/www",
    ],
}

HELM_TESTS = "helm_tests"
INTEGRATION_TESTS = "tests/integration"
SYSTEM_TESTS = "tests/system"

# Those directories are already ignored vu pyproject.toml. We want to exclude them here as well.
NO_RECURSE_DIRS = [
    "tests/_internals",
    "tests/dags_with_system_exit",
    "tests/dags_corrupted",
    "tests/dags",
    "providers/tests/system/google/cloud/dataproc/resources",
    "providers/tests/system/google/cloud/gcs/resources",
]


def find_all_other_tests() -> list[str]:
    all_named_test_folders = list(chain.from_iterable(TEST_TYPE_MAP_TO_PYTEST_ARGS.values()))
    all_named_test_folders.append(HELM_TESTS)
    all_named_test_folders.append(INTEGRATION_TESTS)
    all_named_test_folders.append(SYSTEM_TESTS)
    all_named_test_folders.extend(NO_RECURSE_DIRS)

    all_curent_test_folders = [
        str(path.relative_to(AIRFLOW_SOURCES_ROOT))
        for path in AIRFLOW_SOURCES_ROOT.glob("tests/*")
        if path.is_dir() and path.name != "__pycache__"
    ]
    for named_test_folder in all_named_test_folders:
        if named_test_folder in all_curent_test_folders:
            all_curent_test_folders.remove(named_test_folder)
    return sorted(all_curent_test_folders)


PROVIDERS_LIST_PREFIX = "Providers["
PROVIDERS_LIST_EXCLUDE_PREFIX = "Providers[-"

ALL_TEST_SUITES: dict[str, tuple[str, ...]] = {
    "All": ("tests",),
    "All-Long": ("tests", "-m", "long_running", "--include-long-running"),
    "All-Quarantined": ("tests", "-m", "quarantined", "--include-quarantined"),
    "All-Postgres": ("tests", "--backend", "postgres"),
    "All-MySQL": ("tests", "--backend", "mysql"),
}


def convert_test_type_to_pytest_args(
    *,
    test_type: str,
    skip_provider_tests: bool,
    helm_test_package: str | None = None,
) -> list[str]:
    if test_type == "None":
        return []
    if test_type in ALL_TEST_SUITES:
        return [
            *ALL_TEST_SUITES[test_type],
        ]
    if test_type == "Helm":
        if helm_test_package and helm_test_package != "all":
            return [f"helm_tests/{helm_test_package}"]
        else:
            return [HELM_TESTS]
    if test_type == "Integration":
        if skip_provider_tests:
            return [
                "tests/integration/cli",
                "tests/integration/executors",
                "tests/integration/security",
            ]
        else:
            return [INTEGRATION_TESTS]
    if test_type == "System":
        return [SYSTEM_TESTS]
    if skip_provider_tests and test_type.startswith("Providers"):
        return []
    if test_type.startswith(PROVIDERS_LIST_EXCLUDE_PREFIX):
        excluded_provider_list = test_type[len(PROVIDERS_LIST_EXCLUDE_PREFIX) : -1].split(",")
        providers_with_exclusions = TEST_TYPE_MAP_TO_PYTEST_ARGS["Providers"].copy()
        for excluded_provider in excluded_provider_list:
            providers_with_exclusions.append(
                "--ignore=providers/tests/" + excluded_provider.replace(".", "/")
            )
        return providers_with_exclusions
    if test_type.startswith(PROVIDERS_LIST_PREFIX):
        provider_list = test_type[len(PROVIDERS_LIST_PREFIX) : -1].split(",")
        providers_to_test = []
        for provider in provider_list:
            provider_path = TESTS_PROVIDERS_ROOT.joinpath(provider.replace(".", "/"))
            if provider_path.is_dir():
                providers_to_test.append(provider_path.relative_to(AIRFLOW_SOURCES_ROOT).as_posix())
            else:
                get_console().print(
                    f"[error]Provider directory {provider_path} does not exist for {provider}. "
                    f"This is bad. Please add it (all providers should have a package in tests)"
                )
                sys.exit(1)
        return providers_to_test
    if test_type == "Other":
        return find_all_other_tests()
    test_dirs = TEST_TYPE_MAP_TO_PYTEST_ARGS.get(test_type)
    if test_dirs:
        return test_dirs.copy()
    get_console().print(f"[error]Unknown test type: {test_type}[/]")
    sys.exit(1)


def generate_args_for_pytest(
    *,
    test_type: str,
    test_timeout: int,
    skip_provider_tests: bool,
    skip_db_tests: bool,
    run_db_tests_only: bool,
    backend: str,
    use_xdist: bool,
    enable_coverage: bool,
    collect_only: bool,
    parallelism: int,
    parallel_test_types_list: list[str],
    python_version: str,
    helm_test_package: str | None,
    keep_env_variables: bool,
    no_db_cleanup: bool,
):
    result_log_file, warnings_file, coverage_file = test_paths(test_type, backend, helm_test_package)
    if skip_db_tests and parallel_test_types_list:
        args = convert_parallel_types_to_folders(
            parallel_test_types_list, skip_provider_tests, python_version=python_version
        )
    else:
        args = convert_test_type_to_pytest_args(
            test_type=test_type,
            skip_provider_tests=skip_provider_tests,
            helm_test_package=helm_test_package,
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
            f"-p {COMMON_TESTS_PYTEST_PLUGIN}",
        ]
    )
    if skip_db_tests:
        args.append("--skip-db-tests")
    if run_db_tests_only:
        args.append("--run-db-tests-only")
    if test_type != "System":
        args.append(f"--ignore-glob=*/{SYSTEM_TESTS}")
    if test_type != "Integration":
        args.append(f"--ignore-glob=*/{INTEGRATION_TESTS}")
    if test_type != "Helm":
        # do not produce warnings output for helm tests
        args.append(f"--warning-output-path={warnings_file}")
        args.append(f"--ignore={HELM_TESTS}")
    if test_type not in ("Helm", "System"):
        args.append("--with-db-init")
    args.extend(get_suspended_provider_args())
    args.extend(get_excluded_provider_args(python_version))
    if use_xdist:
        args.extend(["-n", str(parallelism) if parallelism else "auto"])
    # We have to disable coverage for Python 3.12 because of the issue with coverage that takes too long, despite
    # Using experimental support for Python 3.12 PEP 669. The coverage.py is not yet fully compatible with the
    # full scope of PEP-669. That will be fully done when https://github.com/nedbat/coveragepy/issues/1746 is
    # resolve for now we are disabling coverage for Python 3.12, and it causes slower execution and occasional
    # timeouts
    if enable_coverage and python_version != "3.12":
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


def convert_parallel_types_to_folders(
    parallel_test_types_list: list[str], skip_provider_tests: bool, python_version: str
):
    args = []
    for _test_type in parallel_test_types_list:
        args.extend(
            convert_test_type_to_pytest_args(
                test_type=_test_type,
                skip_provider_tests=skip_provider_tests,
                helm_test_package=None,
            )
        )
    # leave only folders, strip --pytest-args that exclude some folders with `-' prefix
    folders = [arg for arg in args if arg.startswith("test") or arg.startswith("providers/tests")]
    # remove specific provider sub-folders if "providers/tests" is already in the list
    # This workarounds pytest issues where it will only run tests from specific subfolders
    # if both parent and child folders are in the list
    # The issue in Pytest (changed behaviour in Pytest 8.2 is tracked here
    # https://github.com/pytest-dev/pytest/issues/12605
    if "providers/tests" in folders:
        folders = [folder for folder in folders if not folder.startswith("providers/tests/")]
    return folders
