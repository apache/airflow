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

from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.suspended_providers import get_suspended_providers_folders


def verify_an_image(
    image_name: str,
    image_type: str,
    output: Output | None,
    slim_image: bool,
    extra_pytest_args: tuple,
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
        test_path = AIRFLOW_SOURCES_ROOT / "docker_tests" / "test_prod_image.py"
    else:
        test_path = AIRFLOW_SOURCES_ROOT / "docker_tests" / "test_ci_image.py"
    env = os.environ.copy()
    env["DOCKER_IMAGE"] = image_name
    if slim_image:
        env["TEST_SLIM_IMAGE"] = "true"
    command_result = run_command(
        [sys.executable, "-m", "pytest", str(test_path), *pytest_args, *extra_pytest_args],
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
    test_path = AIRFLOW_SOURCES_ROOT / "docker_tests" / "test_docker_compose_quick_start.py"
    env = os.environ.copy()
    env["DOCKER_IMAGE"] = image_name
    if skip_docker_compose_deletion:
        env["SKIP_DOCKER_COMPOSE_DELETION"] = "true"
    command_result = run_command(
        [sys.executable, "-m", "pytest", str(test_path), *pytest_args, *extra_pytest_args],
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


def get_suspended_provider_args() -> list[str]:
    pytest_args = []
    suspended_folders = get_suspended_providers_folders()
    for providers in suspended_folders:
        pytest_args.extend(
            [
                "--ignore",
                f"tests/providers/{providers}",
                "--ignore",
                f"tests/system/providers/{providers}",
                "--ignore",
                f"tests/integration/providers/{providers}",
            ]
        )
    return pytest_args


TEST_TYPE_MAP_TO_PYTEST_ARGS: dict[str, list[str]] = {
    "Always": ["tests/always"],
    "API": ["tests/api", "tests/api_experimental", "tests/api_connexion", "tests/api_internal"],
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
    "Providers": ["tests/providers"],
    "PythonVenv": [
        "tests/operators/test_python.py::TestPythonVirtualenvOperator",
    ],
    "Serialization": [
        "tests/serialization",
    ],
    "System": ["tests/system"],
    "WWW": [
        "tests/www",
    ],
}

HELM_TESTS = "helm_tests"
INTEGRATION_TESTS = "tests/integration"
SYSTEM_TESTS = "tests/system"

# Those directories are already ignored vu pyproject.toml. We want to exclude them here as well.
NO_RECURSE_DIRS = [
    "tests/dags_with_system_exit",
    "tests/test_utils",
    "tests/dags_corrupted",
    "tests/dags",
    "tests/system/providers/google/cloud/dataproc/resources",
    "tests/system/providers/google/cloud/gcs/resources",
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
                "tests/integration/api_experimental",
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
                "--ignore=tests/providers/" + excluded_provider.replace(".", "/")
            )
        return providers_with_exclusions
    if test_type.startswith(PROVIDERS_LIST_PREFIX):
        provider_list = test_type[len(PROVIDERS_LIST_PREFIX) : -1].split(",")
        providers_to_test = []
        for provider in provider_list:
            provider_path = "tests/providers/" + provider.replace(".", "/")
            if (AIRFLOW_SOURCES_ROOT / provider_path).is_dir():
                providers_to_test.append(provider_path)
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
        return test_dirs
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
    helm_test_package: str | None,
):
    result_log_file, warnings_file, coverage_file = test_paths(test_type, backend, helm_test_package)
    if skip_db_tests:
        if parallel_test_types_list:
            args = convert_parallel_types_to_folders(parallel_test_types_list, skip_provider_tests)
        else:
            args = ["tests"] if test_type != "None" else []
    else:
        args = convert_test_type_to_pytest_args(
            test_type=test_type, skip_provider_tests=skip_provider_tests, helm_test_package=helm_test_package
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
            "--timeouts-order",
            "moi",
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
    if test_type != "System":
        args.append(f"--ignore={SYSTEM_TESTS}")
    if test_type != "Integration":
        args.append(f"--ignore={INTEGRATION_TESTS}")
    if test_type != "Helm":
        # do not produce warnings output for helm tests
        args.append(f"--warning-output-path={warnings_file}")
        args.append(f"--ignore={HELM_TESTS}")
    if test_type not in ("Helm", "System"):
        args.append("--with-db-init")
    args.extend(get_suspended_provider_args())
    if use_xdist:
        args.extend(["-n", str(parallelism) if parallelism else "auto"])
    if enable_coverage:
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
    return args


def convert_parallel_types_to_folders(parallel_test_types_list: list[str], skip_provider_tests: bool):
    args = []
    for _test_type in parallel_test_types_list:
        args.extend(
            convert_test_type_to_pytest_args(
                test_type=_test_type, skip_provider_tests=skip_provider_tests, helm_test_package=None
            )
        )
    # leave only folders, strip --pytest-args
    return [arg for arg in args if arg.startswith("test")]
