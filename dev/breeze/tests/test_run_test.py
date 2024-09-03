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

from unittest.mock import patch

import pytest

from airflow_breeze.commands.testing_commands import _run_test
from airflow_breeze.params.shell_params import ShellParams


@pytest.fixture(autouse=True)
def mock_run_command():
    """We mock run_command to capture its call args; it returns nothing so mock training is unnecessary."""
    with patch("airflow_breeze.commands.testing_commands.run_command") as mck:
        yield mck


@pytest.fixture(autouse=True)
def mock_generate_args_for_pytest():
    with patch("airflow_breeze.commands.testing_commands.generate_args_for_pytest") as mck:
        yield mck


@pytest.fixture(autouse=True)
def mock_get_suspended_provider_folders():
    with patch("airflow_breeze.utils.run_tests.get_suspended_provider_folders") as mck:
        mck.return_value = []
        yield mck


@pytest.fixture(autouse=True)
def mock_get_excluded_provider_folders():
    with patch("airflow_breeze.utils.run_tests.get_excluded_provider_folders") as mck:
        mck.return_value = []
        yield mck


@pytest.fixture(autouse=True)
def mock_sleep():
    """_run_test does a 10-second sleep in CI, so we mock the sleep function to save CI test time."""
    with patch("airflow_breeze.commands.testing_commands.sleep"):
        yield


@pytest.fixture(autouse=True)
def mock_remove_docker_networks():
    """We mock remove_docker_networks to avoid making actual docker calls during these tests;
    it returns nothing so mock training is unnecessary."""
    with patch("airflow_breeze.commands.testing_commands.remove_docker_networks") as mck:
        yield mck


def test_exits_on_bad_test_type():
    """Verify the quick bail-out if the test type doesn't make sense."""
    with pytest.raises(SystemExit) as se:
        _run_test(
            shell_params=ShellParams(test_type="[bogus]TestType"),
            extra_pytest_args=(),
            python_version="3.8",
            output=None,
            test_timeout=60,
            skip_docker_compose_down=True,
        )
    print(se.value.code)


def test_calls_docker_down(mock_run_command):
    """Verify docker down is called with expected arguments."""
    _run_test(
        shell_params=ShellParams(test_type="Core"),
        extra_pytest_args=(),
        python_version="3.8",
        output=None,
        test_timeout=60,
        skip_docker_compose_down=True,
    )

    docker_down_call_args = mock_run_command.call_args_list[0].args[0]

    assert docker_down_call_args == [
        "docker",
        "compose",
        "--project-name",
        "airflow-test-core",
        "down",
        "--remove-orphans",
        "--volumes",
    ]


def test_calls_docker_run_with_expected_args(mock_run_command, mock_generate_args_for_pytest):
    """This test verifies that 'docker run' is called with arguments concatenated from:
    (a) the docker compose command
    (b) generate_args_for_pytest(), and
    (c) the extra pytest args

    We mock generate_args_for_pytest() since it calls out to another module.
    """
    test_mocked_pytest_args = ["pytest_arg_0", "pytest_arg_1"]
    test_extra_pytest_args = ("extra_1", "extra_2")
    mock_generate_args_for_pytest.return_value = test_mocked_pytest_args

    _run_test(
        shell_params=ShellParams(test_type="Core"),
        extra_pytest_args=test_extra_pytest_args,
        python_version="3.8",
        output=None,
        test_timeout=60,
        skip_docker_compose_down=True,
    )

    docker_run_call_args = mock_run_command.call_args_list[1].args[0]

    assert docker_run_call_args == [
        # docker command args:
        "docker",
        "compose",
        "--project-name",
        "airflow-test-core",
        "run",
        "-T",
        "--service-ports",
        "--rm",
        "airflow",
        # test_mocked_pytest_args:
        *test_mocked_pytest_args,
        # extra_pytest_args:
        *test_extra_pytest_args,
    ]


def test_calls_docker_run_with_ignored_directory_removed(mock_run_command, mock_generate_args_for_pytest):
    test_mocked_pytest_args = ["tests/providers/alpha", "tests/providers/beta"]
    test_extra_pytest_args = ("--ignore=tests/providers/alpha", "--verbose")
    mock_generate_args_for_pytest.return_value = test_mocked_pytest_args

    _run_test(
        shell_params=ShellParams(test_type="Core"),
        extra_pytest_args=test_extra_pytest_args,
        python_version="3.8",
        output=None,
        test_timeout=60,
        skip_docker_compose_down=True,
    )

    docker_run_call_args = mock_run_command.call_args_list[1].args[0]

    assert docker_run_call_args == [
        # docker command args:
        "docker",
        "compose",
        "--project-name",
        "airflow-test-core",
        "run",
        "-T",
        "--service-ports",
        "--rm",
        "airflow",
        # test_mocked_pytest_args, minus the "tests/providers/alpha" element:
        "tests/providers/beta",
        # extra_pytest_args:
        *test_extra_pytest_args,
    ]


def test_skips_docker_commands_when_all_test_directories_removed(
    mock_run_command, mock_generate_args_for_pytest
):
    test_mocked_pytest_args = ["tests/providers/alpha"]
    test_extra_pytest_args = ("--ignore=tests/providers/alpha",)
    mock_generate_args_for_pytest.return_value = test_mocked_pytest_args

    result = _run_test(
        shell_params=ShellParams(test_type="Providers[alpha]"),
        extra_pytest_args=test_extra_pytest_args,
        python_version="3.8",
        output=None,
        test_timeout=60,
        skip_docker_compose_down=True,
    )

    mock_run_command.assert_not_called()
    assert result == (0, "Test skipped: Providers[alpha]")
