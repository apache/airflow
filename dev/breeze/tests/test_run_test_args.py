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
from airflow_breeze.global_constants import GroupOfTests
from airflow_breeze.params.shell_params import ShellParams


@pytest.fixture(autouse=True)
def mock_run_command():
    """We mock run_command to capture its call args; it returns nothing so mock training is unnecessary."""
    with patch("airflow_breeze.commands.testing_commands.run_command") as mck:
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
def _mock_sleep():
    """_run_test does a 10-second sleep in CI, so we mock the sleep function to save CI test time."""
    with patch("airflow_breeze.commands.testing_commands.sleep"):
        yield


@pytest.fixture(autouse=True)
def mock_remove_docker_networks():
    """We mock remove_docker_networks to avoid making actual docker calls during these tests;
    it returns nothing so mock training is unnecessary."""
    with patch("airflow_breeze.commands.testing_commands.remove_docker_networks") as mck:
        yield mck


@pytest.mark.parametrize(
    "mock_fixture",
    ["mock_get_suspended_provider_folders", "mock_get_excluded_provider_folders"],
    ids=["suspended provider", "excluded provider"],
)
def test_irregular_provider_with_extra_ignore_should_be_valid_cmd(mock_run_command, mock_fixture, request):
    """When a provider is suspended or excluded, we expect "--ignore" switches to wind up in the cmd args"""
    fake_provider_name = "unittestingprovider"

    mock_to_train = request.getfixturevalue(mock_fixture)
    mock_to_train.return_value = [fake_provider_name]

    _run_test(
        shell_params=ShellParams(test_group=GroupOfTests.PROVIDERS, test_type="Providers"),
        extra_pytest_args=(),
        python_version="3.9",
        output=None,
        test_timeout=60,
        skip_docker_compose_down=True,
    )

    # the docker run command is the second call to run_command; the command arg list is the first
    # positional arg of the command call
    run_cmd_call = mock_run_command.call_args_list[1]
    arg_str = " ".join(run_cmd_call.args[0])
    assert f"--ignore=providers/{fake_provider_name}/tests " in arg_str
    assert f"providers/{fake_provider_name}/tests" not in arg_str.split(" ")


def test_test_is_skipped_if_all_are_ignored(mock_run_command):
    test_providers = [
        "http",
        "standard",
    ]  # "Providers[<id>]" scans the source tree so we need to use a real provider id
    _run_test(
        shell_params=ShellParams(
            test_group=GroupOfTests.PROVIDERS, test_type=f"Providers[{','.join(test_providers)}]"
        ),
        extra_pytest_args=tuple(f"--ignore=providers/{provider}/tests" for provider in test_providers),
        python_version="3.9",
        output=None,
        test_timeout=60,
        skip_docker_compose_down=True,
    )

    mock_run_command.assert_called_once()  # called only to compose down


def test_test_is_skipped_when_all_providers_excluded_for_python_version(
    mock_run_command, mock_get_excluded_provider_folders
):
    """When all providers in the test type are excluded for the Python version, skip without Docker calls."""
    mock_get_excluded_provider_folders.return_value = ["http"]
    return_code, message = _run_test(
        shell_params=ShellParams(test_group=GroupOfTests.PROVIDERS, test_type="Providers[http]"),
        extra_pytest_args=(),
        python_version="3.14",
        output=None,
        test_timeout=60,
        skip_docker_compose_down=True,
    )
    assert return_code == 0
    assert "Skipped" in message
    mock_run_command.assert_not_called()


def test_test_is_not_skipped_when_some_providers_remain(mock_run_command, mock_get_excluded_provider_folders):
    """When only some providers are excluded, the test should still run."""
    mock_get_excluded_provider_folders.return_value = ["http"]
    _run_test(
        shell_params=ShellParams(test_group=GroupOfTests.PROVIDERS, test_type="Providers[http,standard]"),
        extra_pytest_args=(),
        python_version="3.14",
        output=None,
        test_timeout=60,
        skip_docker_compose_down=True,
    )
    assert mock_run_command.call_count >= 2  # compose down + compose run


def test_none_test_type_with_extra_args_does_not_skip(mock_run_command):
    """test_type=None with user-provided test paths via extra_pytest_args must not skip."""
    _run_test(
        shell_params=ShellParams(test_group=GroupOfTests.CORE, test_type="None"),
        extra_pytest_args=("airflow-core/tests/unit/serialization/test_helpers.py",),
        python_version="3.14",
        output=None,
        test_timeout=60,
        skip_docker_compose_down=True,
    )
    assert mock_run_command.call_count >= 2  # compose down + compose run
