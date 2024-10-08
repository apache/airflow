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

import re
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
        shell_params=ShellParams(test_type="Providers"),
        extra_pytest_args=(f"--ignore=tests/providers/{fake_provider_name}",),
        python_version="3.9",
        output=None,
        test_timeout=60,
        skip_docker_compose_down=True,
    )

    # the docker run command is the second call to run_command; the command arg list is the first
    # positional arg of the command call
    run_cmd_call = mock_run_command.call_args_list[1]
    arg_str = " ".join(run_cmd_call.args[0])

    # The command pattern we look for is "<container id> <tests directory arg> \
    # <*other args we don't care about*> --ignore tests/providers/<provider name> \
    # --ignore tests/system/providers/<provider name> --ignore tests/integration/providers/<provider name>"
    # (the container id is simply to anchor the pattern so we know where we are starting; _run_tests should
    # be refactored to make arg testing easier but until then we have to regex-test the entire command string
    match_pattern = re.compile(
        f" airflow tests/providers .+ --ignore=tests/providers/{fake_provider_name} --ignore=tests/system/providers/{fake_provider_name} --ignore=tests/integration/providers/{fake_provider_name}"
    )

    assert match_pattern.search(arg_str)


def test_primary_test_arg_is_excluded_by_extra_pytest_arg(mock_run_command):
    test_provider = "http"  # "Providers[<id>]" scans the source tree so we need to use a real provider id
    test_provider_not_skipped = "ftp"
    _run_test(
        shell_params=ShellParams(test_type=f"Providers[{test_provider},{test_provider_not_skipped}]"),
        extra_pytest_args=(f"--ignore=tests/providers/{test_provider}",),
        python_version="3.9",
        output=None,
        test_timeout=60,
        skip_docker_compose_down=True,
    )

    assert mock_run_command.call_count > 1
    run_cmd_call = mock_run_command.call_args_list[1]
    arg_str = " ".join(run_cmd_call.args[0])

    # The command pattern we look for is "<container id> --verbosity=0 \
    # <*other args we don't care about*> --ignore=tests/providers/<provider name>"
    # The tests/providers/http argument has been eliminated by the code that preps the args; this is a bug,
    # bc without a directory or module arg, pytest tests everything (which we don't want!)
    # We check "--verbosity=0" to ensure nothing is between the airflow container id and the verbosity arg,
    # IOW that the primary test arg is removed
    match_pattern = re.compile(
        f"airflow tests/providers/{test_provider_not_skipped} --verbosity=0 .+ --ignore=tests/providers/{test_provider}"
    )

    assert match_pattern.search(arg_str)


def test_test_is_skipped_if_all_are_ignored(mock_run_command):
    test_providers = [
        "http",
        "ftp",
    ]  # "Providers[<id>]" scans the source tree so we need to use a real provider id
    _run_test(
        shell_params=ShellParams(test_type=f"Providers[{','.join(test_providers)}]"),
        extra_pytest_args=[f"--ignore=tests/providers/{provider}" for provider in test_providers],
        python_version="3.9",
        output=None,
        test_timeout=60,
        skip_docker_compose_down=True,
    )

    mock_run_command.assert_called_once()  # called only to compose down
