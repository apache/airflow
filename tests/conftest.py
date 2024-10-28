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

import logging
import os
import sys
from typing import TYPE_CHECKING

import pytest

from tests_common.test_utils.log_handlers import non_pytest_handlers

# We should set these before loading _any_ of the rest of airflow so that the
# unit test mode config is set as early as possible.
assert (
    "airflow" not in sys.modules
), "No airflow module can be imported before these lines"

pytest_plugins = "tests_common.pytest_plugin"

# Ignore files that are really test dags to be ignored by pytest
collect_ignore = [
    "tests/dags/subdir1/test_ignore_this.py",
    "tests/dags/test_invalid_dup_task.py",
    "tests/dags_corrupted/test_impersonation_custom.py",
    "tests_common.test_utils/perf/dags/elastic_dag.py",
]


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config) -> None:
    dep_path = [config.rootpath.joinpath("tests", "deprecations_ignore.yml")]
    config.inicfg["airflow_deprecations_ignore"] = (
        config.inicfg.get("airflow_deprecations_ignore", []) + dep_path  # type: ignore[assignment,operator]
    )


@pytest.fixture
def reset_environment():
    """Resets env variables."""
    init_env = os.environ.copy()
    yield
    changed_env = os.environ
    for key in changed_env:
        if key not in init_env:
            del os.environ[key]
        else:
            os.environ[key] = init_env[key]


def remove_non_pytest_log_handlers(logger, *classes):
    for handler in non_pytest_handlers(logger.handlers):
        logger.removeHandler(handler)


def remove_all_non_pytest_log_handlers():
    # Remove handlers from the root logger
    remove_non_pytest_log_handlers(logging.getLogger())
    # Remove handlers from all other loggers
    for logger_name in logging.root.manager.loggerDict:
        remove_non_pytest_log_handlers(logging.getLogger(logger_name))


@pytest.fixture
def clear_all_logger_handlers():
    remove_all_non_pytest_log_handlers()
    yield
    remove_all_non_pytest_log_handlers()


if TYPE_CHECKING:
    # Static checkers do not know about pytest fixtures' types and return,
    # In case if them distributed through third party packages.
    # This hack should help with autosuggestion in IDEs.
    from pytest_mock import MockerFixture
    from requests_mock.contrib.fixture import Fixture as RequestsMockFixture
    from time_machine import TimeMachineFixture

    # pytest-mock
    @pytest.fixture
    def mocker() -> MockerFixture:
        """Function scoped mocker."""

    @pytest.fixture(scope="class")
    def class_mocker() -> MockerFixture:
        """Class scoped mocker."""

    @pytest.fixture(scope="module")
    def module_mocker() -> MockerFixture:
        """Module scoped mocker."""

    @pytest.fixture(scope="package")
    def package_mocker() -> MockerFixture:
        """Package scoped mocker."""

    @pytest.fixture(scope="session")
    def session_mocker() -> MockerFixture:
        """Session scoped mocker."""

    # requests-mock
    @pytest.fixture
    def requests_mock() -> RequestsMockFixture: ...

    # time-machine
    @pytest.fixture  # type: ignore[no-redef]
    def time_machine() -> TimeMachineFixture: ...
