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

import json
import logging
import os
import sys
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING
from unittest import mock

import pytest

from tests_common.test_utils.log_handlers import non_pytest_handlers

# We should set these before loading _any_ of the rest of airflow so that the
# unit test mode config is set as early as possible.
assert "airflow" not in sys.modules, "No airflow module can be imported before these lines"

pytest_plugins = "tests_common.pytest_plugin"

# Ignore files that are really test dags to be ignored by pytest
collect_ignore = [
    "tests/unit/dags/subdir1/test_ignore_this.py",
    "tests/unit/dags/test_invalid_dup_task.py",
    "tests/unit/dags_corrupted/test_impersonation_custom.py",
    "tests_common/test_utils/perf/dags/elastic_dag.py",
]


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


@contextmanager
def _config_bundles(bundles: dict[str, Path | str]):
    from tests_common.test_utils.config import conf_vars

    bundle_config = []
    for name, path in bundles.items():
        bundle_config.append(
            {
                "name": name,
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(path), "refresh_interval": 0},
            }
        )
    with conf_vars({("dag_processor", "dag_bundle_config_list"): json.dumps(bundle_config)}):
        yield


@pytest.fixture
def configure_dag_bundles():
    """Configure arbitrary DAG bundles with the provided paths"""
    return _config_bundles


@pytest.fixture
def configure_testing_dag_bundle():
    """Configure a "testing" DAG bundle with the provided path"""

    @contextmanager
    def _config_bundle(path_to_parse: Path | str):
        with _config_bundles({"testing": path_to_parse}):
            yield

    return _config_bundle


@pytest.fixture
def test_zip_path(tmp_path: Path):
    TEST_DAGS_FOLDER = Path(__file__).parent / "unit" / "dags"
    test_zip_folder = TEST_DAGS_FOLDER / "test_zip"
    zipped = tmp_path / "test_zip.zip"
    with zipfile.ZipFile(zipped, "w") as zf:
        for root, _, files in os.walk(test_zip_folder):
            for file in files:
                zf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), test_zip_folder))

    return os.fspath(zipped)


@pytest.fixture
def mock_supervisor_comms():
    with mock.patch(
        "airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS", create=True
    ) as supervisor_comms:
        yield supervisor_comms


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
