#
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

import sys
from unittest import mock

import pytest

from airflow.dag_processing.dagbag import DagBag
from airflow.executors import local_executor
from airflow.providers.celery.executors import celery_executor
from airflow.providers.cncf.kubernetes.executors import kubernetes_executor

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.stream_capture_manager import (
    StderrCaptureManager,
    StdoutCaptureManager,
)

# Create custom executors here because conftest is imported first
custom_executor_module = type(sys)("custom_executor")
custom_executor_module.CustomCeleryExecutor = type(  # type:  ignore
    "CustomCeleryExecutor", (celery_executor.CeleryExecutor,), {}
)
custom_executor_module.CustomLocalExecutor = type(  # type:  ignore
    "CustomLocalExecutor", (local_executor.LocalExecutor,), {}
)
custom_executor_module.CustomKubernetesExecutor = type(  # type:  ignore
    "CustomKubernetesExecutor", (kubernetes_executor.KubernetesExecutor,), {}
)
sys.modules["custom_executor"] = custom_executor_module


@pytest.fixture(autouse=True)
def load_examples():
    with conf_vars({("core", "load_examples"): "True"}):
        yield


@pytest.fixture(scope="session")
def dagbag():
    return DagBag()


@pytest.fixture(scope="session")
def parser():
    from airflow.cli import cli_parser

    return cli_parser.get_parser()


# The "*_capture" fixtures all ensure that the `caplog` fixture is loaded so that they don't get polluted with
# log messages


@pytest.fixture
def mock_cli_api_client():
    """Mock the CLI airflowctl client and neutralize ``action_cli``'s DB touch points.

    CLI commands that go through the airflowctl client only need the mocked client; the
    ``@action_cli`` audit logging and log-template sync would otherwise open a database
    session. Patching them lets these command tests run without a database or API server.
    """
    client = mock.MagicMock()
    with (
        mock.patch("airflow.cli.api_client.get_cli_api_client", return_value=client),
        mock.patch("airflow.utils.cli_action_loggers.on_pre_execution"),
        mock.patch("airflow.utils.cli_action_loggers.on_post_execution"),
        mock.patch("airflow.utils.db.synchronize_log_template"),
        mock.patch("airflow.utils.db.check_and_run_migrations"),
    ):
        yield client


@pytest.fixture
def stdout_capture(request):
    """Fixture that captures stdout only."""
    request.getfixturevalue("caplog")
    return StdoutCaptureManager()


@pytest.fixture
def stderr_capture(request):
    """Fixture that captures stderr only."""
    request.getfixturevalue("caplog")
    return StderrCaptureManager()
