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

import pytest

from airflow.dag_processing.dagbag import DagBag
from airflow.executors import local_executor
from airflow.providers.celery.executors import celery_executor
from airflow.providers.cncf.kubernetes.executors import kubernetes_executor

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.stream_capture_manager import (
    CombinedCaptureManager,
    StderrCaptureManager,
    StdoutCaptureManager,
    StreamCaptureManager,
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
    return DagBag(include_examples=True)


@pytest.fixture(scope="session")
def parser():
    from airflow.cli import cli_parser

    return cli_parser.get_parser()


# The "*_capture" fixtures all ensure that the `caplog` fixture is loaded so that they dont get polluted with
# log messages


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


@pytest.fixture
def stream_capture(request):
    """Fixture that returns a configurable stream capture manager."""

    def _capture(stdout=True, stderr=False):
        request.getfixturevalue("caplog")
        return StreamCaptureManager(capture_stdout=stdout, capture_stderr=stderr)

    return _capture


@pytest.fixture
def combined_capture(request):
    """Fixture that captures both stdout and stderr."""
    request.getfixturevalue("caplog")
    return CombinedCaptureManager()
