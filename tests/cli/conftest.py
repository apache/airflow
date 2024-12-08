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

from airflow.executors import local_executor
from airflow.models.dagbag import DagBag
from airflow.providers.celery.executors import celery_executor, celery_kubernetes_executor
from airflow.providers.cncf.kubernetes.executors import kubernetes_executor, local_kubernetes_executor

from tests_common.test_utils.config import conf_vars

# Create custom executors here because conftest is imported first
custom_executor_module = type(sys)("custom_executor")
custom_executor_module.CustomCeleryExecutor = type(  # type:  ignore
    "CustomCeleryExecutor", (celery_executor.CeleryExecutor,), {}
)
custom_executor_module.CustomCeleryKubernetesExecutor = type(  # type: ignore
    "CustomCeleryKubernetesExecutor", (celery_kubernetes_executor.CeleryKubernetesExecutor,), {}
)
custom_executor_module.CustomLocalExecutor = type(  # type:  ignore
    "CustomLocalExecutor", (local_executor.LocalExecutor,), {}
)
custom_executor_module.CustomLocalKubernetesExecutor = type(  # type: ignore
    "CustomLocalKubernetesExecutor", (local_kubernetes_executor.LocalKubernetesExecutor,), {}
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
