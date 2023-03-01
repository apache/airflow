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

from contextlib import nullcontext
from unittest import mock

import pytest

from airflow import plugins_manager
from airflow.exceptions import AirflowConfigException
from airflow.executors.executor_loader import ConnectorSource, ExecutorLoader
from tests.test_utils.config import conf_vars

# Plugin Manager creates new modules, which is difficult to mock, so we use test isolation by a unique name.
TEST_PLUGIN_NAME = "unique_plugin_name_to_avoid_collision_i_love_kitties"


class FakeExecutor:
    is_single_threaded = False


class FakeSingleThreadedExecutor:
    is_single_threaded = True


class FakePlugin(plugins_manager.AirflowPlugin):
    name = TEST_PLUGIN_NAME
    executors = [FakeExecutor]


class TestExecutorLoader:
    def setup_method(self) -> None:
        ExecutorLoader._default_executor = None

    def teardown_method(self) -> None:
        ExecutorLoader._default_executor = None

    @pytest.mark.parametrize(
        "executor_name",
        [
            "CeleryExecutor",
            "CeleryKubernetesExecutor",
            "DebugExecutor",
            "KubernetesExecutor",
            "LocalExecutor",
        ],
    )
    def test_should_support_executor_from_core(self, executor_name):
        with conf_vars({("core", "executor"): executor_name}):
            executor = ExecutorLoader.get_default_executor()
            assert executor is not None
            assert executor_name == executor.__class__.__name__

    @mock.patch("airflow.plugins_manager.plugins", [FakePlugin()])
    @mock.patch("airflow.plugins_manager.executors_modules", None)
    def test_should_support_plugins(self):
        with conf_vars({("core", "executor"): f"{TEST_PLUGIN_NAME}.FakeExecutor"}):
            executor = ExecutorLoader.get_default_executor()
            assert executor is not None
            assert "FakeExecutor" == executor.__class__.__name__

    def test_should_support_custom_path(self):
        with conf_vars({("core", "executor"): "tests.executors.test_executor_loader.FakeExecutor"}):
            executor = ExecutorLoader.get_default_executor()
            assert executor is not None
            assert "FakeExecutor" == executor.__class__.__name__

    @pytest.mark.parametrize(
        "executor_name",
        [
            "CeleryExecutor",
            "CeleryKubernetesExecutor",
            "DebugExecutor",
            "KubernetesExecutor",
            "LocalExecutor",
        ],
    )
    def test_should_support_import_executor_from_core(self, executor_name):
        with conf_vars({("core", "executor"): executor_name}):
            executor, import_source = ExecutorLoader.import_default_executor_cls()
            assert executor_name == executor.__name__
            assert import_source == ConnectorSource.CORE

    @mock.patch("airflow.plugins_manager.plugins", [FakePlugin()])
    @mock.patch("airflow.plugins_manager.executors_modules", None)
    def test_should_support_import_plugins(self):
        with conf_vars({("core", "executor"): f"{TEST_PLUGIN_NAME}.FakeExecutor"}):
            executor, import_source = ExecutorLoader.import_default_executor_cls()
            assert "FakeExecutor" == executor.__name__
            assert import_source == ConnectorSource.PLUGIN

    def test_should_support_import_custom_path(self):
        with conf_vars({("core", "executor"): "tests.executors.test_executor_loader.FakeExecutor"}):
            executor, import_source = ExecutorLoader.import_default_executor_cls()
            assert "FakeExecutor" == executor.__name__
            assert import_source == ConnectorSource.CUSTOM_PATH

    @pytest.mark.backend("mssql", "mysql", "postgres")
    @pytest.mark.parametrize("executor", [FakeExecutor, FakeSingleThreadedExecutor])
    def test_validate_database_executor_compatibility_general(self, monkeypatch, executor):
        monkeypatch.delenv("_AIRFLOW__SKIP_DATABASE_EXECUTOR_COMPATIBILITY_CHECK")
        ExecutorLoader.validate_database_executor_compatibility(executor)

    @pytest.mark.backend("sqlite")
    @pytest.mark.parametrize(
        ["executor", "expectation"],
        [
            (FakeSingleThreadedExecutor, nullcontext()),
            (
                FakeExecutor,
                pytest.raises(AirflowConfigException, match=r"^error: cannot use SQLite with the .+"),
            ),
        ],
    )
    def test_validate_database_executor_compatibility_sqlite(self, monkeypatch, executor, expectation):
        monkeypatch.delenv("_AIRFLOW__SKIP_DATABASE_EXECUTOR_COMPATIBILITY_CHECK")
        with expectation:
            ExecutorLoader.validate_database_executor_compatibility(executor)
