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
from importlib import reload
from unittest import mock
from unittest.mock import patch

import pytest

from airflow import plugins_manager
from airflow.exceptions import AirflowConfigException
from airflow.executors import executor_loader
from airflow.executors.executor_loader import ConnectorSource, ExecutorLoader, ExecutorName
from airflow.executors.local_executor import LocalExecutor
from airflow.providers.celery.executors.celery_executor import CeleryExecutor
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
        from airflow.executors import executor_loader

        reload(executor_loader)
        global ExecutorLoader
        ExecutorLoader = executor_loader.ExecutorLoader  # type: ignore

    def teardown_method(self) -> None:
        from airflow.executors import executor_loader

        reload(executor_loader)
        ExecutorLoader.init_executors()

    def test_no_executor_configured(self):
        with conf_vars({("core", "executor"): None}):
            with pytest.raises(AirflowConfigException, match=r".*not found in config$"):
                ExecutorLoader.get_default_executor()

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
            assert executor.name is not None
            assert executor.name == ExecutorName(ExecutorLoader.executors[executor_name], alias=executor_name)
            assert executor.name.connector_source == ConnectorSource.CORE

    @mock.patch("airflow.plugins_manager.plugins", [FakePlugin()])
    @mock.patch("airflow.plugins_manager.executors_modules", None)
    def test_should_support_plugins(self):
        with conf_vars({("core", "executor"): f"{TEST_PLUGIN_NAME}.FakeExecutor"}):
            executor = ExecutorLoader.get_default_executor()
            assert executor is not None
            assert "FakeExecutor" == executor.__class__.__name__
            assert executor.name is not None
            assert executor.name == ExecutorName(f"{TEST_PLUGIN_NAME}.FakeExecutor")
            assert executor.name.connector_source == ConnectorSource.PLUGIN

    def test_should_support_custom_path(self):
        with conf_vars({("core", "executor"): "tests.executors.test_executor_loader.FakeExecutor"}):
            executor = ExecutorLoader.get_default_executor()
            assert executor is not None
            assert "FakeExecutor" == executor.__class__.__name__
            assert executor.name is not None
            assert executor.name == ExecutorName("tests.executors.test_executor_loader.FakeExecutor")
            assert executor.name.connector_source == ConnectorSource.CUSTOM_PATH

    @pytest.mark.parametrize(
        ("executor_config", "expected_executors_list"),
        [
            # Just one executor
            (
                "CeleryExecutor",
                [
                    ExecutorName(
                        "airflow.providers.celery.executors.celery_executor.CeleryExecutor",
                        "CeleryExecutor",
                    ),
                ],
            ),
            # Core executors and custom module path executor and plugin
            (
                f"CeleryExecutor, LocalExecutor, tests.executors.test_executor_loader.FakeExecutor, {TEST_PLUGIN_NAME}.FakeExecutor",
                [
                    ExecutorName(
                        "airflow.providers.celery.executors.celery_executor.CeleryExecutor",
                        "CeleryExecutor",
                    ),
                    ExecutorName(
                        "airflow.executors.local_executor.LocalExecutor",
                        "LocalExecutor",
                    ),
                    ExecutorName(
                        "tests.executors.test_executor_loader.FakeExecutor",
                        None,
                    ),
                    ExecutorName(
                        f"{TEST_PLUGIN_NAME}.FakeExecutor",
                        None,
                    ),
                ],
            ),
            # Core executors and custom module path executor and plugin with aliases
            (
                (
                    "CeleryExecutor, LocalExecutor, fake_exec:tests.executors.test_executor_loader.FakeExecutor, "
                    f"plugin_exec:{TEST_PLUGIN_NAME}.FakeExecutor"
                ),
                [
                    ExecutorName(
                        "airflow.providers.celery.executors.celery_executor.CeleryExecutor",
                        "CeleryExecutor",
                    ),
                    ExecutorName(
                        "airflow.executors.local_executor.LocalExecutor",
                        "LocalExecutor",
                    ),
                    ExecutorName(
                        "tests.executors.test_executor_loader.FakeExecutor",
                        "fake_exec",
                    ),
                    ExecutorName(
                        f"{TEST_PLUGIN_NAME}.FakeExecutor",
                        "plugin_exec",
                    ),
                ],
            ),
        ],
    )
    def test_get_hybrid_executors_from_config(self, executor_config, expected_executors_list):
        ExecutorLoader.block_use_of_hybrid_exec = mock.Mock()
        with conf_vars({("core", "executor"): executor_config}):
            executors = ExecutorLoader._get_executor_names()
            assert executors == expected_executors_list

    def test_init_executors(self):
        ExecutorLoader.block_use_of_hybrid_exec = mock.Mock()
        with conf_vars({("core", "executor"): "CeleryExecutor"}):
            executors = ExecutorLoader.init_executors()
            executor_name = ExecutorLoader.get_default_executor_name()
            assert len(executors) == 1
            assert isinstance(executors[0], CeleryExecutor)
            assert "CeleryExecutor" in ExecutorLoader.executors
            assert ExecutorLoader.executors["CeleryExecutor"] == executor_name.module_path
            assert isinstance(executor_loader._loaded_executors[executor_name], CeleryExecutor)

    @pytest.mark.parametrize(
        "executor_config",
        [
            "CeleryExecutor, LocalExecutor, CeleryExecutor",
            "CeleryExecutor, LocalExecutor, LocalExecutor",
            "CeleryExecutor, my.module.path, my.module.path",
            "CeleryExecutor, my_alias:my.module.path, my.module.path",
            "CeleryExecutor, my_alias:my.module.path, other_alias:my.module.path",
            f"CeleryExecutor, {TEST_PLUGIN_NAME}.FakeExecutor, {TEST_PLUGIN_NAME}.FakeExecutor",
            f"my_alias:{TEST_PLUGIN_NAME}.FakeExecutor, other_alias:{TEST_PLUGIN_NAME}.FakeExecutor",
        ],
    )
    def test_get_hybrid_executors_from_config_duplicates_should_fail(self, executor_config):
        ExecutorLoader.block_use_of_hybrid_exec = mock.Mock()
        with conf_vars({("core", "executor"): executor_config}):
            with pytest.raises(
                AirflowConfigException, match=r".+Duplicate executors are not yet supported.+"
            ):
                ExecutorLoader._get_executor_names()

    @pytest.mark.parametrize(
        "executor_config",
        [
            "Celery::Executor, LocalExecutor",
            "LocalExecutor, Ce:ler:yExecutor, DebugExecutor",
            "LocalExecutor, CeleryExecutor:, DebugExecutor",
            "LocalExecutor, my_cool_alias:",
            "LocalExecutor, my_cool_alias:CeleryExecutor",
            "LocalExecutor, module.path.first:alias_second",
        ],
    )
    def test_get_hybrid_executors_from_config_core_executors_bad_config_format(self, executor_config):
        ExecutorLoader.block_use_of_hybrid_exec = mock.Mock()
        with conf_vars({("core", "executor"): executor_config}):
            with pytest.raises(AirflowConfigException):
                ExecutorLoader._get_executor_names()

    @pytest.mark.parametrize(
        ("executor_config", "expected_value"),
        [
            ("CeleryExecutor", "CeleryExecutor"),
            ("CeleryKubernetesExecutor", "CeleryKubernetesExecutor"),
            ("DebugExecutor", "DebugExecutor"),
            ("KubernetesExecutor", "KubernetesExecutor"),
            ("LocalExecutor", "LocalExecutor"),
            ("CeleryExecutor, LocalExecutor", "CeleryExecutor"),
            ("LocalExecutor, CeleryExecutor, DebugExecutor", "LocalExecutor"),
        ],
    )
    def test_should_support_import_executor_from_core(self, executor_config, expected_value):
        ExecutorLoader.block_use_of_hybrid_exec = mock.Mock()
        with conf_vars({("core", "executor"): executor_config}):
            executor, import_source = ExecutorLoader.import_default_executor_cls()
            assert expected_value == executor.__name__
            assert import_source == ConnectorSource.CORE

    @mock.patch("airflow.plugins_manager.plugins", [FakePlugin()])
    @mock.patch("airflow.plugins_manager.executors_modules", None)
    @pytest.mark.parametrize(
        ("executor_config"),
        [
            (f"{TEST_PLUGIN_NAME}.FakeExecutor"),
            (f"my_cool_alias:{TEST_PLUGIN_NAME}.FakeExecutor, CeleryExecutor"),
        ],
    )
    def test_should_support_import_plugins(self, executor_config):
        ExecutorLoader.block_use_of_hybrid_exec = mock.Mock()
        with conf_vars({("core", "executor"): executor_config}):
            executor, import_source = ExecutorLoader.import_default_executor_cls()
            assert "FakeExecutor" == executor.__name__
            assert import_source == ConnectorSource.PLUGIN

    @pytest.mark.parametrize(
        "executor_config",
        [
            ("tests.executors.test_executor_loader.FakeExecutor"),
            ("tests.executors.test_executor_loader.FakeExecutor, CeleryExecutor"),
            ("my_cool_alias:tests.executors.test_executor_loader.FakeExecutor, CeleryExecutor"),
        ],
    )
    def test_should_support_import_custom_path(self, executor_config):
        ExecutorLoader.block_use_of_hybrid_exec = mock.Mock()
        with conf_vars({("core", "executor"): executor_config}):
            executor, import_source = ExecutorLoader.import_default_executor_cls()
            assert "FakeExecutor" == executor.__name__
            assert import_source == ConnectorSource.CUSTOM_PATH

    @pytest.mark.parametrize(
        "executor_config",
        [
            ("tests.executors.test_executor_loader.FakeExecutor, CeleryExecutor"),
            ("CeleryExecutor, LocalExecutor"),
            ("my_cool_alias:tests.executors.test_executor_loader.FakeExecutor"),
        ],
    )
    def test_block_use_of_hybrid_exec(self, executor_config):
        with conf_vars({("core", "executor"): executor_config}):
            with pytest.raises(AirflowConfigException):
                ExecutorLoader._get_executor_names()

    @pytest.mark.db_test
    @pytest.mark.backend("mysql", "postgres")
    @pytest.mark.parametrize("executor", [FakeExecutor, FakeSingleThreadedExecutor])
    def test_validate_database_executor_compatibility_general(self, monkeypatch, executor):
        monkeypatch.delenv("_AIRFLOW__SKIP_DATABASE_EXECUTOR_COMPATIBILITY_CHECK")
        ExecutorLoader.validate_database_executor_compatibility(executor)

    @pytest.mark.db_test
    @pytest.mark.backend("sqlite")
    @pytest.mark.parametrize(
        ["executor", "expectation"],
        [
            pytest.param(FakeSingleThreadedExecutor, nullcontext(), id="single-threaded"),
            pytest.param(
                FakeExecutor,
                pytest.raises(AirflowConfigException, match=r"^error: cannot use SQLite with the .+"),
                id="multi-threaded",
            ),
        ],
    )
    def test_validate_database_executor_compatibility_sqlite(self, monkeypatch, executor, expectation):
        monkeypatch.delenv("_AIRFLOW__SKIP_DATABASE_EXECUTOR_COMPATIBILITY_CHECK")
        with expectation:
            ExecutorLoader.validate_database_executor_compatibility(executor)

    def test_load_executor(self):
        with patch.object(ExecutorLoader, "block_use_of_hybrid_exec"):
            with conf_vars({("core", "executor"): "LocalExecutor"}):
                ExecutorLoader.init_executors()
                assert isinstance(ExecutorLoader.load_executor("LocalExecutor"), LocalExecutor)
                assert isinstance(
                    ExecutorLoader.load_executor(executor_loader._executor_names[0]), LocalExecutor
                )
                assert isinstance(ExecutorLoader.load_executor(None), LocalExecutor)

    def test_load_executor_alias(self):
        with patch.object(ExecutorLoader, "block_use_of_hybrid_exec"):
            with conf_vars(
                {("core", "executor"): "local_exec:airflow.executors.local_executor.LocalExecutor"}
            ):
                ExecutorLoader.init_executors()
                assert isinstance(ExecutorLoader.load_executor("local_exec"), LocalExecutor)
                assert isinstance(
                    ExecutorLoader.load_executor("airflow.executors.local_executor.LocalExecutor"),
                    LocalExecutor,
                )
                assert isinstance(
                    ExecutorLoader.load_executor(executor_loader._executor_names[0]), LocalExecutor
                )
