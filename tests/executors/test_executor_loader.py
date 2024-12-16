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

from importlib import reload
from unittest import mock

import pytest

from airflow.exceptions import AirflowConfigException
from airflow.executors import executor_loader
from airflow.executors.executor_loader import ConnectorSource, ExecutorLoader, ExecutorName
from airflow.executors.local_executor import LocalExecutor
from airflow.providers.amazon.aws.executors.ecs.ecs_executor import AwsEcsExecutor
from airflow.providers.celery.executors.celery_executor import CeleryExecutor

from tests_common.test_utils.config import conf_vars


class FakeExecutor:
    pass


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

    def test_should_support_custom_path(self):
        with conf_vars({("core", "executor"): "tests.executors.test_executor_loader.FakeExecutor"}):
            executor = ExecutorLoader.get_default_executor()
            assert executor is not None
            assert executor.__class__.__name__ == "FakeExecutor"
            assert executor.name is not None
            assert executor.name == ExecutorName("tests.executors.test_executor_loader.FakeExecutor")
            assert executor.name.connector_source == ConnectorSource.CUSTOM_PATH

    @pytest.mark.parametrize(
        ("executor_config", "team_executor_config", "expected_executors_list"),
        [
            pytest.param(
                "CeleryExecutor",
                [],
                [
                    ExecutorName(
                        "airflow.providers.celery.executors.celery_executor.CeleryExecutor",
                        "CeleryExecutor",
                    ),
                ],
                id="one_executor",
            ),
            pytest.param(
                "CeleryExecutor",
                [
                    ("team_a", ["CeleryExecutor"]),
                    ("team_b", ["LocalExecutor"]),
                ],
                [
                    ExecutorName(
                        "airflow.providers.celery.executors.celery_executor.CeleryExecutor",
                        "CeleryExecutor",
                    ),
                    ExecutorName(
                        "airflow.providers.celery.executors.celery_executor.CeleryExecutor",
                        "CeleryExecutor",
                        "team_a",
                    ),
                    ExecutorName(
                        "airflow.executors.local_executor.LocalExecutor",
                        "LocalExecutor",
                        "team_b",
                    ),
                ],
                id="one_executor_per_team",
            ),
            pytest.param(
                "CeleryExecutor, LocalExecutor, tests.executors.test_executor_loader.FakeExecutor",
                [],
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
                ],
                id="core_executors_and_custom_module_path_executor",
            ),
            pytest.param(
                "CeleryExecutor, LocalExecutor, tests.executors.test_executor_loader.FakeExecutor",
                [
                    ("team_a", ["CeleryExecutor", "tests.executors.test_executor_loader.FakeExecutor"]),
                    ("team_b", ["tests.executors.test_executor_loader.FakeExecutor"]),
                ],
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
                        "airflow.providers.celery.executors.celery_executor.CeleryExecutor",
                        "CeleryExecutor",
                        "team_a",
                    ),
                    ExecutorName(
                        "tests.executors.test_executor_loader.FakeExecutor",
                        None,
                        "team_a",
                    ),
                    ExecutorName(
                        "tests.executors.test_executor_loader.FakeExecutor",
                        None,
                        "team_b",
                    ),
                ],
                id="core_executors_and_custom_module_path_executor_per_team",
            ),
            pytest.param(
                (
                    "CeleryExecutor, LocalExecutor, fake_exec:tests.executors.test_executor_loader.FakeExecutor"
                ),
                [],
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
                ],
                id="core_executors_and_custom_module_path_executor_with_aliases",
            ),
            pytest.param(
                (
                    "CeleryExecutor, LocalExecutor, fake_exec:tests.executors.test_executor_loader.FakeExecutor"
                ),
                [
                    (
                        "team_a",
                        ["CeleryExecutor", "fake_exec:tests.executors.test_executor_loader.FakeExecutor"],
                    ),
                    ("team_b", ["fake_exec:tests.executors.test_executor_loader.FakeExecutor"]),
                ],
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
                        "airflow.providers.celery.executors.celery_executor.CeleryExecutor",
                        "CeleryExecutor",
                        "team_a",
                    ),
                    ExecutorName(
                        "tests.executors.test_executor_loader.FakeExecutor",
                        "fake_exec",
                        "team_a",
                    ),
                    ExecutorName(
                        "tests.executors.test_executor_loader.FakeExecutor",
                        "fake_exec",
                        "team_b",
                    ),
                ],
                id="core_executors_and_custom_module_path_executor_with_aliases_per_team",
            ),
        ],
    )
    def test_get_hybrid_executors_from_config(
        self, executor_config, team_executor_config, expected_executors_list
    ):
        with conf_vars({("core", "executor"): executor_config}):
            with mock.patch(
                "airflow.executors.executor_loader.ExecutorLoader._get_team_executor_configs",
                return_value=team_executor_config,
            ):
                executors = ExecutorLoader._get_executor_names()
                assert executors == expected_executors_list

    def test_init_executors(self):
        with conf_vars({("core", "executor"): "CeleryExecutor"}):
            executors = ExecutorLoader.init_executors()
            executor_name = ExecutorLoader.get_default_executor_name()
            assert len(executors) == 1
            assert isinstance(executors[0], CeleryExecutor)
            assert "CeleryExecutor" in ExecutorLoader.executors
            assert ExecutorLoader.executors["CeleryExecutor"] == executor_name.module_path

    @pytest.mark.parametrize(
        "executor_config",
        [
            "CeleryExecutor, LocalExecutor, CeleryExecutor",
            "CeleryExecutor, LocalExecutor, LocalExecutor",
            "CeleryExecutor, my.module.path, my.module.path",
            "CeleryExecutor, my_alias:my.module.path, my.module.path",
            "CeleryExecutor, my_alias:my.module.path, other_alias:my.module.path",
        ],
    )
    def test_get_hybrid_executors_from_config_duplicates_should_fail(self, executor_config):
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
        with conf_vars({("core", "executor"): executor_config}):
            executor, import_source = ExecutorLoader.import_default_executor_cls()
            assert expected_value == executor.__name__
            assert import_source == ConnectorSource.CORE

    @pytest.mark.parametrize(
        "executor_config",
        [
            ("tests.executors.test_executor_loader.FakeExecutor"),
            ("tests.executors.test_executor_loader.FakeExecutor, CeleryExecutor"),
            ("my_cool_alias:tests.executors.test_executor_loader.FakeExecutor, CeleryExecutor"),
        ],
    )
    def test_should_support_import_custom_path(self, executor_config):
        with conf_vars({("core", "executor"): executor_config}):
            executor, import_source = ExecutorLoader.import_default_executor_cls()
            assert executor.__name__ == "FakeExecutor"
            assert import_source == ConnectorSource.CUSTOM_PATH

    def test_load_executor(self):
        with conf_vars({("core", "executor"): "LocalExecutor"}):
            ExecutorLoader.init_executors()
            assert isinstance(ExecutorLoader.load_executor("LocalExecutor"), LocalExecutor)
            assert isinstance(ExecutorLoader.load_executor(executor_loader._executor_names[0]), LocalExecutor)
            assert isinstance(ExecutorLoader.load_executor(None), LocalExecutor)

    def test_load_executor_alias(self):
        with conf_vars({("core", "executor"): "local_exec:airflow.executors.local_executor.LocalExecutor"}):
            ExecutorLoader.init_executors()
            assert isinstance(ExecutorLoader.load_executor("local_exec"), LocalExecutor)
            assert isinstance(
                ExecutorLoader.load_executor("airflow.executors.local_executor.LocalExecutor"),
                LocalExecutor,
            )
            assert isinstance(ExecutorLoader.load_executor(executor_loader._executor_names[0]), LocalExecutor)

    @mock.patch("airflow.providers.amazon.aws.executors.ecs.ecs_executor.AwsEcsExecutor", autospec=True)
    def test_load_custom_executor_with_classname(self, mock_executor):
        with conf_vars(
            {
                (
                    "core",
                    "executor",
                ): "my_alias:airflow.providers.amazon.aws.executors.ecs.ecs_executor.AwsEcsExecutor"
            }
        ):
            ExecutorLoader.init_executors()
            assert isinstance(ExecutorLoader.load_executor("my_alias"), AwsEcsExecutor)
            assert isinstance(ExecutorLoader.load_executor("AwsEcsExecutor"), AwsEcsExecutor)
            assert isinstance(
                ExecutorLoader.load_executor(
                    "airflow.providers.amazon.aws.executors.ecs.ecs_executor.AwsEcsExecutor"
                ),
                AwsEcsExecutor,
            )
            assert isinstance(
                ExecutorLoader.load_executor(executor_loader._executor_names[0]), AwsEcsExecutor
            )
