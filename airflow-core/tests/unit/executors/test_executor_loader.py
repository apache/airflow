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

from unittest import mock
from unittest.mock import patch

import pytest

from airflow.exceptions import AirflowConfigException
from airflow.executors import executor_loader
from airflow.executors.executor_loader import ConnectorSource, ExecutorName
from airflow.executors.local_executor import LocalExecutor

from tests_common.test_utils.config import conf_vars


class FakeExecutor:
    pass


celery_executor = pytest.importorskip("airflow.providers.celery.executors.celery_executor")
ecs_executor = pytest.importorskip("airflow.providers.amazon.aws.executors.ecs.ecs_executor")


@pytest.mark.usefixtures("clean_executor_loader")
class TestExecutorLoader:
    def test_no_executor_configured(self):
        with conf_vars({("core", "executor"): None}):
            with pytest.raises(AirflowConfigException, match=r".*not found in config$"):
                executor_loader.ExecutorLoader.get_default_executor()

    def test_empty_executor_configured(self):
        with conf_vars({("core", "executor"): ""}):
            with pytest.raises(
                AirflowConfigException,
                match="The 'executor' key in the 'core' section of the configuration is mandatory and cannot be empty",
            ):
                executor_loader.ExecutorLoader.get_default_executor()

    @pytest.mark.parametrize(
        "executor_name",
        [
            "CeleryExecutor",
            "KubernetesExecutor",
            "LocalExecutor",
        ],
    )
    def test_should_support_executor_from_core(self, executor_name):
        with conf_vars({("core", "executor"): executor_name}):
            executor = executor_loader.ExecutorLoader.get_default_executor()
            assert executor is not None
            assert executor_name == executor.__class__.__name__
            assert executor.name is not None
            assert executor.name == ExecutorName(
                executor_loader.ExecutorLoader.executors[executor_name], alias=executor_name
            )
            assert executor.name.connector_source == ConnectorSource.CORE

    def test_should_support_custom_path(self):
        with conf_vars({("core", "executor"): "unit.executors.test_executor_loader.FakeExecutor"}):
            executor = executor_loader.ExecutorLoader.get_default_executor()
            assert executor is not None
            assert executor.__class__.__name__ == "FakeExecutor"
            assert executor.name is not None
            assert executor.name == ExecutorName("unit.executors.test_executor_loader.FakeExecutor")
            assert executor.name.connector_source == ConnectorSource.CUSTOM_PATH

    @pytest.mark.parametrize(
        ("executor_config", "expected_executors_list"),
        [
            pytest.param(
                "CeleryExecutor",
                [
                    ExecutorName(
                        module_path="airflow.providers.celery.executors.celery_executor.CeleryExecutor",
                        alias="CeleryExecutor",
                        team_name=None,
                    ),
                ],
                id="one_executor",
            ),
            pytest.param(
                "=CeleryExecutor;team_a=CeleryExecutor;team_b=LocalExecutor",
                [
                    ExecutorName(
                        module_path="airflow.providers.celery.executors.celery_executor.CeleryExecutor",
                        alias="CeleryExecutor",
                        team_name=None,
                    ),
                    ExecutorName(
                        module_path="airflow.providers.celery.executors.celery_executor.CeleryExecutor",
                        alias="CeleryExecutor",
                        team_name="team_a",
                    ),
                    ExecutorName(
                        module_path="airflow.executors.local_executor.LocalExecutor",
                        alias="LocalExecutor",
                        team_name="team_b",
                    ),
                ],
                id="one_executor_per_team",
            ),
            pytest.param(
                "CeleryExecutor, LocalExecutor, unit.executors.test_executor_loader.FakeExecutor",
                [
                    ExecutorName(
                        module_path="airflow.providers.celery.executors.celery_executor.CeleryExecutor",
                        alias="CeleryExecutor",
                        team_name=None,
                    ),
                    ExecutorName(
                        module_path="airflow.executors.local_executor.LocalExecutor",
                        alias="LocalExecutor",
                        team_name=None,
                    ),
                    ExecutorName(
                        module_path="unit.executors.test_executor_loader.FakeExecutor",
                        alias=None,
                        team_name=None,
                    ),
                ],
                id="core_executors_and_custom_module_path_executor",
            ),
            pytest.param(
                "=CeleryExecutor,LocalExecutor,unit.executors.test_executor_loader.FakeExecutor;team_a=CeleryExecutor,unit.executors.test_executor_loader.FakeExecutor;team_b=unit.executors.test_executor_loader.FakeExecutor",
                [
                    ExecutorName(
                        module_path="airflow.providers.celery.executors.celery_executor.CeleryExecutor",
                        alias="CeleryExecutor",
                        team_name=None,
                    ),
                    ExecutorName(
                        module_path="airflow.executors.local_executor.LocalExecutor",
                        alias="LocalExecutor",
                        team_name=None,
                    ),
                    ExecutorName(
                        module_path="unit.executors.test_executor_loader.FakeExecutor",
                        alias=None,
                        team_name=None,
                    ),
                    ExecutorName(
                        module_path="airflow.providers.celery.executors.celery_executor.CeleryExecutor",
                        alias="CeleryExecutor",
                        team_name="team_a",
                    ),
                    ExecutorName(
                        module_path="unit.executors.test_executor_loader.FakeExecutor",
                        alias=None,
                        team_name="team_a",
                    ),
                    ExecutorName(
                        module_path="unit.executors.test_executor_loader.FakeExecutor",
                        alias=None,
                        team_name="team_b",
                    ),
                ],
                id="core_executors_and_custom_module_path_executor_per_team",
            ),
            pytest.param(
                ("CeleryExecutor, LocalExecutor, fake_exec:unit.executors.test_executor_loader.FakeExecutor"),
                [
                    ExecutorName(
                        module_path="airflow.providers.celery.executors.celery_executor.CeleryExecutor",
                        alias="CeleryExecutor",
                        team_name=None,
                    ),
                    ExecutorName(
                        module_path="airflow.executors.local_executor.LocalExecutor",
                        alias="LocalExecutor",
                        team_name=None,
                    ),
                    ExecutorName(
                        module_path="unit.executors.test_executor_loader.FakeExecutor",
                        alias="fake_exec",
                        team_name=None,
                    ),
                ],
                id="core_executors_and_custom_module_path_executor_with_aliases",
            ),
            pytest.param(
                (
                    "=CeleryExecutor,LocalExecutor,fake_exec:unit.executors.test_executor_loader.FakeExecutor;team_a=CeleryExecutor,fake_exec:unit.executors.test_executor_loader.FakeExecutor;team_b=fake_exec:unit.executors.test_executor_loader.FakeExecutor"
                ),
                [
                    ExecutorName(
                        module_path="airflow.providers.celery.executors.celery_executor.CeleryExecutor",
                        alias="CeleryExecutor",
                        team_name=None,
                    ),
                    ExecutorName(
                        module_path="airflow.executors.local_executor.LocalExecutor",
                        alias="LocalExecutor",
                        team_name=None,
                    ),
                    ExecutorName(
                        module_path="unit.executors.test_executor_loader.FakeExecutor",
                        alias="fake_exec",
                        team_name=None,
                    ),
                    ExecutorName(
                        module_path="airflow.providers.celery.executors.celery_executor.CeleryExecutor",
                        alias="CeleryExecutor",
                        team_name="team_a",
                    ),
                    ExecutorName(
                        module_path="unit.executors.test_executor_loader.FakeExecutor",
                        alias="fake_exec",
                        team_name="team_a",
                    ),
                    ExecutorName(
                        module_path="unit.executors.test_executor_loader.FakeExecutor",
                        alias="fake_exec",
                        team_name="team_b",
                    ),
                ],
                id="core_executors_and_custom_module_path_executor_with_aliases_per_team",
            ),
        ],
    )
    def test_get_hybrid_executors_from_configs(self, executor_config, expected_executors_list):
        # Mock the blocking method for tests that involve actual team configurations
        with (
            mock.patch.object(executor_loader.ExecutorLoader, "block_use_of_multi_team"),
            mock.patch.object(executor_loader.ExecutorLoader, "_validate_teams_exist_in_database"),
        ):
            with conf_vars({("core", "executor"): executor_config, ("core", "multi_team"): "True"}):
                executors = executor_loader.ExecutorLoader._get_executor_names()
                assert executors == expected_executors_list

    def test_get_multi_team_executors_from_config_blocked_by_default(self):
        """By default the use of multiple team based executors is blocked for now."""
        with conf_vars({("core", "executor"): "=CeleryExecutor;team_a=CeleryExecutor;team_b=LocalExecutor"}):
            with pytest.raises(
                AirflowConfigException,
                match=r".*Configuring multiple team based executors is not yet supported!.*",
            ):
                executor_loader.ExecutorLoader._get_executor_names()

    def test_init_executors(self):
        from airflow.providers.celery.executors.celery_executor import CeleryExecutor

        with conf_vars({("core", "executor"): "CeleryExecutor"}):
            executors = executor_loader.ExecutorLoader.init_executors()
            executor_name = executor_loader.ExecutorLoader.get_default_executor_name()
            assert len(executors) == 1
            assert isinstance(executors[0], CeleryExecutor)
            assert "CeleryExecutor" in executor_loader.ExecutorLoader.executors
            assert executor_loader.ExecutorLoader.executors["CeleryExecutor"] == executor_name.module_path

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
                executor_loader.ExecutorLoader._get_executor_names()

    @pytest.mark.parametrize(
        "executor_config",
        [
            "Celery::Executor, LocalExecutor",
            "LocalExecutor, Ce:ler:yExecutor",
            "LocalExecutor, CeleryExecutor:",
            "LocalExecutor, my_cool_alias:",
            "LocalExecutor, my_cool_alias:CeleryExecutor",
            "LocalExecutor, module.path.first:alias_second",
        ],
    )
    def test_get_hybrid_executors_from_config_core_executors_bad_config_format(self, executor_config):
        with conf_vars({("core", "executor"): executor_config}):
            with pytest.raises(AirflowConfigException):
                executor_loader.ExecutorLoader._get_executor_names()

    @pytest.mark.parametrize(
        ("executor_config", "expected_value"),
        [
            ("CeleryExecutor", "CeleryExecutor"),
            ("KubernetesExecutor", "KubernetesExecutor"),
            ("LocalExecutor", "LocalExecutor"),
            ("CeleryExecutor, LocalExecutor", "CeleryExecutor"),
            ("LocalExecutor, CeleryExecutor", "LocalExecutor"),
        ],
    )
    def test_should_support_import_executor_from_core(self, executor_config, expected_value):
        with conf_vars({("core", "executor"): executor_config}):
            executor, import_source = executor_loader.ExecutorLoader.import_default_executor_cls()
            assert expected_value == executor.__name__
            assert import_source == ConnectorSource.CORE

    @pytest.mark.parametrize(
        "executor_config",
        [
            ("unit.executors.test_executor_loader.FakeExecutor"),
            ("unit.executors.test_executor_loader.FakeExecutor, CeleryExecutor"),
            ("my_cool_alias:unit.executors.test_executor_loader.FakeExecutor, CeleryExecutor"),
        ],
    )
    def test_should_support_import_custom_path(self, executor_config):
        with conf_vars({("core", "executor"): executor_config}):
            executor, import_source = executor_loader.ExecutorLoader.import_default_executor_cls()
            assert executor.__name__ == "FakeExecutor"
            assert import_source == ConnectorSource.CUSTOM_PATH

    def test_load_executor(self):
        with conf_vars({("core", "executor"): "LocalExecutor"}):
            executor_loader.ExecutorLoader.init_executors()
            assert isinstance(executor_loader.ExecutorLoader.load_executor("LocalExecutor"), LocalExecutor)
            assert isinstance(
                executor_loader.ExecutorLoader.load_executor(executor_loader._executor_names[0]),
                LocalExecutor,
            )
            assert isinstance(executor_loader.ExecutorLoader.load_executor(None), LocalExecutor)

    def test_load_executor_alias(self):
        with conf_vars({("core", "executor"): "local_exec:airflow.executors.local_executor.LocalExecutor"}):
            executor_loader.ExecutorLoader.init_executors()
            assert isinstance(executor_loader.ExecutorLoader.load_executor("local_exec"), LocalExecutor)
            assert isinstance(
                executor_loader.ExecutorLoader.load_executor(
                    "airflow.executors.local_executor.LocalExecutor"
                ),
                LocalExecutor,
            )
            assert isinstance(
                executor_loader.ExecutorLoader.load_executor(executor_loader._executor_names[0]),
                LocalExecutor,
            )

    @mock.patch(
        "airflow.executors.executor_loader.ExecutorLoader._get_executor_names",
        wraps=executor_loader.ExecutorLoader._get_executor_names,
    )
    def test_call_load_executor_method_without_init_executors(self, mock_get_executor_names):
        with conf_vars({("core", "executor"): "LocalExecutor"}):
            executor_loader.ExecutorLoader.load_executor("LocalExecutor")
            mock_get_executor_names.assert_called_once()

    def test_load_custom_executor_with_classname(self):
        from airflow.providers.amazon.aws.executors.ecs.ecs_executor import AwsEcsExecutor

        with patch("airflow.providers.amazon.aws.executors.ecs.ecs_executor.AwsEcsExecutor", autospec=True):
            with conf_vars(
                {
                    (
                        "core",
                        "executor",
                    ): "my_alias:airflow.providers.amazon.aws.executors.ecs.ecs_executor.AwsEcsExecutor"
                }
            ):
                executor_loader.ExecutorLoader.init_executors()
                assert isinstance(executor_loader.ExecutorLoader.load_executor("my_alias"), AwsEcsExecutor)
                assert isinstance(
                    executor_loader.ExecutorLoader.load_executor("AwsEcsExecutor"), AwsEcsExecutor
                )
                assert isinstance(
                    executor_loader.ExecutorLoader.load_executor(
                        "airflow.providers.amazon.aws.executors.ecs.ecs_executor.AwsEcsExecutor"
                    ),
                    AwsEcsExecutor,
                )
                assert isinstance(
                    executor_loader.ExecutorLoader.load_executor(executor_loader._executor_names[0]),
                    AwsEcsExecutor,
                )

    def test_get_executor_names_set_module_variables(self):
        with conf_vars(
            {
                ("core", "multi_team"): "True",
                (
                    "core",
                    "executor",
                ): "=CeleryExecutor,LocalExecutor,fake_exec:unit.executors.test_executor_loader.FakeExecutor;team_a=CeleryExecutor,unit.executors.test_executor_loader.FakeExecutor;team_b=fake_exec:unit.executors.test_executor_loader.FakeExecutor",
            }
        ):
            celery_path = "airflow.providers.celery.executors.celery_executor.CeleryExecutor"
            local_path = "airflow.executors.local_executor.LocalExecutor"
            fake_exec_path = "unit.executors.test_executor_loader.FakeExecutor"
            celery_global = ExecutorName(module_path=celery_path, alias="CeleryExecutor", team_name=None)
            local_global = ExecutorName(module_path=local_path, alias="LocalExecutor", team_name=None)
            fake_global = ExecutorName(module_path=fake_exec_path, alias="fake_exec", team_name=None)
            team_a_celery = ExecutorName(
                module_path=celery_path,
                alias="CeleryExecutor",
                team_name="team_a",
            )
            team_a_fake = ExecutorName(
                module_path=fake_exec_path,
                team_name="team_a",
            )
            team_b_fake = ExecutorName(
                module_path=fake_exec_path,
                alias="fake_exec",
                team_name="team_b",
            )
            assert executor_loader._executor_names == []
            assert executor_loader._alias_to_executors_per_team == {}
            assert executor_loader._module_to_executors_per_team == {}
            assert executor_loader._classname_to_executors_per_team == {}
            assert executor_loader._team_name_to_executors == {}
            with (
                mock.patch.object(executor_loader.ExecutorLoader, "block_use_of_multi_team"),
                mock.patch.object(executor_loader.ExecutorLoader, "_validate_teams_exist_in_database"),
            ):
                executor_loader.ExecutorLoader._get_executor_names()
            assert executor_loader._executor_names == [
                celery_global,
                local_global,
                fake_global,
                team_a_celery,
                team_a_fake,
                team_b_fake,
            ]
            assert executor_loader._alias_to_executors_per_team == {
                None: {
                    "CeleryExecutor": celery_global,
                    "LocalExecutor": local_global,
                    "fake_exec": fake_global,
                },
                "team_a": {"CeleryExecutor": team_a_celery},
                "team_b": {"fake_exec": team_b_fake},
            }
            assert executor_loader._module_to_executors_per_team == {
                None: {
                    celery_path: celery_global,
                    local_path: local_global,
                    fake_exec_path: fake_global,
                },
                "team_a": {
                    celery_path: team_a_celery,
                    fake_exec_path: team_a_fake,
                },
                "team_b": {
                    fake_exec_path: team_b_fake,
                },
            }
            assert executor_loader._classname_to_executors_per_team == {
                None: {
                    "CeleryExecutor": celery_global,
                    "LocalExecutor": local_global,
                    "FakeExecutor": fake_global,
                },
                "team_a": {
                    "CeleryExecutor": team_a_celery,
                    "FakeExecutor": team_a_fake,
                },
                "team_b": {
                    "FakeExecutor": team_b_fake,
                },
            }
            assert executor_loader._team_name_to_executors == {
                None: [celery_global, local_global, fake_global],
                "team_a": [team_a_celery, team_a_fake],
                "team_b": [team_b_fake],
            }

    @pytest.mark.parametrize(
        "executor_config",
        [
            "CeleryExecutor;team1=LocalExecutor;team1=KubernetesExecutor",
            "CeleryExecutor;team_a=LocalExecutor;team_b=KubernetesExecutor;team_a=CeleryExecutor",
        ],
    )
    def test_duplicate_team_names_should_fail(self, executor_config):
        """Test that duplicate team names in executor configuration raise an exception."""
        with mock.patch.object(executor_loader.ExecutorLoader, "block_use_of_multi_team"):
            with conf_vars({("core", "executor"): executor_config, ("core", "multi_team"): "True"}):
                with pytest.raises(
                    AirflowConfigException,
                    match=r"Team '.+' appears more than once in executor configuration",
                ):
                    executor_loader.ExecutorLoader._get_team_executor_configs()

    @pytest.mark.parametrize(
        "executor_config",
        [
            "CeleryExecutor;LocalExecutor",  # Two separate global teams
            "CeleryExecutor;KubernetesExecutor;LocalExecutor",  # Three separate global teams
            "=CeleryExecutor;LocalExecutor",  # Explicit global team followed by another global team
            "CeleryExecutor;=LocalExecutor",  # Global team followed by explicit global team
        ],
    )
    def test_multiple_global_team_specifications_should_fail(self, executor_config):
        """Test that multiple global team specifications raise an exception.

        Only one global team specification should be allowed (comma-delimited executors),
        not multiple semicolon-separated global teams.
        """
        with conf_vars({("core", "executor"): executor_config}):
            with pytest.raises(
                AirflowConfigException, match=r"Team 'None' appears more than once in executor configuration"
            ):
                executor_loader.ExecutorLoader._get_team_executor_configs()

    def test_valid_team_configurations_order_preservation(self):
        """Test that valid team configurations preserve order and work correctly."""
        executor_config = "LocalExecutor;team1=CeleryExecutor,KubernetesExecutor;team2=LocalExecutor"
        expected_configs = [
            (None, ["LocalExecutor"]),
            ("team1", ["CeleryExecutor", "KubernetesExecutor"]),
            ("team2", ["LocalExecutor"]),
        ]

        with (
            mock.patch.object(executor_loader.ExecutorLoader, "block_use_of_multi_team"),
            mock.patch.object(executor_loader.ExecutorLoader, "_validate_teams_exist_in_database"),
        ):
            with conf_vars({("core", "executor"): executor_config, ("core", "multi_team"): "True"}):
                configs = executor_loader.ExecutorLoader._get_team_executor_configs()
                assert configs == expected_configs

    @pytest.mark.parametrize(
        ("executor_config", "expected_configs"),
        [
            # Single global team with one executor
            ("CeleryExecutor", [(None, ["CeleryExecutor"])]),
            # Single global team with multiple comma-delimited executors
            ("CeleryExecutor,LocalExecutor", [(None, ["CeleryExecutor", "LocalExecutor"])]),
            (
                "CeleryExecutor,LocalExecutor,KubernetesExecutor",
                [(None, ["CeleryExecutor", "LocalExecutor", "KubernetesExecutor"])],
            ),
            # Single global team with explicit = prefix
            ("=CeleryExecutor,LocalExecutor", [(None, ["CeleryExecutor", "LocalExecutor"])]),
        ],
    )
    def test_single_global_team_configurations_work(self, executor_config, expected_configs):
        """Test that single global team configurations work correctly.

        A single global team can have multiple executors specified as comma-delimited list.
        """
        with conf_vars({("core", "executor"): executor_config}):
            configs = executor_loader.ExecutorLoader._get_team_executor_configs()
            assert configs == expected_configs

    @pytest.mark.parametrize(
        "executor_config",
        [
            "team1=CeleryExecutor",
            "team1=CeleryExecutor;team2=LocalExecutor",
            "team1=CeleryExecutor;team2=LocalExecutor;team3=KubernetesExecutor",
            "team_a=CeleryExecutor,LocalExecutor;team_b=KubernetesExecutor",
        ],
    )
    def test_team_only_configurations_should_fail(self, executor_config):
        """Test that configurations with only team-based executors fail validation."""
        with (
            mock.patch.object(executor_loader.ExecutorLoader, "block_use_of_multi_team"),
            conf_vars({("core", "executor"): executor_config}),
        ):
            with pytest.raises(
                AirflowConfigException, match=r"At least one global executor must be configured"
            ):
                executor_loader.ExecutorLoader._get_team_executor_configs()

    def test_team_validation_with_valid_teams_in_config(self):
        """Test that executor config with valid teams loads successfully."""
        mock_team_names = {"team_a", "team_b"}

        with (
            patch.object(executor_loader.Team, "get_all_team_names", return_value=mock_team_names),
            mock.patch.object(executor_loader.ExecutorLoader, "block_use_of_multi_team"),
        ):
            with conf_vars(
                {
                    ("core", "executor"): "=CeleryExecutor;team_a=CeleryExecutor;team_b=LocalExecutor",
                    ("core", "multi_team"): "True",
                }
            ):
                configs = executor_loader.ExecutorLoader._get_team_executor_configs()

                assert len(configs) == 3
                assert configs[0] == (None, ["CeleryExecutor"])
                assert configs[1] == ("team_a", ["CeleryExecutor"])
                assert configs[2] == ("team_b", ["LocalExecutor"])

    def test_team_validation_with_invalid_teams_in_config(self):
        """Test that executor config with invalid teams fails with clear error."""
        mock_team_names = {"team_a"}  # team_b and team_c are missing

        with (
            patch.object(executor_loader.Team, "get_all_team_names", return_value=mock_team_names),
            mock.patch.object(executor_loader.ExecutorLoader, "block_use_of_multi_team"),
        ):
            with conf_vars(
                {
                    (
                        "core",
                        "executor",
                    ): "=CeleryExecutor;team_a=CeleryExecutor;team_b=LocalExecutor;team_c=KubernetesExecutor",
                    ("core", "multi_team"): "True",
                }
            ):
                with pytest.raises(AirflowConfigException):
                    executor_loader.ExecutorLoader._get_team_executor_configs()

    def test_team_validation_skips_global_teams(self):
        """Test that team validation does not validate global teams."""
        with patch.object(executor_loader.Team, "get_all_team_names") as mock_get_team_names:
            with conf_vars({("core", "executor"): "CeleryExecutor,LocalExecutor"}):
                configs = executor_loader.ExecutorLoader._get_team_executor_configs()

                assert len(configs) == 1
                assert configs[0] == (None, ["CeleryExecutor", "LocalExecutor"])

                # No team validation should occur since only global teams are configured
                mock_get_team_names.assert_not_called()

    def test_get_executor_names_skip_team_validation(self):
        """Test that get_executor_names can skip team validation."""
        with (
            patch.object(executor_loader.Team, "get_all_team_names") as mock_get_team_names,
            mock.patch.object(executor_loader.ExecutorLoader, "block_use_of_multi_team"),
        ):
            with conf_vars(
                {("core", "executor"): "=CeleryExecutor;team_a=LocalExecutor", ("core", "multi_team"): "True"}
            ):
                # Should not call team validation when validate_teams=False
                executor_loader.ExecutorLoader.get_executor_names(validate_teams=False)
                mock_get_team_names.assert_not_called()

    def test_get_executor_names_default_validates_teams(self):
        """Test that get_executor_names validates teams by default."""
        with (
            patch.object(executor_loader.Team, "get_all_team_names") as mock_get_team_names,
            mock.patch.object(executor_loader.ExecutorLoader, "block_use_of_multi_team"),
        ):
            with conf_vars(
                {("core", "executor"): "=CeleryExecutor;team_a=LocalExecutor", ("core", "multi_team"): "True"}
            ):
                # Default behavior should validate teams
                mock_get_team_names.return_value = {"team_a"}
                executor_loader.ExecutorLoader.get_executor_names()
                mock_get_team_names.assert_called_once()
