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

import pytest

from airflow.models import Base
from airflow.utils.db_manager import BaseDBManager

pytestmark = [pytest.mark.db_test]


class MockDBManager(BaseDBManager):
    metadata = Base.metadata
    version_table_name = "mock_alembic_version"
    migration_dir = "mock_migration_dir"
    alembic_file = "mock_alembic.ini"
    supports_table_dropping = True


class CustomDBManager(BaseDBManager):
    metadata = Base.metadata
    version_table_name = "custom_alembic_version"
    migration_dir = "custom_migration_dir"
    alembic_file = "custom_alembic.ini"

    def downgrade(self, to_revision, from_revision=None, show_sql_only=False):
        from alembic import command as alembic_command

        config = self.get_alembic_config()
        alembic_command.downgrade(config, revision=to_revision, sql=show_sql_only)


class TestBaseDBManager:
    @mock.patch.object(BaseDBManager, "get_alembic_config")
    @mock.patch.object(BaseDBManager, "get_current_revision")
    @mock.patch.object(BaseDBManager, "create_db_from_orm")
    def test_create_db_from_orm_called_from_init(
        self, mock_create_db_from_orm, mock_current_revision, mock_config, session
    ):
        mock_current_revision.return_value = None

        manager = MockDBManager(session)
        manager.initdb()
        mock_create_db_from_orm.assert_called_once()

    @mock.patch.object(BaseDBManager, "get_alembic_config")
    @mock.patch("alembic.command.upgrade")
    def test_upgrade(self, mock_alembic_cmd, mock_alembic_config, session, caplog):
        manager = MockDBManager(session)
        manager.upgradedb()
        mock_alembic_cmd.assert_called_once()
        assert "Upgrading the MockDBManager database" in caplog.text

    @mock.patch.object(BaseDBManager, "get_script_object")
    @mock.patch.object(BaseDBManager, "get_current_revision")
    def test_check_migration(self, mock_script_obj, mock_current_revision, session):
        manager = MockDBManager(session)
        manager.check_migration()  # just ensure this can be called

    def test_custom_db_manager_downgrade_uses_revision_kwarg(self, session):
        manager = CustomDBManager(session)
        with (
            mock.patch.object(BaseDBManager, "get_alembic_config") as mock_config,
            mock.patch("alembic.command.downgrade") as mock_alembic_downgrade,
        ):
            cfg = object()
            mock_config.return_value = cfg
            manager.downgrade(to_revision="abc123", show_sql_only=True)
            mock_alembic_downgrade.assert_called_once_with(cfg, revision="abc123", sql=True)

    def test_custom_db_manager_downgrade_rejects_to_version_kwarg(self, session):
        manager = CustomDBManager(session)
        with pytest.raises(TypeError):
            # Ensure the old kwarg name is not accepted anymore
            manager.downgrade(to_version="1.2.3")  # type: ignore[call-arg]


class TestRunDBManager:
    """Tests for RunDBManager executor DB manager discovery."""

    @mock.patch("airflow.executors.executor_loader.ExecutorLoader.get_executor_names")
    @mock.patch("airflow.executors.executor_loader.ExecutorLoader.import_executor_cls")
    def test_rundbmanager_discovers_executor_db_managers(self, mock_import_executor, mock_get_names, session):
        """Test that RunDBManager discovers and loads executor DB managers."""
        from airflow.utils.db_manager import RunDBManager

        # Mock executor with DB manager
        class MockExecutor:
            @staticmethod
            def get_db_manager():
                return "mock.executor.db.Manager"

        mock_get_names.return_value = ["MockExecutor"]
        mock_import_executor.return_value = (MockExecutor, None)

        with mock.patch("airflow.utils.db_manager.import_string") as mock_import_string:
            manager = RunDBManager()
            assert manager is not None

            mock_get_names.assert_called_once_with(validate_teams=False)
            mock_import_executor.assert_called_once_with("MockExecutor")
            mock_import_string.assert_any_call("mock.executor.db.Manager")

    @mock.patch("airflow.executors.executor_loader.ExecutorLoader.get_executor_names")
    @mock.patch("airflow.executors.executor_loader.ExecutorLoader.import_executor_cls")
    def test_rundbmanager_discovers_multiple_executor_db_managers(
        self, mock_import_executor, mock_get_names, session
    ):
        """Test discovery of DB managers from multiple executors."""
        from airflow.utils.db_manager import RunDBManager

        class Executor1:
            @staticmethod
            def get_db_manager():
                return "executor1.db.Manager"

        class Executor2:
            @staticmethod
            def get_db_manager():
                return "executor2.db.Manager"

        mock_get_names.return_value = ["Executor1", "Executor2"]
        mock_import_executor.side_effect = [
            (Executor1, None),
            (Executor2, None),
        ]

        with mock.patch("airflow.utils.db_manager.import_string") as mock_import_string:
            manager = RunDBManager()
            assert manager is not None

            assert mock_import_executor.call_count == 2
            assert mock_import_string.call_count >= 2

    @mock.patch("airflow.executors.executor_loader.ExecutorLoader.get_executor_names")
    @mock.patch("airflow.executors.executor_loader.ExecutorLoader.import_executor_cls")
    def test_rundbmanager_handles_executor_without_db_manager(
        self, mock_import_executor, mock_get_names, session
    ):
        """Test that executors without get_db_manager() are skipped gracefully."""
        from airflow.utils.db_manager import RunDBManager

        class LegacyExecutor:
            pass  # No get_db_manager method

        mock_get_names.return_value = ["LegacyExecutor"]
        mock_import_executor.return_value = (LegacyExecutor, None)

        # Should not raise an exception
        manager = RunDBManager()
        assert manager is not None

    @mock.patch("airflow.executors.executor_loader.ExecutorLoader.get_executor_names")
    @mock.patch("airflow.executors.executor_loader.ExecutorLoader.import_executor_cls")
    def test_rundbmanager_handles_executor_db_manager_none(
        self, mock_import_executor, mock_get_names, session
    ):
        """Test that executors returning None from get_db_manager() are handled."""
        from airflow.executors.base_executor import BaseExecutor
        from airflow.utils.db_manager import RunDBManager

        class ExecutorWithoutDB(BaseExecutor):
            @staticmethod
            def get_db_manager():
                return None

        mock_get_names.return_value = ["ExecutorWithoutDB"]
        mock_import_executor.return_value = (ExecutorWithoutDB, None)

        with mock.patch("airflow.utils.db_manager.import_string") as mock_import_string:
            manager = RunDBManager()
            assert manager is not None

            # import_string should not be called for None values
            # Only auth manager import should happen
            assert mock_import_string.call_count <= 1

    @mock.patch("airflow.executors.executor_loader.ExecutorLoader.get_executor_names")
    @mock.patch("airflow.executors.executor_loader.ExecutorLoader.import_executor_cls")
    def test_rundbmanager_prevents_duplicate_db_managers(self, mock_import_executor, mock_get_names, session):
        """Test that duplicate DB manager paths are not added multiple times."""
        from airflow.utils.db_manager import RunDBManager

        class Executor1:
            @staticmethod
            def get_db_manager():
                return "shared.db.Manager"

        class Executor2:
            @staticmethod
            def get_db_manager():
                return "shared.db.Manager"  # Same as Executor1

        mock_get_names.return_value = ["Executor1", "Executor2"]
        mock_import_executor.side_effect = [
            (Executor1, None),
            (Executor2, None),
        ]

        with mock.patch("airflow.utils.db_manager.import_string") as mock_import_string:
            manager = RunDBManager()
            assert manager is not None

            # Should only import the shared manager once
            shared_manager_calls = [
                call for call in mock_import_string.call_args_list if "shared.db.Manager" in str(call)
            ]
            assert len(shared_manager_calls) == 1

    @mock.patch("airflow.executors.executor_loader.ExecutorLoader.get_executor_names")
    @mock.patch("airflow.executors.executor_loader.ExecutorLoader.import_executor_cls")
    def test_rundbmanager_handles_executor_import_error(self, mock_import_executor, mock_get_names, session):
        """Test graceful handling when executor import fails."""
        from airflow.utils.db_manager import RunDBManager

        mock_get_names.return_value = ["BrokenExecutor"]
        mock_import_executor.side_effect = Exception("Import failed")

        # Should not raise an exception, just skip the broken executor
        manager = RunDBManager()
        assert manager is not None

    @mock.patch("airflow.executors.executor_loader.ExecutorLoader.get_executor_names")
    def test_rundbmanager_handles_get_executor_names_error(self, mock_get_names, session):
        """Test graceful handling when get_executor_names fails."""
        from airflow.utils.db_manager import RunDBManager

        mock_get_names.side_effect = Exception("Config error")

        # Should not raise an exception, just skip executor DB manager discovery
        manager = RunDBManager()
        assert manager is not None

    @mock.patch("airflow.api_fastapi.app.create_auth_manager")
    @mock.patch("airflow.executors.executor_loader.ExecutorLoader.get_executor_names")
    @mock.patch("airflow.executors.executor_loader.ExecutorLoader.import_executor_cls")
    def test_rundbmanager_integrates_auth_and_executor_managers(
        self, mock_import_executor, mock_get_names, mock_auth_manager, session
    ):
        """Test that both auth manager and executor DB managers are loaded."""
        from airflow.utils.db_manager import RunDBManager

        # Mock auth manager with DB manager
        mock_auth = mock.Mock()
        mock_auth.get_db_manager.return_value = "auth.db.Manager"
        mock_auth_manager.return_value = mock_auth

        # Mock executor with DB manager
        class CustomExecutor:
            @staticmethod
            def get_db_manager():
                return "executor.db.Manager"

        mock_get_names.return_value = ["CustomExecutor"]
        mock_import_executor.return_value = (CustomExecutor, None)

        with mock.patch("airflow.utils.db_manager.import_string") as mock_import_string:
            manager = RunDBManager()
            assert manager is not None

            # Should import both auth and executor managers
            calls = [str(call) for call in mock_import_string.call_args_list]
            assert any("auth.db.Manager" in call for call in calls)
            assert any("executor.db.Manager" in call for call in calls)

    @mock.patch("airflow.executors.executor_loader.ExecutorLoader.get_executor_names")
    def test_rundbmanager_skips_team_validation(self, mock_get_names, session):
        """Test that RunDBManager calls get_executor_names with validate_teams=False."""
        from airflow.utils.db_manager import RunDBManager

        mock_get_names.return_value = []

        manager = RunDBManager()
        assert manager is not None

        mock_get_names.assert_called_once_with(validate_teams=False)
