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

from airflow.models import Base
from airflow.utils.db_manager import BaseDBManager, RunDBManager

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


class LegacySignatureExternalManager:
    initdb_calls = 0
    upgradedb_calls = 0

    def __init__(self, _session):
        pass

    def initdb(self):
        type(self).initdb_calls += 1

    def upgradedb(self):
        type(self).upgradedb_calls += 1


class ExplicitKwargExternalManager:
    initdb_kwargs: list[bool] = []
    upgradedb_kwargs: list[bool] = []

    def __init__(self, _session):
        pass

    def initdb(self, use_migration_files=False):
        type(self).initdb_kwargs.append(use_migration_files)

    def upgradedb(self, use_migration_files=False):
        type(self).upgradedb_kwargs.append(use_migration_files)


class VarKwargExternalManager:
    initdb_kwargs: list[dict] = []
    upgradedb_kwargs: list[dict] = []

    def __init__(self, _session):
        pass

    def initdb(self, **kwargs):
        type(self).initdb_kwargs.append(kwargs)

    def upgradedb(self, **kwargs):
        type(self).upgradedb_kwargs.append(kwargs)


def _create_run_db_manager(*managers):
    run_db_manager = RunDBManager.__new__(RunDBManager)
    run_db_manager._managers = list(managers)
    return run_db_manager


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

    @mock.patch.object(BaseDBManager, "upgradedb")
    @mock.patch.object(BaseDBManager, "get_current_revision")
    def test_initdb_use_migration_files_calls_upgrade(self, mock_current_revision, mock_upgradedb, session):
        mock_current_revision.return_value = None

        manager = MockDBManager(session)
        manager.initdb(use_migration_files=True)

        mock_upgradedb.assert_called_once_with(use_migration_files=True)

    @mock.patch.object(BaseDBManager, "get_alembic_config")
    @mock.patch("alembic.command.upgrade")
    @mock.patch.object(BaseDBManager, "get_current_revision")
    def test_upgrade(self, mock_current_revision, mock_alembic_cmd, mock_alembic_config, session, caplog):
        mock_current_revision.return_value = "current-revision"
        manager = MockDBManager(session)
        manager.upgradedb()
        mock_alembic_cmd.assert_called_once()
        assert "Upgrading the MockDBManager database" in caplog.text

    @mock.patch.object(BaseDBManager, "create_db_from_orm")
    @mock.patch.object(BaseDBManager, "get_current_revision")
    def test_upgrade_empty_db_without_migration_files_uses_create_db_from_orm(
        self, mock_current_revision, mock_create_db_from_orm, session
    ):
        mock_current_revision.return_value = None
        manager = MockDBManager(session)
        manager.upgradedb()
        mock_create_db_from_orm.assert_called_once()

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

    @mock.patch("airflow.utils.db.create_global_lock", return_value=nullcontext())
    @mock.patch.object(MockDBManager, "drop_tables")
    @mock.patch("airflow.utils.db_manager.settings.engine.connect")
    def test_resetdb_supports_legacy_initdb_override(
        self, mock_connect, mock_drop_tables, mock_create_global_lock, session
    ):
        class LegacyInitOverrideManager(MockDBManager):
            initdb_calls = 0

            def initdb(self):
                type(self).initdb_calls += 1

        LegacyInitOverrideManager.initdb_calls = 0
        mock_connect.return_value.begin.return_value = nullcontext()

        manager = LegacyInitOverrideManager(session)
        manager.resetdb(use_migration_files=True)

        assert LegacyInitOverrideManager.initdb_calls == 1
        mock_drop_tables.assert_called_once()

    @mock.patch.object(MockDBManager, "create_db_from_orm")
    @mock.patch.object(MockDBManager, "get_current_revision", return_value=None)
    def test_initdb_supports_legacy_upgradedb_override(
        self, mock_get_current_revision, mock_create_db_from_orm, session
    ):
        class LegacyUpgradeOverrideManager(MockDBManager):
            upgradedb_calls = 0

            def upgradedb(self, to_revision=None, from_revision=None, show_sql_only=False):
                type(self).upgradedb_calls += 1

        LegacyUpgradeOverrideManager.upgradedb_calls = 0

        manager = LegacyUpgradeOverrideManager(session)
        manager.initdb(use_migration_files=True)

        assert LegacyUpgradeOverrideManager.upgradedb_calls == 1
        mock_create_db_from_orm.assert_not_called()
        mock_get_current_revision.assert_called_once()


class TestRunDBManager:
    def test_initdb_and_upgradedb_support_legacy_manager_signatures(self, session):
        LegacySignatureExternalManager.initdb_calls = 0
        LegacySignatureExternalManager.upgradedb_calls = 0

        run_db_manager = _create_run_db_manager(LegacySignatureExternalManager)
        run_db_manager.initdb(session=session, use_migration_files=True)
        run_db_manager.upgradedb(session=session, use_migration_files=True)

        assert LegacySignatureExternalManager.initdb_calls == 1
        assert LegacySignatureExternalManager.upgradedb_calls == 1

    def test_initdb_and_upgradedb_pass_use_migration_files_to_explicit_kwarg_manager(self, session):
        ExplicitKwargExternalManager.initdb_kwargs = []
        ExplicitKwargExternalManager.upgradedb_kwargs = []

        run_db_manager = _create_run_db_manager(ExplicitKwargExternalManager)
        run_db_manager.initdb(session=session, use_migration_files=True)
        run_db_manager.upgradedb(session=session, use_migration_files=False)

        assert ExplicitKwargExternalManager.initdb_kwargs == [True]
        assert ExplicitKwargExternalManager.upgradedb_kwargs == [False]

    def test_initdb_and_upgradedb_pass_use_migration_files_to_var_kwarg_manager(self, session):
        VarKwargExternalManager.initdb_kwargs = []
        VarKwargExternalManager.upgradedb_kwargs = []
        run_db_manager = _create_run_db_manager(VarKwargExternalManager)
        run_db_manager.initdb(session=session, use_migration_files=True)
        run_db_manager.upgradedb(session=session, use_migration_files=False)

        assert VarKwargExternalManager.initdb_kwargs == [{"use_migration_files": True}]
        assert VarKwargExternalManager.upgradedb_kwargs == [{"use_migration_files": False}]

    @mock.patch("airflow.utils.db_manager.import_string")
    def test_run_db_manager_uses_providers_manager_when_auth_manager_fails(self, mock_import):
        """
        When create_auth_manager() raises in a migration-only context (e.g. the Helm
        migrateDatabaseJob), RunDBManager must still load managers discovered via
        ProvidersManager — the exception must not silently drop all DB managers.
        """
        sentinel = object()
        mock_import.return_value = sentinel

        with (
            mock.patch(
                "airflow.providers_manager.ProvidersManager",
            ) as mock_pm,
            mock.patch(
                "airflow.api_fastapi.app.create_auth_manager",
                side_effect=RuntimeError("No app context"),
            ),
        ):
            mock_pm.return_value.db_managers = ["airflow.providers.fab.auth_manager.models.db.FABDBManager"]
            with mock.patch("airflow.utils.db_manager.conf") as mock_conf:
                mock_conf.get.return_value = None
                rdm = RunDBManager.__new__(RunDBManager)
                # Manually call __init__ so we control all side-effects
                RunDBManager.__init__(rdm)

        # The sentinel class returned by import_string must be in _managers
        assert sentinel in rdm._managers

    @mock.patch("airflow.utils.db_manager.import_string")
    def test_run_db_manager_includes_auth_manager_db_manager_when_available(self, mock_import):
        """
        When create_auth_manager() succeeds and returns a DB manager class name not
        already in the ProvidersManager list, it must be appended to _managers.
        """
        sentinel_pm = object()
        sentinel_am = object()
        call_order = []

        def _import(path):
            if "fab" in path:
                call_order.append("fab")
                return sentinel_pm
            call_order.append("auth_manager_extra")
            return sentinel_am

        mock_import.side_effect = _import

        mock_am = mock.MagicMock()
        mock_am.get_db_manager.return_value = "some.extra.AuthManagerDBManager"

        with (
            mock.patch("airflow.providers_manager.ProvidersManager") as mock_pm,
            mock.patch(
                "airflow.api_fastapi.app.create_auth_manager",
                return_value=mock_am,
            ),
        ):
            mock_pm.return_value.db_managers = ["airflow.providers.fab.auth_manager.models.db.FABDBManager"]
            with mock.patch("airflow.utils.db_manager.conf") as mock_conf:
                mock_conf.get.return_value = None
                rdm = RunDBManager.__new__(RunDBManager)
                RunDBManager.__init__(rdm)

        assert sentinel_pm in rdm._managers
        assert sentinel_am in rdm._managers
