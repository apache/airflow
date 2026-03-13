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
