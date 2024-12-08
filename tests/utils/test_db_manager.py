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
from sqlalchemy import Table

from airflow.exceptions import AirflowException
from airflow.models import Base
from airflow.utils.db import downgrade, initdb
from airflow.utils.db_manager import BaseDBManager, RunDBManager

from tests_common.test_utils.config import conf_vars

pytestmark = [pytest.mark.db_test]


class TestRunDBManager:
    @conf_vars(
        {("database", "external_db_managers"): "airflow.providers.fab.auth_manager.models.db.FABDBManager"}
    )
    def test_fab_db_manager_is_default(self):
        from airflow.providers.fab.auth_manager.models.db import FABDBManager

        run_db_manager = RunDBManager()
        assert run_db_manager._managers == [FABDBManager]

    @conf_vars(
        {("database", "external_db_managers"): "airflow.providers.fab.auth_manager.models.db.FABDBManager"}
    )
    def test_defining_table_same_name_as_airflow_table_name_raises(self):
        from sqlalchemy import Column, Integer, String

        run_db_manager = RunDBManager()
        metadata = run_db_manager._managers[0].metadata
        # Add dag_run table to metadata
        mytable = Table(
            "dag_run", metadata, Column("id", Integer, primary_key=True), Column("name", String(50))
        )
        metadata._add_table("dag_run", None, mytable)
        with pytest.raises(AirflowException, match="Table 'dag_run' already exists in the Airflow metadata"):
            run_db_manager.validate()
        metadata._remove_table("dag_run", None)

    @mock.patch.object(RunDBManager, "downgrade")
    @mock.patch.object(RunDBManager, "upgradedb")
    @mock.patch.object(RunDBManager, "initdb")
    def test_init_db_calls_rundbmanager(self, mock_initdb, mock_upgrade_db, mock_downgrade_db, session):
        initdb(session=session)
        mock_initdb.assert_called()
        mock_initdb.assert_called_once_with(session)
        mock_downgrade_db.assert_not_called()

    @mock.patch.object(RunDBManager, "downgrade")
    @mock.patch.object(RunDBManager, "upgradedb")
    @mock.patch.object(RunDBManager, "initdb")
    @mock.patch("alembic.command")
    def test_downgrade_dont_call_rundbmanager(
        self, mock_alembic_command, mock_initdb, mock_upgrade_db, mock_downgrade_db, session
    ):
        downgrade(to_revision="base")
        mock_alembic_command.downgrade.assert_called_once_with(mock.ANY, revision="base", sql=False)
        mock_upgrade_db.assert_not_called()
        mock_initdb.assert_not_called()
        mock_downgrade_db.assert_not_called()

    @conf_vars(
        {("database", "external_db_managers"): "airflow.providers.fab.auth_manager.models.db.FABDBManager"}
    )
    @mock.patch("airflow.providers.fab.auth_manager.models.db.FABDBManager")
    def test_rundbmanager_calls_dbmanager_methods(self, mock_fabdb_manager, session):
        mock_fabdb_manager.supports_table_dropping = True
        fabdb_manager = mock_fabdb_manager.return_value
        ext_db = RunDBManager()
        # initdb
        ext_db.initdb(session=session)
        fabdb_manager.initdb.assert_called_once()
        # upgradedb
        ext_db.upgradedb(session=session)
        fabdb_manager.upgradedb.assert_called_once()
        # downgrade
        ext_db.downgrade(session=session)
        mock_fabdb_manager.return_value.downgrade.assert_called_once()
        connection = mock.MagicMock()
        ext_db.drop_tables(session, connection)
        mock_fabdb_manager.return_value.drop_tables.assert_called_once_with(connection)


class MockDBManager(BaseDBManager):
    metadata = Base.metadata
    version_table_name = "mock_alembic_version"
    migration_dir = "mock_migration_dir"
    alembic_file = "mock_alembic.ini"
    supports_table_dropping = True


class TestBaseDBManager:
    @mock.patch.object(BaseDBManager, "get_alembic_config")
    @mock.patch.object(BaseDBManager, "get_current_revision")
    @mock.patch.object(BaseDBManager, "_create_db_from_orm")
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
