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
from airflow.utils.db import downgrade, initdb, upgradedb
from airflow.utils.db_manager import BaseDBManager, RunDBManager

pytestmark = [pytest.mark.db_test]


class TestRunDBManager:
    def setup_method(self):
        self.run_db_manager = RunDBManager()
        self.manager = self.run_db_manager._managers[0]
        self.metadata = self.manager.metadata

    def test_fab_db_manager_is_default(self):
        from airflow.providers.fab.auth_manager.models.db import FABDBManager

        assert self.run_db_manager._managers == [FABDBManager]

    def test_defining_table_same_name_as_airflow_table_name_raises(self):
        from sqlalchemy import Column, Integer, String

        # Add dag_run table to metadata
        mytable = Table(
            "dag_run", self.metadata, Column("id", Integer, primary_key=True), Column("name", String(50))
        )
        self.metadata._add_table("dag_run", None, mytable)
        with pytest.raises(AirflowException, match="Table 'dag_run' already exists in the Airflow metadata"):
            self.run_db_manager.validate()
        self.metadata._remove_table("dag_run", schema=None)

    @mock.patch.object(RunDBManager, "downgradedb")
    @mock.patch.object(RunDBManager, "upgradedb")
    @mock.patch.object(RunDBManager, "initdb")
    def test_init_db_calls_rundbmanager(self, mock_initdb, mock_upgrade_db, mock_downgrade_db, session):
        initdb(session=session)
        mock_initdb.assert_called()
        mock_initdb.assert_called_once_with(session)
        mock_upgrade_db.assert_not_called()
        mock_downgrade_db.assert_not_called()

    @mock.patch.object(RunDBManager, "downgradedb")
    @mock.patch.object(RunDBManager, "upgradedb")
    @mock.patch.object(RunDBManager, "initdb")
    @mock.patch("alembic.command")
    def test_upgradedb_or_downgrade_dont_call_rundbmanager(
        self, mock_alembic_command, mock_initdb, mock_upgrade_db, mock_downgrade_db, session
    ):
        upgradedb(session=session)
        mock_alembic_command.upgrade.assert_called_once_with(mock.ANY, revision="heads")
        downgrade(to_revision="base")
        mock_alembic_command.downgrade.assert_called_once_with(mock.ANY, revision="base", sql=False)
        mock_initdb.assert_not_called()
        mock_upgrade_db.assert_not_called()
        mock_downgrade_db.assert_not_called()

    @mock.patch("airflow.providers.fab.auth_manager.models.db.FABDBManager")
    def test_rundbmanager_calls_dbmanager_methods(self, mock_fabdb_manager, session):
        ext_db = RunDBManager()
        # initdb
        ext_db.initdb(session=session)
        mock_fabdb_manager.return_value.initdb.assert_called_once()
        # upgradedb
        ext_db.upgradedb(session=session)
        mock_fabdb_manager.return_value.upgradedb.assert_called_once()
        # downgradedb
        ext_db.downgradedb(session=session)
        mock_fabdb_manager.return_value.downgradedb.assert_called_once()


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
    def test_upgradedb(self, mock_alembic_cmd, mock_alembic_config, session, caplog):
        manager = MockDBManager(session)
        manager.upgradedb()
        mock_alembic_cmd.assert_called_once()
        assert "Upgrading the MockDBManager database" in caplog.text
