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
    def test_fab_db_manager_is_default(self):
        from airflow.providers.fab.auth_manager.models.db import FABDBManager

        run_db_manager = RunDBManager()
        assert run_db_manager._managers == [FABDBManager]

    def test_defining_table_same_name_as_airflow_table_name_raises(self):
        from sqlalchemy import Column, Integer, String

        run_db_manager = RunDBManager()
        manager = run_db_manager._managers[0]
        # Add dag_run table to metadata
        mytable = Table(
            "dag_run", manager.metadata, Column("id", Integer, primary_key=True), Column("name", String(50))
        )
        manager.metadata._add_table("dag_run", None, mytable)
        with pytest.raises(AirflowException, match="Table 'dag_run' already exists in the Airflow metadata"):
            run_db_manager.validate()

    @mock.patch("airflow.utils.db_manager.RunDBManager")
    def test_init_db_calls_rundbmanager(self, mock_rundbmanager, session):
        initdb(session=session)
        mock_rundbmanager.return_value.initdb.assert_called_once_with(session)
        mock_rundbmanager.return_value.upgradedb.assert_not_called()
        mock_rundbmanager.return_value.downgrade.assert_not_called()

    @mock.patch("airflow.utils.db_manager.RunDBManager")
    @mock.patch("alembic.command")
    def test_upgradedb_or_downgrade_dont_call_rundbmanager(
        self, mock_alembic_command, mock_rundbmanager, session
    ):
        upgradedb(session=session)
        mock_alembic_command.upgrade.assert_called_once_with(mock.ANY, revision="heads")
        downgrade(to_revision="base")
        mock_alembic_command.downgrade.assert_called_once_with(mock.ANY, revision="base", sql=False)
        mock_rundbmanager.return_value.initdb.assert_not_called()
        mock_rundbmanager.return_value.upgradedb.assert_not_called()
        mock_rundbmanager.return_value.downgrade.assert_not_called()

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


def test_subclassing_db_manager_with_missing_attrs():
    """Test subclassing BaseDBManager."""

    with pytest.raises(AttributeError, match="SubclassDBManager is missing required attribute: metadata"):

        class SubclassDBManager(BaseDBManager): ...


def test_subclassing_db_manager_with_set_metadata():
    with pytest.raises(
        AttributeError, match="SubclassDbManager is missing required attribute: migration_dir"
    ):

        class SubclassDbManager(BaseDBManager):
            metadata = Base.metadata


def test_subclassing_db_manager_with_set_metadata_and_migration_dir():
    with pytest.raises(AttributeError, match="SubclassDbManager is missing required attribute: alembic_file"):

        class SubclassDbManager(BaseDBManager):
            metadata = Base.metadata
            migration_dir = "some_dir"


def test_subclassing_db_manager_with_attrs_set_except_version_table_name():
    with pytest.raises(
        AttributeError, match="SubclassDbManager is missing required attribute: version_table_name"
    ):

        class SubclassDbManager(BaseDBManager):
            metadata = Base.metadata
            migration_dir = "some_dir"
            alembic_file = "some_file"


def test_subclassing_db_manager_with_attrs_set_dont_raise(session):
    class SubclassDbManager(BaseDBManager):
        metadata = Base.metadata
        migration_dir = "some_dir"
        alembic_file = "some_file"
        version_table_name = "some_table"
