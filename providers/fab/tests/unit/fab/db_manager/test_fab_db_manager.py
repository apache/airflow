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

from airflow.providers.common.compat.sdk import AirflowException
from airflow.utils.db import initdb
from airflow.utils.db_manager import RunDBManager

from tests_common.test_utils.config import conf_vars

pytestmark = [pytest.mark.db_test]


class TestRunDBManagerWithFab:
    @conf_vars(
        {("database", "external_db_managers"): "airflow.providers.fab.auth_manager.models.db.FABDBManager"}
    )
    def test_db_manager_uses_config(self):
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

    @mock.patch.object(RunDBManager, "upgradedb")
    @mock.patch.object(RunDBManager, "initdb")
    def test_init_db_calls_rundbmanager(self, mock_initdb, mock_upgrade_db, session):
        initdb(session=session)
        mock_initdb.assert_called()
        mock_initdb.assert_called_once_with(session)

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
        # drop_tables
        connection = mock.MagicMock()
        ext_db.drop_tables(session, connection)
        mock_fabdb_manager.return_value.drop_tables.assert_called_once_with(connection)
