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

import os
from unittest import mock

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import MetaData

import airflow
from airflow.settings import engine
from airflow.utils.db import (
    compare_server_default,
    compare_type,
)

pytestmark = [pytest.mark.db_test]
try:
    from airflow.providers.fab.auth_manager.models.db import FABDBManager

    class TestFABDBManager:
        def setup_method(self):
            self.airflow_dir = os.path.dirname(airflow.__file__)

        def test_version_table_name_set(self, session):
            assert FABDBManager(session=session).version_table_name == "fab_alembic_version"

        def test_migration_dir_set(self, session):
            assert (
                FABDBManager(session=session).migration_dir == f"{self.airflow_dir}/providers/fab/migrations"
            )

        def test_alembic_file_set(self, session):
            assert (
                FABDBManager(session=session).alembic_file == f"{self.airflow_dir}/providers/fab/alembic.ini"
            )

        def test_supports_table_dropping_set(self, session):
            assert FABDBManager(session=session).supports_table_dropping is True

        def test_database_schema_and_sqlalchemy_model_are_in_sync(self, session):
            def include_object(_, name, type_, *args):
                if type_ == "table" and name not in FABDBManager(session=session).metadata.tables:
                    return False
                return True

            all_meta_data = MetaData()
            for table_name, table in FABDBManager(session=session).metadata.tables.items():
                all_meta_data._add_table(table_name, table.schema, table)
            # create diff between database schema and SQLAlchemy model
            mctx = MigrationContext.configure(
                engine.connect(),
                opts={
                    "compare_type": compare_type,
                    "compare_server_default": compare_server_default,
                    "include_object": include_object,
                },
            )
            diff = compare_metadata(mctx, all_meta_data)

            assert not diff, "Database schema and SQLAlchemy model are not in sync: " + str(diff)

        @mock.patch("airflow.providers.fab.auth_manager.models.db._offline_migration")
        def test_downgrade_sql_no_from(self, mock_om, session, caplog):
            FABDBManager(session=session).downgrade(to_revision="abc", show_sql_only=True, from_revision=None)
            # TODO: When we have a migration, uncomment the following line and remove the last
            # actual = mock_om.call_args.kwargs["revision"]
            # assert re.match(r"[a-z0-9]+:abc", actual) is not None
            assert "No revision found" in caplog.text

        @mock.patch("airflow.providers.fab.auth_manager.models.db._offline_migration")
        def test_downgrade_sql_with_from(self, mock_om, session):
            FABDBManager(session=session).downgrade(
                to_revision="abc", show_sql_only=True, from_revision="123"
            )
            actual = mock_om.call_args.kwargs["revision"]
            assert actual == "123:abc"

        @mock.patch("alembic.command.downgrade")
        def test_downgrade_invalid_combo(self, mock_om, session):
            """can't combine `sql=False` and `from_revision`"""
            with pytest.raises(ValueError, match="can't be combined"):
                FABDBManager(session=session).downgrade(to_revision="abc", from_revision="123")

        @mock.patch("alembic.command.downgrade")
        def test_downgrade_with_from(self, mock_om, session):
            FABDBManager(session=session).downgrade(to_revision="abc")
            actual = mock_om.call_args.kwargs["revision"]
            assert actual == "abc"

        @mock.patch.object(FABDBManager, "get_current_revision")
        def test_sqlite_offline_upgrade_raises_with_revision(self, mock_gcr, session):
            with mock.patch(
                "airflow.providers.fab.auth_manager.models.db.settings.engine.dialect"
            ) as dialect:
                dialect.name = "sqlite"
                with pytest.raises(SystemExit, match="Offline migration not supported for SQLite"):
                    FABDBManager(session).upgradedb(from_revision=None, to_revision=None, show_sql_only=True)
except ModuleNotFoundError:
    pass
