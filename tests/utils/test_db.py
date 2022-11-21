#
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

import inspect
import io
import re
from contextlib import redirect_stdout
from unittest import mock
from unittest.mock import MagicMock

import pytest
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.runtime.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from sqlalchemy import MetaData

from airflow.exceptions import AirflowException
from airflow.models import Base as airflow_base
from airflow.settings import engine
from airflow.utils.db import (
    check_migrations,
    compare_server_default,
    compare_type,
    create_default_connections,
    downgrade,
    resetdb,
    upgradedb,
)


class TestDb:
    def test_database_schema_and_sqlalchemy_model_are_in_sync(self):
        import airflow.models

        airflow.models.import_all_models()
        all_meta_data = MetaData()
        for (table_name, table) in airflow_base.metadata.tables.items():
            all_meta_data._add_table(table_name, table.schema, table)

        # create diff between database schema and SQLAlchemy model
        mctx = MigrationContext.configure(
            engine.connect(),
            opts={"compare_type": compare_type, "compare_server_default": compare_server_default},
        )
        diff = compare_metadata(mctx, all_meta_data)
        # known diffs to ignore
        ignores = [
            # ignore tables created by celery
            lambda t: (t[0] == "remove_table" and t[1].name == "celery_taskmeta"),
            lambda t: (t[0] == "remove_table" and t[1].name == "celery_tasksetmeta"),
            # ignore indices created by celery
            lambda t: (t[0] == "remove_index" and t[1].name == "task_id"),
            lambda t: (t[0] == "remove_index" and t[1].name == "taskset_id"),
            # from test_security unit test
            lambda t: (t[0] == "remove_table" and t[1].name == "some_model"),
            # MSSQL default tables
            lambda t: (t[0] == "remove_table" and t[1].name == "spt_monitor"),
            lambda t: (t[0] == "remove_table" and t[1].name == "spt_fallback_db"),
            lambda t: (t[0] == "remove_table" and t[1].name == "spt_fallback_usg"),
            lambda t: (t[0] == "remove_table" and t[1].name == "MSreplication_options"),
            lambda t: (t[0] == "remove_table" and t[1].name == "spt_fallback_dev"),
            # MSSQL foreign keys where CASCADE has been removed
            lambda t: (t[0] == "remove_fk" and t[1].name == "task_reschedule_dr_fkey"),
            lambda t: (t[0] == "add_fk" and t[1].name == "task_reschedule_dr_fkey"),
            # Ignore flask-session table/index
            lambda t: (t[0] == "remove_table" and t[1].name == "session"),
            lambda t: (t[0] == "remove_index" and t[1].name == "session_id"),
            # sqlite sequence is used for autoincrementing columns created with `sqlite_autoincrement` option
            lambda t: (t[0] == "remove_table" and t[1].name == "sqlite_sequence"),
        ]

        for ignore in ignores:
            diff = [d for d in diff if not ignore(d)]

        assert not diff, "Database schema and SQLAlchemy model are not in sync: " + str(diff)

    def test_only_single_head_revision_in_migrations(self):
        config = Config()
        config.set_main_option("script_location", "airflow:migrations")
        script = ScriptDirectory.from_config(config)

        from airflow.settings import engine

        with EnvironmentContext(
            config,
            script,
            as_sql=True,
        ) as env:
            env.configure(dialect_name=engine.dialect.name)
            # This will raise if there are multiple heads
            # To resolve, use the command `alembic merge`
            script.get_current_head()

    def test_default_connections_sort(self):
        pattern = re.compile("conn_id=[\"|'](.*?)[\"|']", re.DOTALL)
        source = inspect.getsource(create_default_connections)
        src = pattern.findall(source)
        assert sorted(src) == src

    def test_check_migrations(self):
        # Should run without error. Can't easily test the behaviour, but we can check it works
        check_migrations(0)
        check_migrations(1)

    @mock.patch("alembic.command")
    def test_upgradedb(self, mock_alembic_command):
        upgradedb()
        mock_alembic_command.upgrade.assert_called_once_with(mock.ANY, revision="heads")

    @pytest.mark.parametrize(
        "from_revision, to_revision",
        [("be2bfac3da23", "e959f08ac86c"), ("ccde3e26fe78", "2e42bb497a22")],
    )
    def test_offline_upgrade_wrong_order(self, from_revision, to_revision):
        with mock.patch("airflow.utils.db.settings.engine.dialect"):
            with mock.patch("alembic.command.upgrade"):
                with pytest.raises(ValueError, match="to.* revision .* older than .*from"):
                    upgradedb(from_revision=from_revision, to_revision=to_revision, show_sql_only=True)

    @pytest.mark.parametrize(
        "to_revision, from_revision",
        [
            ("e959f08ac86c", "e959f08ac86c"),
        ],
    )
    def test_offline_upgrade_revision_nothing(self, from_revision, to_revision):
        with mock.patch("airflow.utils.db.settings.engine.dialect"):
            with mock.patch("alembic.command.upgrade"):
                with redirect_stdout(io.StringIO()) as temp_stdout:
                    upgradedb(to_revision=to_revision, from_revision=from_revision, show_sql_only=True)
                stdout = temp_stdout.getvalue()
                assert "nothing to do" in stdout

    @pytest.mark.parametrize(
        "from_revision, to_revision",
        [("90d1635d7b86", "54bebd308c5f"), ("e959f08ac86c", "587bdf053233")],
    )
    def test_offline_upgrade_revision(self, from_revision, to_revision):
        with mock.patch("airflow.utils.db.settings.engine.dialect"):
            with mock.patch("alembic.command.upgrade") as mock_alembic_upgrade:
                upgradedb(from_revision=from_revision, to_revision=to_revision, show_sql_only=True)
        mock_alembic_upgrade.assert_called_once_with(mock.ANY, f"{from_revision}:{to_revision}", sql=True)

    @mock.patch("airflow.utils.db._offline_migration")
    @mock.patch("airflow.utils.db._get_current_revision")
    def test_offline_upgrade_no_versions(self, mock_gcr, mock_om):
        """Offline upgrade should work with no version / revision options."""
        with mock.patch("airflow.utils.db.settings.engine.dialect") as dialect:
            dialect.name = "postgresql"  # offline migration not supported with postgres
            mock_gcr.return_value = "90d1635d7b86"
            upgradedb(from_revision=None, to_revision=None, show_sql_only=True)
            actual = mock_om.call_args[0][2]
            assert re.match(r"90d1635d7b86:[a-z0-9]+", actual) is not None

    def test_offline_upgrade_fails_for_migration_less_than_2_0_0_head(self):
        with mock.patch("airflow.utils.db.settings.engine.dialect"):
            with pytest.raises(ValueError, match="Check that e1a11ece99cc is a valid revision"):
                upgradedb(from_revision="e1a11ece99cc", to_revision="54bebd308c5f", show_sql_only=True)

    def test_sqlite_offline_upgrade_raises_with_revision(self):
        with mock.patch("airflow.utils.db.settings.engine.dialect") as dialect:
            dialect.name = "sqlite"
            with pytest.raises(AirflowException, match="Offline migration not supported for SQLite"):
                upgradedb(from_revision="e1a11ece99cc", to_revision="54bebd308c5f", show_sql_only=True)

    def test_offline_upgrade_fails_for_migration_less_than_2_2_0_head_for_mssql(self):
        with mock.patch("airflow.utils.db.settings.engine.dialect") as dialect:
            dialect.name = "mssql"
            with pytest.raises(ValueError, match="Check that .* is a valid .* For dialect 'mssql'"):
                upgradedb(from_revision="e1a11ece99cc", to_revision="54bebd308c5f", show_sql_only=True)

    @mock.patch("airflow.utils.db._offline_migration")
    def test_downgrade_sql_no_from(self, mock_om):
        downgrade(to_revision="abc", show_sql_only=True, from_revision=None)
        actual = mock_om.call_args[1]["revision"]
        assert re.match(r"[a-z0-9]+:abc", actual) is not None

    @mock.patch("airflow.utils.db._offline_migration")
    def test_downgrade_sql_with_from(self, mock_om):
        downgrade(to_revision="abc", show_sql_only=True, from_revision="123")
        actual = mock_om.call_args[1]["revision"]
        assert actual == "123:abc"

    @mock.patch("alembic.command.downgrade")
    def test_downgrade_invalid_combo(self, mock_om):
        """can't combine `sql=False` and `from_revision`"""
        with pytest.raises(ValueError, match="can't be combined"):
            downgrade(to_revision="abc", from_revision="123")

    @mock.patch("alembic.command.downgrade")
    def test_downgrade_with_from(self, mock_om):
        downgrade(to_revision="abc")
        actual = mock_om.call_args[1]["revision"]
        assert actual == "abc"

    @pytest.mark.parametrize("skip_init", [False, True])
    @mock.patch("airflow.utils.db.create_global_lock", new=MagicMock)
    @mock.patch("airflow.utils.db.drop_airflow_models")
    @mock.patch("airflow.utils.db.drop_flask_models")
    @mock.patch("airflow.utils.db.drop_airflow_moved_tables")
    @mock.patch("airflow.utils.db.initdb")
    @mock.patch("airflow.settings.engine.connect")
    def test_resetdb(
        self,
        mock_connect,
        mock_init,
        mock_drop_moved,
        mock_drop_flask,
        mock_drop_airflow,
        skip_init,
    ):
        session_mock = MagicMock()
        resetdb(session_mock, skip_init=skip_init)
        mock_drop_airflow.assert_called_once_with(mock_connect.return_value)
        mock_drop_flask.assert_called_once_with(mock_connect.return_value)
        mock_drop_moved.assert_called_once_with(session_mock)
        if skip_init:
            mock_init.assert_not_called()
        else:
            mock_init.assert_called_once_with(session=session_mock)
