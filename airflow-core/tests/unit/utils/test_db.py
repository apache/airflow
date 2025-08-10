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
import logging
import os
import re
from contextlib import redirect_stdout
from io import StringIO
from unittest import mock

import pytest
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.runtime.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from sqlalchemy import Column, Integer, MetaData, Table, select

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.models import Base as airflow_base
from airflow.utils.db import (
    _REVISION_HEADS_MAP,
    AutocommitEngineForMySQL,
    LazySelectSequence,
    _get_alembic_config,
    check_migrations,
    compare_server_default,
    compare_type,
    create_default_connections,
    downgrade,
    resetdb,
    upgradedb,
)
from airflow.utils.db_manager import RunDBManager

from tests_common.test_utils.config import conf_vars
from unit.cli.commands.test_kerberos_command import PY313

pytestmark = pytest.mark.db_test


class TestDb:
    def test_database_schema_and_sqlalchemy_model_are_in_sync(self):
        import airflow.models

        airflow.models.import_all_models()
        all_meta_data = MetaData()
        # Airflow DB
        for table_name, table in airflow_base.metadata.tables.items():
            all_meta_data._add_table(table_name, table.schema, table)
        # External DB Managers
        external_db_managers = RunDBManager()
        for dbmanager in external_db_managers._managers:
            for table_name, table in dbmanager.metadata.tables.items():
                all_meta_data._add_table(table_name, table.schema, table)
        skip_fab = PY313
        if not skip_fab:
            # FAB DB Manager
            from airflow.providers.fab.auth_manager.models.db import FABDBManager

            # test FAB models
            for table_name, table in FABDBManager.metadata.tables.items():
                all_meta_data._add_table(table_name, table.schema, table)
        else:
            print("Ignoring FAB models in Python 3.13+ as FAB is not compatible with 3.13+ yet.")
        # create diff between database schema and SQLAlchemy model
        mctx = MigrationContext.configure(
            settings.engine.connect(),
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
            # Ignore flask-session table/index
            lambda t: (t[0] == "remove_table" and t[1].name == "session"),
            lambda t: (t[0] == "remove_index" and t[1].name == "session_id"),
            lambda t: (t[0] == "remove_index" and t[1].name == "session_session_id_uq"),
            # sqlite sequence is used for autoincrementing columns created with `sqlite_autoincrement` option
            lambda t: (t[0] == "remove_table" and t[1].name == "sqlite_sequence"),
            # fab version table
            lambda t: (t[0] == "remove_table" and t[1].name == "alembic_version_fab"),
            # Ignore _xcom_archive table
            lambda t: (t[0] == "remove_table" and t[1].name == "_xcom_archive"),
        ]

        if skip_fab:
            ignores.append(lambda t: (t[1].name.startswith("ab_")))
            ignores.append(
                lambda t: (t[0] == "remove_index" and t[1].columns[0].table.name.startswith("ab_"))
            )

        for ignore in ignores:
            diff = [d for d in diff if not ignore(d)]
        if diff:
            print("Database schema and SQLAlchemy model are not in sync: ")
            for single_diff in diff:
                print(f"Diff: {single_diff}")
            pytest.fail("Database schema and SQLAlchemy model are not in sync")

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

    @pytest.mark.parametrize(
        "auth, expected",
        [
            (
                {
                    (
                        "core",
                        "auth_manager",
                    ): "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
                },
                1,
            ),
            (
                {
                    (
                        "core",
                        "auth_manager",
                    ): "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
                },
                2,
            ),
        ],
    )
    @mock.patch("alembic.command")
    def test_upgradedb(self, mock_alembic_command, auth, expected):
        if PY313 and "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager" in str(auth):
            pytest.skip(
                "Skipping test for FAB Auth Manager on Python 3.13+ as FAB is not compatible with 3.13+ yet."
            )
        with conf_vars(auth):
            upgradedb()
            mock_alembic_command.upgrade.assert_called_with(mock.ANY, revision="heads")
            assert mock_alembic_command.upgrade.call_count == expected

    @pytest.mark.parametrize(
        "from_revision, to_revision",
        [("be2bfac3da23", "e959f08ac86c"), ("ccde3e26fe78", "2e42bb497a22")],
    )
    def test_offline_upgrade_wrong_order(self, from_revision, to_revision):
        with mock.patch("airflow.utils.db.settings.engine.dialect"):
            with mock.patch("alembic.command.upgrade"):
                with pytest.raises(ValueError, match="Error while checking history for revision range *:*"):
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
                with redirect_stdout(StringIO()) as temp_stdout:
                    upgradedb(to_revision=to_revision, from_revision=from_revision, show_sql_only=True)
                stdout = temp_stdout.getvalue()
                assert "nothing to do" in stdout

    @mock.patch("airflow.utils.db._offline_migration")
    @mock.patch("airflow.utils.db._get_current_revision")
    def test_offline_upgrade_no_versions(self, mock_gcr, mock_om, caplog):
        """Offline upgrade should work with no version / revision options."""
        with mock.patch("airflow.utils.db.settings.engine.dialect") as dialect:
            dialect.name = "postgresql"  # offline migration supported with postgres
            mock_gcr.return_value = "22ed7efa9da2"
            upgradedb(from_revision=None, to_revision=None, show_sql_only=True)
            actual = mock_om.call_args.args[2]
            assert re.match(r"22ed7efa9da2:[a-z0-9]+", actual) is not None

    @mock.patch("airflow.utils.db._get_current_revision")
    def test_sqlite_offline_upgrade_raises_with_revision(self, mock_gcr):
        with mock.patch("airflow.utils.db.settings.engine.dialect") as dialect:
            dialect.name = "sqlite"
            with pytest.raises(SystemExit, match="Offline migration not supported for SQLite"):
                upgradedb(from_revision=None, to_revision=None, show_sql_only=True)

    @mock.patch("airflow.utils.db._offline_migration")
    def test_downgrade_sql_no_from(self, mock_om):
        downgrade(to_revision="abc", show_sql_only=True, from_revision=None)
        actual = mock_om.call_args.kwargs["revision"]
        assert re.match(r"[a-z0-9]+:abc", actual) is not None

    @mock.patch("airflow.utils.db._offline_migration")
    def test_downgrade_sql_with_from(self, mock_om):
        downgrade(to_revision="abc", show_sql_only=True, from_revision="123")
        actual = mock_om.call_args.kwargs["revision"]
        assert actual == "123:abc"

    @mock.patch("alembic.command.downgrade")
    def test_downgrade_invalid_combo(self, mock_om):
        """Can't combine `sql=False` and `from_revision`"""
        with pytest.raises(ValueError, match="can't be combined"):
            downgrade(to_revision="abc", from_revision="123")

    @mock.patch("alembic.command.downgrade")
    def test_downgrade_with_from(self, mock_om):
        downgrade(to_revision="abc")
        actual = mock_om.call_args.kwargs["revision"]
        assert actual == "abc"

    def test_resetdb_logging_level(self):
        unset_logging_level = logging.root.level
        logging.root.setLevel(logging.DEBUG)
        set_logging_level = logging.root.level
        resetdb()
        assert logging.root.level == set_logging_level
        assert logging.root.level != unset_logging_level

    def test_alembic_configuration(self):
        with mock.patch.dict(
            os.environ, {"AIRFLOW__DATABASE__ALEMBIC_INI_FILE_PATH": "/tmp/alembic.ini"}, clear=True
        ):
            config = _get_alembic_config()
            assert config.config_file_name == "/tmp/alembic.ini"

        # default behaviour
        config = _get_alembic_config()
        import airflow

        assert config.config_file_name == os.path.join(os.path.dirname(airflow.__file__), "alembic.ini")

    def test_bool_lazy_select_sequence(self):
        class MockSession:
            def __init__(self):
                pass

            def scalar(self, stmt):
                return None

        t = Table("t", MetaData(), Column("id", Integer, primary_key=True))
        lss = LazySelectSequence.from_select(select(t.c.id), order_by=[], session=MockSession())

        assert bool(lss) is False

    @conf_vars({("core", "unit_test_mode"): "False"})
    @mock.patch("airflow.utils.db.inspect")
    def test_downgrade_raises_if_lower_than_v3_0_0_and_no_ab_user(self, mock_inspect):
        mock_inspect.return_value.has_table.return_value = False
        msg = (
            "Downgrade to revision less than 3.0.0 requires that `ab_user` table is present. "
            "Please add FabDBManager to [core] external_db_managers and run fab migrations before "
            "proceeding"
        )
        with pytest.raises(AirflowException, match=re.escape(msg)):
            downgrade(to_revision=_REVISION_HEADS_MAP["2.7.0"])


class TestAutocommitEngineForMySQL:
    """Test the AutocommitEngineForMySQL context manager."""

    def test_non_mysql_database_is_noop(self, mocker):
        """Test that non-MySQL databases don't trigger any changes."""
        # Mock settings to use PostgreSQL
        mocker.patch.object(settings, "SQL_ALCHEMY_CONN", "postgresql://user:pass@localhost/db")
        mock_dispose = mocker.patch.object(settings, "dispose_orm")
        mock_configure = mocker.patch.object(settings, "configure_orm")

        original_prepare = settings.prepare_engine_args

        with AutocommitEngineForMySQL() as ctx:
            # Should return self but not modify anything
            assert ctx is not None
            # prepare_engine_args should be unchanged
            assert settings.prepare_engine_args == original_prepare

        # No engine operations should have been called
        mock_dispose.assert_not_called()
        mock_configure.assert_not_called()

        # After exit, should still be unchanged
        assert settings.prepare_engine_args == original_prepare

    def test_mysql_database_modifies_engine(self, mocker):
        """Test that MySQL databases trigger AUTOCOMMIT mode."""
        # Mock settings to use MySQL
        mocker.patch.object(settings, "SQL_ALCHEMY_CONN", "mysql+mysqlconnector://user:pass@localhost/db")
        mock_dispose = mocker.patch.object(settings, "dispose_orm")
        mock_configure = mocker.patch.object(settings, "configure_orm")

        # Create a mock for original prepare_engine_args
        original_prepare = mocker.Mock(return_value={"pool_size": 5})
        mocker.patch.object(settings, "prepare_engine_args", original_prepare)

        with AutocommitEngineForMySQL():
            # Check that prepare_engine_args was replaced
            assert settings.prepare_engine_args != original_prepare

            # Call the new prepare_engine_args and verify it adds AUTOCOMMIT
            result = settings.prepare_engine_args()
            assert result["isolation_level"] == "AUTOCOMMIT"
            assert result["pool_size"] == 5  # Original args preserved

            # Verify engine was recreated on enter
            assert mock_dispose.call_count == 1
            assert mock_configure.call_count == 1

        # After exit, original should be restored
        assert settings.prepare_engine_args == original_prepare

        # Verify engine was recreated on exit
        assert mock_dispose.call_count == 2
        assert mock_configure.call_count == 2

    def test_mysql_variants_detected(self, mocker):
        """Test that different MySQL connection strings are detected."""
        mock_dispose = mocker.patch.object(settings, "dispose_orm")
        mock_configure = mocker.patch.object(settings, "configure_orm")

        mysql_variants = [
            "mysql://user:pass@localhost/db",
            "mysql+pymysql://user:pass@localhost/db",
            "mysql+mysqlconnector://user:pass@localhost/db",
            "mysql+mysqldb://user:pass@localhost/db",
            "MySQL://user:pass@localhost/db",  # Case insensitive
        ]

        for conn_string in mysql_variants:
            mocker.patch.object(settings, "SQL_ALCHEMY_CONN", conn_string)
            mock_dispose.reset_mock()
            mock_configure.reset_mock()

            with AutocommitEngineForMySQL():
                # Should trigger engine recreation for all MySQL variants
                assert mock_dispose.call_count == 1
                assert mock_configure.call_count == 1

            # Cleanup on exit
            assert mock_dispose.call_count == 2
            assert mock_configure.call_count == 2

    def test_none_sql_alchemy_conn(self, mocker):
        """Test behavior when SQL_ALCHEMY_CONN is None."""
        mocker.patch.object(settings, "SQL_ALCHEMY_CONN", None)
        mock_dispose = mocker.patch.object(settings, "dispose_orm")
        mock_configure = mocker.patch.object(settings, "configure_orm")

        with AutocommitEngineForMySQL():
            # Should be a no-op when connection string is None
            mock_dispose.assert_not_called()
            mock_configure.assert_not_called()

    def test_prepare_engine_args_with_parameters(self, mocker):
        """Test that prepare_engine_args parameters are properly forwarded."""
        mocker.patch.object(settings, "SQL_ALCHEMY_CONN", "mysql://user:pass@localhost/db")
        mocker.patch.object(settings, "dispose_orm")
        mocker.patch.object(settings, "configure_orm")

        # Mock original with different behavior based on parameters
        original_prepare = mocker.Mock(
            side_effect=lambda disable_connection_pool=False, pool_class=None: {
                "disabled": disable_connection_pool,
                "pool_class": pool_class,
                "original": True,
            }
        )
        mocker.patch.object(settings, "prepare_engine_args", original_prepare)

        with AutocommitEngineForMySQL():
            # Test with different parameters
            result1 = settings.prepare_engine_args(disable_connection_pool=True)
            assert result1["isolation_level"] == "AUTOCOMMIT"
            assert result1["disabled"] is True
            assert result1["original"] is True

            result2 = settings.prepare_engine_args(pool_class="CustomPool")
            assert result2["isolation_level"] == "AUTOCOMMIT"
            assert result2["pool_class"] == "CustomPool"

            # Verify original was called with correct parameters
            original_prepare.assert_any_call(disable_connection_pool=True, pool_class=None)
            original_prepare.assert_any_call(disable_connection_pool=False, pool_class="CustomPool")

    def test_exception_during_context(self, mocker):
        """Test that cleanup happens even if an exception occurs."""
        mocker.patch.object(settings, "SQL_ALCHEMY_CONN", "mysql://user:pass@localhost/db")
        mock_dispose = mocker.patch.object(settings, "dispose_orm")
        mock_configure = mocker.patch.object(settings, "configure_orm")
        original_prepare = mocker.Mock()
        mocker.patch.object(settings, "prepare_engine_args", original_prepare)

        # Setup and assertions before raising the exception
        with AutocommitEngineForMySQL():
            assert mock_dispose.call_count == 1
            assert mock_configure.call_count == 1
            assert settings.prepare_engine_args != original_prepare

            # Now check that ValueError is raised
            with pytest.raises(ValueError):
                raise ValueError("Test exception")

        # Verify cleanup still happened despite exception
        assert settings.prepare_engine_args == original_prepare
        assert mock_dispose.call_count == 2
        assert mock_configure.call_count == 2

    def test_logging_messages(self, mocker):
        """Test that appropriate log messages are generated."""
        mocker.patch.object(settings, "SQL_ALCHEMY_CONN", "mysql://user:pass@localhost/db")
        mocker.patch.object(settings, "dispose_orm")
        mocker.patch.object(settings, "configure_orm")

        # Mock the log object in the db module
        mock_log = mocker.patch("airflow.utils.db.log")

        with AutocommitEngineForMySQL():
            pass

        # Get the list of calls made to the mock's 'info' method
        info_calls = mock_log.info.mock_calls

        # Assert that each expected call is present in the list of actual calls
        assert mocker.call("Entering AUTOCOMMIT mode for MySQL DDL operations") in info_calls
        assert mocker.call("Exiting AUTOCOMMIT mode, restoring normal transaction engine") in info_calls

    def test_integration_with_actual_settings_module(self, mocker):
        """Test integration using a fully mocked settings module."""
        # Setup mock settings module
        mock_settings = mocker.Mock()
        mock_settings.SQL_ALCHEMY_CONN = "mysql://user:pass@localhost/db"
        mock_settings.dispose_orm = mocker.Mock()
        mock_settings.configure_orm = mocker.Mock()
        original_prepare = mocker.Mock(return_value={"test": "value"})
        mock_settings.prepare_engine_args = original_prepare

        # Import the class with mocked settings
        from airflow.utils.db import AutocommitEngineForMySQL

        mocker.patch("airflow.utils.db.settings", mock_settings)
        with AutocommitEngineForMySQL():
            # Verify the prepare_engine_args was replaced
            assert mock_settings.prepare_engine_args != original_prepare

            # Test the wrapper function
            result = mock_settings.prepare_engine_args()
            assert "isolation_level" in result
            assert result["isolation_level"] == "AUTOCOMMIT"
            assert result["test"] == "value"

        # Verify restoration
        assert mock_settings.prepare_engine_args == original_prepare
        assert mock_settings.dispose_orm.call_count == 2
        assert mock_settings.configure_orm.call_count == 2
