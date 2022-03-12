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

import inspect
import re
from unittest import mock

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
from airflow.utils.db import check_migrations, create_default_connections, downgrade, upgradedb


class TestDb:
    def test_database_schema_and_sqlalchemy_model_are_in_sync(self):
        all_meta_data = MetaData()
        for (table_name, table) in airflow_base.metadata.tables.items():
            all_meta_data._add_table(table_name, table.schema, table)

        # create diff between database schema and SQLAlchemy model
        mctx = MigrationContext.configure(engine.connect())
        diff = compare_metadata(mctx, all_meta_data)

        # known diffs to ignore
        ignores = [
            # ignore tables created by celery
            lambda t: (t[0] == 'remove_table' and t[1].name == 'celery_taskmeta'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'celery_tasksetmeta'),
            # ignore indices created by celery
            lambda t: (t[0] == 'remove_index' and t[1].name == 'task_id'),
            lambda t: (t[0] == 'remove_index' and t[1].name == 'taskset_id'),
            # Ignore all the fab tables
            lambda t: (t[0] == 'remove_table' and t[1].name == 'ab_permission'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'ab_register_user'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'ab_role'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'ab_permission_view'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'ab_permission_view_role'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'ab_user_role'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'ab_user'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'ab_view_menu'),
            # Ignore all the fab indices
            lambda t: (t[0] == 'remove_index' and t[1].name == 'permission_id'),
            lambda t: (t[0] == 'remove_index' and t[1].name == 'name'),
            lambda t: (t[0] == 'remove_index' and t[1].name == 'user_id'),
            lambda t: (t[0] == 'remove_index' and t[1].name == 'username'),
            lambda t: (t[0] == 'remove_index' and t[1].name == 'field_string'),
            lambda t: (t[0] == 'remove_index' and t[1].name == 'email'),
            lambda t: (t[0] == 'remove_index' and t[1].name == 'permission_view_id'),
            # from test_security unit test
            lambda t: (t[0] == 'remove_table' and t[1].name == 'some_model'),
            # MSSQL default tables
            lambda t: (t[0] == 'remove_table' and t[1].name == 'spt_monitor'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'spt_fallback_db'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'spt_fallback_usg'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'MSreplication_options'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'spt_fallback_dev'),
            # Ignore flask-session table/index
            lambda t: (t[0] == 'remove_table' and t[1].name == 'session'),
            lambda t: (t[0] == 'remove_index' and t[1].name == 'session_id'),
        ]
        for ignore in ignores:
            diff = [d for d in diff if not ignore(d)]

        assert not diff, 'Database schema and SQLAlchemy model are not in sync: ' + str(diff)

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
        pattern = re.compile('conn_id=[\"|\'](.*?)[\"|\']', re.DOTALL)
        source = inspect.getsource(create_default_connections)
        src = pattern.findall(source)
        assert sorted(src) == src

    def test_check_migrations(self):
        # Should run without error. Can't easily test the behaviour, but we can check it works
        check_migrations(1)

    @mock.patch('alembic.command')
    def test_upgradedb(self, mock_alembic_command):
        upgradedb()
        mock_alembic_command.upgrade.assert_called_once_with(mock.ANY, 'heads')

    @pytest.mark.parametrize(
        'version, revision',
        [('2.0.0:2.2.3', "e959f08ac86c:be2bfac3da23"), ("2.0.2:2.1.4", "2e42bb497a22:ccde3e26fe78")],
    )
    def test_offline_upgrade_version(self, version, revision):
        with mock.patch('airflow.utils.db.settings.engine.dialect'):
            with mock.patch('alembic.command.upgrade') as mock_alembic_upgrade:
                upgradedb(version_range=version)
        mock_alembic_upgrade.assert_called_once_with(mock.ANY, revision, sql=True)

    @pytest.mark.parametrize(
        'version, revision',
        [('2.2.3:2.0.0', "be2bfac3da23:e959f08ac86c"), ("2.1.4:2.0.2", "ccde3e26fe78:2e42bb497a22")],
    )
    def test_offline_upgrade_fails_for_migration_incorrect_versions(self, version, revision):
        with mock.patch('airflow.utils.db.settings.engine.dialect'):
            with pytest.raises(AirflowException) as e:
                upgradedb(version)
        assert e.exconly() == (
            f"airflow.exceptions.AirflowException: "
            f"Error while checking history for revision range {revision}. "
            f"Check that the supplied airflow version is in the format 'old_version:new_version'."
        )

    @pytest.mark.parametrize(
        'version, error',
        [
            ('2.2.3', 'Please provide Airflow version range with the format "old_version:new_version"'),
            ("2.1.2:2.1.5", "Please provide valid Airflow versions above 2.0.0."),
        ],
    )
    def test_offline_upgrade_fails_for_migration_single_versions_or_not_existing_head(self, version, error):
        with pytest.raises(AirflowException) as e:
            upgradedb(version)
        assert e.exconly() == (f"airflow.exceptions.AirflowException: {error}")

    @pytest.mark.parametrize('revision', ['90d1635d7b86:54bebd308c5f', "e959f08ac86c:587bdf053233"])
    def test_offline_upgrade_revision(self, revision):
        with mock.patch('airflow.utils.db.settings.engine.dialect'):
            with mock.patch('alembic.command.upgrade') as mock_alembic_upgrade:
                upgradedb(revision_range=revision)
        mock_alembic_upgrade.assert_called_once_with(mock.ANY, revision, sql=True)

    def test_offline_upgrade_fails_for_migration_less_than_2_0_0_head(self):
        rev_2_0_0_head = 'e959f08ac86c'
        with mock.patch('airflow.utils.db.settings.engine.dialect'):
            with pytest.raises(AirflowException) as e:
                upgradedb(revision_range='e1a11ece99cc:54bebd308c5f')
        revision = f"{rev_2_0_0_head}:e1a11ece99cc"
        assert e.exconly() == (
            f"airflow.exceptions.AirflowException: "
            f"Error while checking history for revision range {revision}. "
            f"Check that {revision.split(':')[1]} is a valid revision. "
            f"Supported revision for offline migration is from {rev_2_0_0_head} "
            f"which is airflow 2.0.0 head"
        )

    def test_sqlite_offline_upgrade_raises_with_revision(self):
        with mock.patch('airflow.utils.db.settings.engine.dialect') as dialect:
            dialect.name = 'sqlite'
            with pytest.raises(AirflowException) as e:
                upgradedb(revision_range='e1a11ece99cc:54bebd308c5f')
        assert e.exconly() == (
            "airflow.exceptions.AirflowException: SQLite is not supported for offline migration."
        )

    def test_sqlite_offline_upgrade_raises_with_version(self):
        with mock.patch('airflow.utils.db.settings.engine.dialect') as dialect:
            dialect.name = 'sqlite'
            with pytest.raises(AirflowException) as e:
                upgradedb(revision_range='2.0.0:2.2.3')
        assert e.exconly() == (
            "airflow.exceptions.AirflowException: SQLite is not supported for offline migration."
        )

    def test_offline_upgrade_fails_for_migration_less_than_2_2_0_head_for_mssql(self):
        rev_2_2_0_head = '7b2661a43ba3'
        with mock.patch('airflow.utils.db.settings.engine.dialect') as dialect:
            dialect.name = 'mssql'
            with pytest.raises(AirflowException) as e:
                upgradedb(revision_range='e1a11ece99cc:54bebd308c5f')
        revision = f"{rev_2_2_0_head}:e1a11ece99cc"
        assert e.exconly() == (
            f"airflow.exceptions.AirflowException: "
            f"Error while checking history for revision range {revision}. "
            f"Check that {revision.split(':')[1]} is a valid revision. "
            f"Supported revision for offline migration is from {rev_2_2_0_head} "
            f"which is airflow 2.2.0 head"
        )

    def test_versions_without_migration_donot_raise(self):
        with mock.patch('airflow.utils.db.settings.engine.dialect'):
            with mock.patch('alembic.command.upgrade') as mock_alembic_upgrade:
                upgradedb("2.1.1:2.1.2")
        mock_alembic_upgrade.assert_not_called()

    @mock.patch('airflow.utils.db._offline_migration')
    def test_downgrade_sql_no_from(self, mock_om):
        downgrade(to_revision='abc', sql=True, from_revision=None)
        actual = mock_om.call_args[1]['revision']
        assert re.match(r'[a-z0-9]+:abc', actual) is not None

    @mock.patch('airflow.utils.db._offline_migration')
    def test_downgrade_sql_with_from(self, mock_om):
        downgrade(to_revision='abc', sql=True, from_revision='123')
        actual = mock_om.call_args[1]['revision']
        assert actual == '123:abc'

    @mock.patch('alembic.command.downgrade')
    def test_downgrade_invalid_combo(self, mock_om):
        """can't combine `sql=False` and `from_revision`"""
        with pytest.raises(ValueError, match="can't be combined"):
            downgrade(to_revision='abc', from_revision='123')

    @mock.patch('alembic.command.downgrade')
    def test_downgrade_with_from(self, mock_om):
        downgrade(to_revision='abc')
        actual = mock_om.call_args[1]['revision']
        assert actual == 'abc'
