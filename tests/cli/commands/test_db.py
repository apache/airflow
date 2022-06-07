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

import re
from unittest import mock

import pendulum
import pytest
from click.testing import CliRunner
from pytest import param
from sqlalchemy.engine.url import make_url

from airflow.cli.commands import db
from airflow.exceptions import AirflowException


class TestCliDb:
    @classmethod
    def setup_class(cls):
        cls.runner = CliRunner()

    @mock.patch("airflow.utils.db.initdb")
    def test_cli_initdb(self, mock_initdb):
        response = self.runner.invoke(db.db_init)

        assert response.exit_code == 0
        mock_initdb.assert_called_once_with()

    @mock.patch("airflow.utils.db.resetdb")
    def test_cli_resetdb(self, mock_resetdb):
        response = self.runner.invoke(db.db_reset, ['--yes'])

        assert response.exit_code == 0
        mock_resetdb.assert_called_once_with(skip_init=False)

    @mock.patch("airflow.utils.db.resetdb")
    def test_cli_resetdb_skip_init(self, mock_resetdb):
        response = self.runner.invoke(db.db_reset, ['--yes', '--skip-init'])

        assert response.exit_code == 0
        mock_resetdb.assert_called_once_with(skip_init=True)

    @mock.patch("airflow.utils.db.check_migrations")
    def test_cli_check_migrations(self, mock_wait_for_migrations):
        response = self.runner.invoke(db.check_migrations)

        assert response.exit_code == 0
        mock_wait_for_migrations.assert_called_once_with(timeout=60)

    @pytest.mark.parametrize(
        'args, called_with',
        [
            param(
                [],
                dict(to_revision=None, from_revision=None, show_sql_only=False),
                id="No flags",
            ),
            param(
                ['--show-sql-only'],
                dict(to_revision=None, from_revision=None, show_sql_only=True),
                id="Just show SQL",
            ),
            param(
                ['--to-revision', 'abc'],
                dict(to_revision='abc', from_revision=None, show_sql_only=False),
                id="Just --to-revision",
            ),
            param(
                ['--to-revision', 'abc', '--show-sql-only'],
                dict(to_revision='abc', from_revision=None, show_sql_only=True),
                id="Show SQL with --to-revison",
            ),
            param(
                ['--to-version', '2.2.2'],
                dict(to_revision='7b2661a43ba3', from_revision=None, show_sql_only=False),
                id="Just --to-version",
            ),
            param(
                ['--to-version', '2.2.2', '--show-sql-only'],
                dict(to_revision='7b2661a43ba3', from_revision=None, show_sql_only=True),
                id="Show SQL with --to-version",
            ),
            param(
                ['--to-revision', 'abc', '--from-revision', 'abc123', '--show-sql-only'],
                dict(to_revision='abc', from_revision='abc123', show_sql_only=True),
                id="Show SQL with from revision and to revision",
            ),
            param(
                ['--to-revision', 'abc', '--from-version', '2.2.2', '--show-sql-only'],
                dict(to_revision='abc', from_revision='7b2661a43ba3', show_sql_only=True),
                id="Show SQL with from version and to revision",
            ),
            param(
                ['--to-version', '2.2.4', '--from-revision', 'abc123', '--show-sql-only'],
                dict(to_revision='587bdf053233', from_revision='abc123', show_sql_only=True),
                id="Show SQL with from revision and to version",
            ),
            param(
                ['--to-version', '2.2.4', '--from-version', '2.2.2', '--show-sql-only'],
                dict(to_revision='587bdf053233', from_revision='7b2661a43ba3', show_sql_only=True),
                id="Show SQL with from version and to version",
            ),
        ],
    )
    @mock.patch("airflow.utils.db.upgradedb")
    def test_cli_upgrade_success(self, mock_upgradedb, args, called_with):
        response = self.runner.invoke(db.upgrade, args, input='y')

        assert response.exit_code == 0
        mock_upgradedb.assert_called_once_with(**called_with)

    @pytest.mark.parametrize(
        'args, pattern',
        [
            param(['--to-version', '2.1.25'], 'not supported', id='bad version'),
            param(
                ['--to-revision', 'abc', '--from-revision', 'abc123'],
                'used with `--show-sql-only`',
                id='requires offline',
            ),
            param(
                ['--to-revision', 'abc', '--from-version', '2.0.2'],
                'used with `--show-sql-only`',
                id='requires offline',
            ),
            param(
                ['--to-revision', 'abc', '--from-version', '2.1.25', '--show-sql-only'],
                'Unknown version',
                id='bad version',
            ),
        ],
    )
    @mock.patch("airflow.utils.db.upgradedb")
    def test_cli_upgrade_failure(self, mock_upgradedb, args, pattern):
        response = self.runner.invoke(db.upgrade, args, input='y')

        assert response.exit_code != 0
        assert pattern in str(response.exception)

    @mock.patch("airflow.cli.commands.db.execute_interactive")
    @mock.patch("airflow.cli.commands.db.NamedTemporaryFile")
    @mock.patch("airflow.cli.commands.db.settings.engine.url", make_url("mysql://root@mysql:3306/airflow"))
    def test_cli_shell_mysql(self, mock_tmp_file, mock_execute_interactive):
        mock_tmp_file.return_value.__enter__.return_value.name = "/tmp/name"

        response = self.runner.invoke(db.shell)

        assert response.exit_code == 0
        mock_execute_interactive.assert_called_once_with(['mysql', '--defaults-extra-file=/tmp/name'])
        mock_tmp_file.return_value.__enter__.return_value.write.assert_called_once_with(
            b'[client]\nhost     = mysql\nuser     = root\npassword = \nport     = 3306'
            b'\ndatabase = airflow'
        )

    @mock.patch("airflow.cli.commands.db.execute_interactive")
    @mock.patch("airflow.cli.commands.db.NamedTemporaryFile")
    @mock.patch("airflow.cli.commands.db.settings.engine.url", make_url("mysql://root@mysql/airflow"))
    def test_cli_shell_mysql_without_port(self, mock_tmp_file, mock_execute_interactive):
        mock_tmp_file.return_value.__enter__.return_value.name = "/tmp/name"

        response = self.runner.invoke(db.shell)

        assert response.exit_code == 0
        mock_execute_interactive.assert_called_once_with(['mysql', '--defaults-extra-file=/tmp/name'])
        mock_tmp_file.return_value.__enter__.return_value.write.assert_called_once_with(
            b'[client]\nhost     = mysql\nuser     = root\npassword = \nport     = 3306'
            b'\ndatabase = airflow'
        )

    @mock.patch("airflow.cli.commands.db.execute_interactive")
    @mock.patch("airflow.cli.commands.db.settings.engine.url", make_url("sqlite:////root/airflow/airflow.db"))
    def test_cli_shell_sqlite(self, mock_execute_interactive):
        response = self.runner.invoke(db.shell)

        assert response.exit_code == 0
        mock_execute_interactive.assert_called_once_with(['sqlite3', '/root/airflow/airflow.db'])

    @mock.patch("airflow.cli.commands.db.execute_interactive")
    @mock.patch(
        "airflow.cli.commands.db.settings.engine.url",
        make_url("postgresql+psycopg2://postgres:airflow@postgres:5432/airflow"),
    )
    def test_cli_shell_postgres(self, mock_execute_interactive):
        response = self.runner.invoke(db.shell)

        assert response.exit_code == 0
        mock_execute_interactive.assert_called_once_with(['psql'], env=mock.ANY)
        _, kwargs = mock_execute_interactive.call_args
        env = kwargs['env']
        postgres_env = {k: v for k, v in env.items() if k.startswith('PG')}
        assert {
            'PGDATABASE': 'airflow',
            'PGHOST': 'postgres',
            'PGPASSWORD': 'airflow',
            'PGPORT': '5432',
            'PGUSER': 'postgres',
        } == postgres_env

    @mock.patch("airflow.cli.commands.db.execute_interactive")
    @mock.patch(
        "airflow.cli.commands.db.settings.engine.url",
        make_url("postgresql+psycopg2://postgres:airflow@postgres/airflow"),
    )
    def test_cli_shell_postgres_without_port(self, mock_execute_interactive):
        response = self.runner.invoke(db.shell)

        assert response.exit_code == 0
        mock_execute_interactive.assert_called_once_with(['psql'], env=mock.ANY)
        _, kwargs = mock_execute_interactive.call_args
        env = kwargs['env']
        postgres_env = {k: v for k, v in env.items() if k.startswith('PG')}
        assert {
            'PGDATABASE': 'airflow',
            'PGHOST': 'postgres',
            'PGPASSWORD': 'airflow',
            'PGPORT': '5432',
            'PGUSER': 'postgres',
        } == postgres_env

    @mock.patch(
        "airflow.cli.commands.db.settings.engine.url",
        make_url("invalid+psycopg2://postgres:airflow@postgres/airflow"),
    )
    def test_cli_shell_invalid(self):
        response = self.runner.invoke(db.shell)

        assert response.exit_code != 0
        assert isinstance(response.exception, AirflowException)
        assert "Unknown driver: invalid+psycopg2" in str(response.exception)

    @pytest.mark.parametrize(
        'args, pattern',
        [
            param(
                ['-y', '--to-revision', 'abc', '--to-version', '2.2.0'],
                r'Cannot supply both .*',
                id="Both --to-revision and --to-version",
            ),
            param(
                ['-y', '--to-revision', 'abc1', '--from-revision', 'abc2'],
                r'.* may only be used with `--show-sql-only`',
                id="Only with --show-sql-only: --to-revision and --from-revision",
            ),
            param(
                ['-y', '--to-revision', 'abc1', '--from-version', '2.2.2'],
                r'.* may only be used with `--show-sql-only`',
                id="Only with --show-sql-only: --to-revision and --from-version",
            ),
            param(
                ['-y', '--to-version', '2.2.2', '--from-version', '2.2.2'],
                r'.* only be used with `--show-sql-only`',
                id="Only with --show-sql-only: --to-version and --from-version",
            ),
            param(
                ['-y', '--to-revision', 'abc', '--from-version', '2.2.0', '--from-revision', 'abc'],
                r'Cannot supply both .*',
                id="Both --from-revision and --from-version",
            ),
            param(
                ['-y', '--to-version', 'abc'],
                r'Downgrading to .* is not supported\.',
                id="Downgrading to version not supported",
            ),
            param(['-y'], 'Must provide either', id="Must provide either --to-revision or --to-version"),
        ],
    )
    @mock.patch("airflow.utils.db.downgrade")
    def test_cli_downgrade_invalid(self, mock_dg, args, pattern):
        """We test some options that should produce an error"""

        response = self.runner.invoke(db.downgrade, args)

        assert response.exit_code != 0
        assert re.match(pattern, str(response.exception))

    @pytest.mark.parametrize(
        'args, expected',
        [
            param(
                ['-y', '--to-revision', 'abc1'],
                dict(to_revision='abc1'),
                id="Just --to-revision",
            ),
            param(
                ['-y', '--to-revision', 'abc1', '--from-revision', 'abc2', '-s'],
                dict(to_revision='abc1', from_revision='abc2', show_sql_only=True),
                id="",
            ),
            param(
                ['-y', '--to-revision', 'abc1', '--from-version', '2.2.2', '-s'],
                dict(to_revision='abc1', from_revision='7b2661a43ba3', show_sql_only=True),
                id="",
            ),
            param(
                ['-y', '--to-version', '2.2.2', '--from-version', '2.2.2', '-s'],
                dict(to_revision='7b2661a43ba3', from_revision='7b2661a43ba3', show_sql_only=True),
                id="",
            ),
            param(
                ['-y', '--to-version', '2.2.2'],
                dict(to_revision='7b2661a43ba3'),
                id="",
            ),
        ],
    )
    @mock.patch("airflow.utils.db.downgrade")
    def test_cli_downgrade_good(self, mock_dg, args, expected):
        defaults = dict(from_revision=None, show_sql_only=False)

        response = self.runner.invoke(db.downgrade, args)

        assert response.exit_code == 0
        mock_dg.assert_called_with(**{**defaults, **expected})

    @pytest.mark.parametrize(
        'resp, raise_',
        [
            ('y', False),
            ('Y', False),
            ('n', True),
            ('a', True),  # any other value
        ],
    )
    @mock.patch("airflow.utils.db.downgrade")
    def test_cli_downgrade_confirm(self, mock_dg, resp, raise_):
        if raise_:
            response = self.runner.invoke(db.downgrade, ['--to-revision', 'abc'], input=resp)

            assert response.exit_code != 0
            assert mock_dg.not_called

        else:
            response = self.runner.invoke(db.downgrade, ['--to-revision', 'abc'], input=resp)

            assert response.exit_code == 0
            mock_dg.assert_called_with(to_revision='abc', from_revision=None, show_sql_only=False)


class TestCLIDBClean:
    @classmethod
    def setup_class(cls):
        cls.runner = CliRunner()

    @pytest.mark.parametrize('timezone', ['UTC', 'Europe/Berlin', 'America/Los_Angeles'])
    @mock.patch('airflow.utils.db_cleanup.run_cleanup')
    def test_date_timezone_omitted(self, run_cleanup_mock, timezone):
        """
        When timezone omitted we should always expect that the timestamp is
        coerced to tz-aware with default timezone
        """
        timestamp = '2021-01-01 00:00:00'
        with mock.patch('airflow.utils.timezone.TIMEZONE', pendulum.timezone(timezone)):
            response = self.runner.invoke(
                db.cleanup_tables, ['--clean-before-timestamp', f"{timestamp}", '-y']
            )

        assert response.exit_code == 0

        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=False,
            clean_before_timestamp=pendulum.parse(timestamp, tz=timezone),
            verbose=False,
            confirm=False,
        )

    @pytest.mark.parametrize('timezone', ['UTC', 'Europe/Berlin', 'America/Los_Angeles'])
    @mock.patch('airflow.utils.db_cleanup.run_cleanup')
    def test_date_timezone_supplied(self, run_cleanup_mock, timezone):
        """
        When tz included in the string then default timezone should not be used.
        """
        timestamp = '2021-01-01 00:00:00+03:00'
        with mock.patch('airflow.utils.timezone.TIMEZONE', pendulum.timezone(timezone)):
            response = self.runner.invoke(
                db.cleanup_tables, ['--clean-before-timestamp', f"{timestamp}", '-y']
            )

        assert response.exit_code == 0

        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=False,
            clean_before_timestamp=pendulum.parse(timestamp),
            verbose=False,
            confirm=False,
        )

    @pytest.mark.parametrize('confirm_arg, expected', [(['-y'], False), ([], True)])
    @mock.patch('airflow.utils.db_cleanup.run_cleanup')
    def test_confirm(self, run_cleanup_mock, confirm_arg, expected):
        """
        When tz included in the string then default timezone should not be used.
        """
        response = self.runner.invoke(
            db.cleanup_tables,
            [
                '--clean-before-timestamp',
                '2021-01-01',
                *confirm_arg,
            ],
        )

        assert response.exit_code == 0
        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=False,
            clean_before_timestamp=pendulum.parse('2021-01-01 00:00:00Z'),
            verbose=False,
            confirm=expected,
        )

    @pytest.mark.parametrize('dry_run_arg, expected', [(['--dry-run'], True), ([], False)])
    @mock.patch('airflow.utils.db_cleanup.run_cleanup')
    def test_dry_run(self, run_cleanup_mock, dry_run_arg, expected):
        """
        When tz included in the string then default timezone should not be used.
        """
        response = self.runner.invoke(
            db.cleanup_tables,
            [
                '--clean-before-timestamp',
                '2021-01-01',
                *dry_run_arg,
            ],
        )

        assert response.exit_code == 0
        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=expected,
            clean_before_timestamp=pendulum.parse('2021-01-01 00:00:00Z'),
            verbose=False,
            confirm=True,
        )

    @pytest.mark.parametrize(
        'extra_args, expected', [(['--tables', 'hello, goodbye'], ['hello', 'goodbye']), ([], None)]
    )
    @mock.patch('airflow.utils.db_cleanup.run_cleanup')
    def test_tables(self, run_cleanup_mock, extra_args, expected):
        """
        When tz included in the string then default timezone should not be used.
        """
        response = self.runner.invoke(
            db.cleanup_tables,
            [
                '--clean-before-timestamp',
                '2021-01-01',
                *extra_args,
            ],
        )

        assert response.exit_code == 0
        run_cleanup_mock.assert_called_once_with(
            table_names=expected,
            dry_run=False,
            clean_before_timestamp=pendulum.parse('2021-01-01 00:00:00Z'),
            verbose=False,
            confirm=True,
        )

    @pytest.mark.parametrize('extra_args, expected', [(['--verbose'], True), ([], False)])
    @mock.patch('airflow.utils.db_cleanup.run_cleanup')
    def test_verbose(self, run_cleanup_mock, extra_args, expected):
        """
        When tz included in the string then default timezone should not be used.
        """
        response = self.runner.invoke(
            db.cleanup_tables,
            [
                '--clean-before-timestamp',
                '2021-01-01',
                *extra_args,
            ],
        )

        assert response.exit_code == 0
        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=False,
            clean_before_timestamp=pendulum.parse('2021-01-01 00:00:00Z'),
            verbose=expected,
            confirm=True,
        )
