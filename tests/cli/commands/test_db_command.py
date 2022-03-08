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

from unittest import mock
from unittest.mock import patch

import pendulum
import pytest
from sqlalchemy.engine.url import make_url

from airflow.cli import cli_parser
from airflow.cli.commands import db_command
from airflow.exceptions import AirflowException


class TestCliDb:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.cli.commands.db_command.db.initdb")
    def test_cli_initdb(self, mock_initdb):
        db_command.initdb(self.parser.parse_args(['db', 'init']))

        mock_initdb.assert_called_once_with()

    @mock.patch("airflow.cli.commands.db_command.db.resetdb")
    def test_cli_resetdb(self, mock_resetdb):
        db_command.resetdb(self.parser.parse_args(['db', 'reset', '--yes']))

        mock_resetdb.assert_called_once_with()

    @mock.patch("airflow.cli.commands.db_command.db.check_migrations")
    def test_cli_check_migrations(self, mock_wait_for_migrations):
        db_command.check_migrations(self.parser.parse_args(['db', 'check-migrations']))

        mock_wait_for_migrations.assert_called_once_with(timeout=60)

    @pytest.mark.parametrize(
        'args, called_with',
        [
            ([], dict(to_revision=None, from_revision=None, sql=False)),
            (['--sql-only'], dict(to_revision=None, from_revision=None, sql=True)),
            (['--revision', 'abc'], dict(to_revision='abc', from_revision=None, sql=False)),
            (['--revision', 'abc', '--sql-only'], dict(to_revision='abc', from_revision=None, sql=True)),
            (['--version', '2.2.2'], dict(to_revision='7b2661a43ba3', from_revision=None, sql=False)),
            (
                ['--version', '2.2.2', '--sql-only'],
                dict(to_revision='7b2661a43ba3', from_revision=None, sql=True),
            ),
            (
                ['--revision', 'abc', '--from-revision', 'abc123', '--sql-only'],
                dict(to_revision='abc', from_revision='abc123', sql=True),
            ),
            (
                ['--revision', 'abc', '--from-version', '2.2.2', '--sql-only'],
                dict(to_revision='abc', from_revision='7b2661a43ba3', sql=True),
            ),
            (
                ['--version', '2.2.4', '--from-revision', 'abc123', '--sql-only'],
                dict(to_revision='587bdf053233', from_revision='abc123', sql=True),
            ),
            (
                ['--version', '2.2.4', '--from-version', '2.2.2', '--sql-only'],
                dict(to_revision='587bdf053233', from_revision='7b2661a43ba3', sql=True),
            ),
        ],
    )
    @mock.patch("airflow.cli.commands.db_command.db.upgradedb")
    def test_cli_upgrade_success(self, mock_upgradedb, args, called_with):
        db_command.upgradedb(self.parser.parse_args(['db', 'upgrade', *args]))
        mock_upgradedb.assert_called_once_with(**called_with)

    @pytest.mark.parametrize(
        'args, pattern',
        [
            (['--version', '2.1.25'], 'not supported'),
            (['--revision', 'abc', '--from-revision', 'abc123'], 'used with `--sql-only`'),
            (['--revision', 'abc', '--from-version', '2.0.2'], 'used with `--sql-only`'),
            (['--revision', 'abc', '--from-version', '2.1.25', '--sql-only'], 'Unknown version'),
        ],
    )
    @mock.patch("airflow.cli.commands.db_command.db.upgradedb")
    def test_cli_upgrade_failure(self, mock_upgradedb, args, pattern):
        with pytest.raises(SystemExit, match=pattern):
            db_command.upgradedb(self.parser.parse_args(['db', 'upgrade', *args]))

    @mock.patch("airflow.cli.commands.db_command.db.upgradedb")
    def test_no_changes(self, mock_upgradedb, args, pattern):
        """test that when from rev and to rev are same, throw error or something"""
        raise Exception()

    @mock.patch("airflow.cli.commands.db_command.execute_interactive")
    @mock.patch("airflow.cli.commands.db_command.NamedTemporaryFile")
    @mock.patch(
        "airflow.cli.commands.db_command.settings.engine.url", make_url("mysql://root@mysql:3306/airflow")
    )
    def test_cli_shell_mysql(self, mock_tmp_file, mock_execute_interactive):
        mock_tmp_file.return_value.__enter__.return_value.name = "/tmp/name"
        db_command.shell(self.parser.parse_args(['db', 'shell']))
        mock_execute_interactive.assert_called_once_with(['mysql', '--defaults-extra-file=/tmp/name'])
        mock_tmp_file.return_value.__enter__.return_value.write.assert_called_once_with(
            b'[client]\nhost     = mysql\nuser     = root\npassword = \nport     = 3306'
            b'\ndatabase = airflow'
        )

    @mock.patch("airflow.cli.commands.db_command.execute_interactive")
    @mock.patch("airflow.cli.commands.db_command.NamedTemporaryFile")
    @mock.patch("airflow.cli.commands.db_command.settings.engine.url", make_url("mysql://root@mysql/airflow"))
    def test_cli_shell_mysql_without_port(self, mock_tmp_file, mock_execute_interactive):
        mock_tmp_file.return_value.__enter__.return_value.name = "/tmp/name"
        db_command.shell(self.parser.parse_args(['db', 'shell']))
        mock_execute_interactive.assert_called_once_with(['mysql', '--defaults-extra-file=/tmp/name'])
        mock_tmp_file.return_value.__enter__.return_value.write.assert_called_once_with(
            b'[client]\nhost     = mysql\nuser     = root\npassword = \nport     = 3306'
            b'\ndatabase = airflow'
        )

    @mock.patch("airflow.cli.commands.db_command.execute_interactive")
    @mock.patch(
        "airflow.cli.commands.db_command.settings.engine.url", make_url("sqlite:////root/airflow/airflow.db")
    )
    def test_cli_shell_sqlite(self, mock_execute_interactive):
        db_command.shell(self.parser.parse_args(['db', 'shell']))
        mock_execute_interactive.assert_called_once_with(['sqlite3', '/root/airflow/airflow.db'])

    @mock.patch("airflow.cli.commands.db_command.execute_interactive")
    @mock.patch(
        "airflow.cli.commands.db_command.settings.engine.url",
        make_url("postgresql+psycopg2://postgres:airflow@postgres:5432/airflow"),
    )
    def test_cli_shell_postgres(self, mock_execute_interactive):
        db_command.shell(self.parser.parse_args(['db', 'shell']))
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

    @mock.patch("airflow.cli.commands.db_command.execute_interactive")
    @mock.patch(
        "airflow.cli.commands.db_command.settings.engine.url",
        make_url("postgresql+psycopg2://postgres:airflow@postgres/airflow"),
    )
    def test_cli_shell_postgres_without_port(self, mock_execute_interactive):
        db_command.shell(self.parser.parse_args(['db', 'shell']))
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
        "airflow.cli.commands.db_command.settings.engine.url",
        make_url("invalid+psycopg2://postgres:airflow@postgres/airflow"),
    )
    def test_cli_shell_invalid(self):
        with pytest.raises(AirflowException, match=r"Unknown driver: invalid\+psycopg2"):
            db_command.shell(self.parser.parse_args(['db', 'shell']))

    @pytest.mark.parametrize(
        'args, match',
        [
            (['-y', '--revision', 'abc', '--version', '2.2.0'], 'Cannot supply both'),
            (['-y', '--revision', 'abc1', '--from-revision', 'abc2'], 'only .* with `--sql-only`'),
            (['-y', '--revision', 'abc1', '--from-version', '2.2.2'], 'only .* with `--sql-only`'),
            (['-y', '--version', '2.2.2', '--from-version', '2.2.2'], 'only .* with `--sql-only`'),
            (
                ['-y', '--revision', 'abc', '--from-version', '2.2.0', '--from-revision', 'abc'],
                'may not be combined',
            ),
            (['-y', '--version', 'abc'], r'Downgrading to .* not supported\.'),
            (['-y'], 'Must provide either'),
        ],
    )
    @mock.patch("airflow.utils.db.downgrade")
    def test_cli_downgrade_invalid(self, mock_dg, args, match):
        """We test some options that should produce an error"""

        with pytest.raises(SystemExit, match=match):
            db_command.downgrade(self.parser.parse_args(['db', 'downgrade', *args]))

    @pytest.mark.parametrize(
        'args, expected',
        [
            (['-y', '--revision', 'abc1'], dict(to_revision='abc1')),
            (
                ['-y', '--revision', 'abc1', '--from-revision', 'abc2', '-s'],
                dict(to_revision='abc1', from_revision='abc2', sql=True),
            ),
            (
                ['-y', '--revision', 'abc1', '--from-version', '2.2.2', '-s'],
                dict(to_revision='abc1', from_revision='7b2661a43ba3', sql=True),
            ),
            (
                ['-y', '--version', '2.2.2', '--from-version', '2.2.2', '-s'],
                dict(to_revision='7b2661a43ba3', from_revision='7b2661a43ba3', sql=True),
            ),
            (['-y', '--version', '2.2.2'], dict(to_revision='7b2661a43ba3')),
        ],
    )
    @mock.patch("airflow.utils.db.downgrade")
    def test_cli_downgrade_good(self, mock_dg, args, expected):
        defaults = dict(from_revision=None, sql=False)
        db_command.downgrade(self.parser.parse_args(['db', 'downgrade', *args]))
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
    @mock.patch("airflow.cli.commands.db_command.input")
    def test_cli_downgrade_confirm(self, mock_input, mock_dg, resp, raise_):
        mock_input.return_value = resp
        if raise_:
            with pytest.raises(SystemExit):
                db_command.downgrade(self.parser.parse_args(['db', 'downgrade', '--revision', 'abc']))
        else:
            db_command.downgrade(self.parser.parse_args(['db', 'downgrade', '--revision', 'abc']))
            mock_dg.assert_called_with(to_revision='abc', from_revision=None, sql=False)


class TestCLIDBClean:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @pytest.mark.parametrize('timezone', ['UTC', 'Europe/Berlin', 'America/Los_Angeles'])
    @patch('airflow.cli.commands.db_command.run_cleanup')
    def test_date_timezone_omitted(self, run_cleanup_mock, timezone):
        """
        When timezone omitted we should always expect that the timestamp is
        coerced to tz-aware with default timezone
        """
        timestamp = '2021-01-01 00:00:00'
        with patch('airflow.utils.timezone.TIMEZONE', pendulum.timezone(timezone)):
            args = self.parser.parse_args(['db', 'clean', '--clean-before-timestamp', f"{timestamp}", '-y'])
            db_command.cleanup_tables(args)
        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=False,
            clean_before_timestamp=pendulum.parse(timestamp, tz=timezone),
            verbose=False,
            confirm=False,
        )

    @pytest.mark.parametrize('timezone', ['UTC', 'Europe/Berlin', 'America/Los_Angeles'])
    @patch('airflow.cli.commands.db_command.run_cleanup')
    def test_date_timezone_supplied(self, run_cleanup_mock, timezone):
        """
        When tz included in the string then default timezone should not be used.
        """
        timestamp = '2021-01-01 00:00:00+03:00'
        with patch('airflow.utils.timezone.TIMEZONE', pendulum.timezone(timezone)):
            args = self.parser.parse_args(['db', 'clean', '--clean-before-timestamp', f"{timestamp}", '-y'])
            db_command.cleanup_tables(args)

        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=False,
            clean_before_timestamp=pendulum.parse(timestamp),
            verbose=False,
            confirm=False,
        )

    @pytest.mark.parametrize('confirm_arg, expected', [(['-y'], False), ([], True)])
    @patch('airflow.cli.commands.db_command.run_cleanup')
    def test_confirm(self, run_cleanup_mock, confirm_arg, expected):
        """
        When tz included in the string then default timezone should not be used.
        """
        args = self.parser.parse_args(
            [
                'db',
                'clean',
                '--clean-before-timestamp',
                '2021-01-01',
                *confirm_arg,
            ]
        )
        db_command.cleanup_tables(args)

        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=False,
            clean_before_timestamp=pendulum.parse('2021-01-01 00:00:00Z'),
            verbose=False,
            confirm=expected,
        )

    @pytest.mark.parametrize('dry_run_arg, expected', [(['--dry-run'], True), ([], False)])
    @patch('airflow.cli.commands.db_command.run_cleanup')
    def test_dry_run(self, run_cleanup_mock, dry_run_arg, expected):
        """
        When tz included in the string then default timezone should not be used.
        """
        args = self.parser.parse_args(
            [
                'db',
                'clean',
                '--clean-before-timestamp',
                '2021-01-01',
                *dry_run_arg,
            ]
        )
        db_command.cleanup_tables(args)

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
    @patch('airflow.cli.commands.db_command.run_cleanup')
    def test_tables(self, run_cleanup_mock, extra_args, expected):
        """
        When tz included in the string then default timezone should not be used.
        """
        args = self.parser.parse_args(
            [
                'db',
                'clean',
                '--clean-before-timestamp',
                '2021-01-01',
                *extra_args,
            ]
        )
        db_command.cleanup_tables(args)

        run_cleanup_mock.assert_called_once_with(
            table_names=expected,
            dry_run=False,
            clean_before_timestamp=pendulum.parse('2021-01-01 00:00:00Z'),
            verbose=False,
            confirm=True,
        )

    @pytest.mark.parametrize('extra_args, expected', [(['--verbose'], True), ([], False)])
    @patch('airflow.cli.commands.db_command.run_cleanup')
    def test_verbose(self, run_cleanup_mock, extra_args, expected):
        """
        When tz included in the string then default timezone should not be used.
        """
        args = self.parser.parse_args(
            [
                'db',
                'clean',
                '--clean-before-timestamp',
                '2021-01-01',
                *extra_args,
            ]
        )
        db_command.cleanup_tables(args)

        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=False,
            clean_before_timestamp=pendulum.parse('2021-01-01 00:00:00Z'),
            verbose=expected,
            confirm=True,
        )
