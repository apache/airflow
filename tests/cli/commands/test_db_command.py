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

import unittest
from unittest import mock

import pytest
from sqlalchemy.engine.url import make_url

from airflow.cli import cli_parser
from airflow.cli.commands import db_command
from airflow.exceptions import AirflowException


class TestCliDb(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
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

        mock_wait_for_migrations.assert_called_once_with(timeout=0)

    @mock.patch("airflow.cli.commands.db_command.db.upgradedb")
    def test_cli_upgradedb(self, mock_upgradedb):
        db_command.upgradedb(self.parser.parse_args(['db', 'upgrade']))

        mock_upgradedb.assert_called_once_with()

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
