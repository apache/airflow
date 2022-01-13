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
#
from unittest.mock import patch

import pendulum
import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import maintenance_command


class TestCLIMaintenance:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @pytest.mark.parametrize('timezone', ['UTC', 'Europe/Berlin', 'America/Los_Angeles'])
    @patch('airflow.cli.commands.maintenance_command.run_cleanup')
    def test_date_timezone_omitted(self, run_cleanup_mock, timezone):
        """
        When timezone omitted we should always expect that the timestamp is
        coerced to tz-aware with default timezone
        """
        timestamp = '2021-01-01 00:00:00'
        with patch('airflow.utils.timezone.TIMEZONE', pendulum.timezone(timezone)):
            args = self.parser.parse_args(
                ['maintenance', 'cleanup-tables', f"--clean-before-timestamp", f"{timestamp}", '-y']
            )
            maintenance_command.cleanup_tables(args)
        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=False,
            clean_before_timestamp=pendulum.parse(timestamp, tz=timezone),
            verbose=False,
            confirm=False,
        )

    @pytest.mark.parametrize('timezone', ['UTC', 'Europe/Berlin', 'America/Los_Angeles'])
    @patch('airflow.cli.commands.maintenance_command.run_cleanup')
    def test_date_timezone_supplied(self, run_cleanup_mock, timezone):
        """
        When tz included in the string then default timezone should not be used.
        """
        timestamp = '2021-01-01 00:00:00+03:00'
        with patch('airflow.utils.timezone.TIMEZONE', pendulum.timezone(timezone)):
            args = self.parser.parse_args(
                ['maintenance', 'cleanup-tables', f"--clean-before-timestamp", f"{timestamp}", '-y']
            )
            maintenance_command.cleanup_tables(args)

        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=False,
            clean_before_timestamp=pendulum.parse(timestamp),
            verbose=False,
            confirm=False,
        )

    @pytest.mark.parametrize('confirm_arg, expected', [(['-y'], False), ([], True)])
    @patch('airflow.cli.commands.maintenance_command.run_cleanup')
    def test_confirm(self, run_cleanup_mock, confirm_arg, expected):
        """
        When tz included in the string then default timezone should not be used.
        """
        args = self.parser.parse_args(
            [
                'maintenance',
                'cleanup-tables',
                f"--clean-before-timestamp",
                "2021-01-01",
                *confirm_arg,
            ]
        )
        maintenance_command.cleanup_tables(args)

        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=False,
            clean_before_timestamp=pendulum.parse('2021-01-01 00:00:00Z'),
            verbose=False,
            confirm=expected,
        )

    @pytest.mark.parametrize('dry_run_arg, expected', [(['--dry-run'], True), ([], False)])
    @patch('airflow.cli.commands.maintenance_command.run_cleanup')
    def test_dry_run(self, run_cleanup_mock, dry_run_arg, expected):
        """
        When tz included in the string then default timezone should not be used.
        """
        args = self.parser.parse_args(
            [
                'maintenance',
                'cleanup-tables',
                f"--clean-before-timestamp",
                "2021-01-01",
                *dry_run_arg,
            ]
        )
        maintenance_command.cleanup_tables(args)

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
    @patch('airflow.cli.commands.maintenance_command.run_cleanup')
    def test_tables(self, run_cleanup_mock, extra_args, expected):
        """
        When tz included in the string then default timezone should not be used.
        """
        args = self.parser.parse_args(
            [
                'maintenance',
                'cleanup-tables',
                f"--clean-before-timestamp",
                "2021-01-01",
                *extra_args,
            ]
        )
        maintenance_command.cleanup_tables(args)

        run_cleanup_mock.assert_called_once_with(
            table_names=expected,
            dry_run=False,
            clean_before_timestamp=pendulum.parse('2021-01-01 00:00:00Z'),
            verbose=False,
            confirm=True,
        )

    @pytest.mark.parametrize('extra_args, expected', [(['--verbose'], True), ([], False)])
    @patch('airflow.cli.commands.maintenance_command.run_cleanup')
    def test_verbose(self, run_cleanup_mock, extra_args, expected):
        """
        When tz included in the string then default timezone should not be used.
        """
        args = self.parser.parse_args(
            [
                'maintenance',
                'cleanup-tables',
                f"--clean-before-timestamp",
                "2021-01-01",
                *extra_args,
            ]
        )
        maintenance_command.cleanup_tables(args)

        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=False,
            clean_before_timestamp=pendulum.parse('2021-01-01 00:00:00Z'),
            verbose=expected,
            confirm=True,
        )
