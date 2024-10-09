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

from airflow.cli import cli_parser
from airflow.cli.commands import kerberos_command
from airflow.security.kerberos import KerberosMode

from dev.tests_common.test_utils.config import conf_vars

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


class TestKerberosCommand:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.cli.commands.kerberos_command.krb")
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def test_run_command(self, mock_krb):
        args = self.parser.parse_args(["kerberos", "PRINCIPAL", "--keytab", "/tmp/airflow.keytab"])

        kerberos_command.kerberos(args)
        mock_krb.run.assert_called_once_with(
            keytab="/tmp/airflow.keytab", principal="PRINCIPAL", mode=KerberosMode.STANDARD
        )

    @mock.patch("airflow.cli.commands.daemon_utils.TimeoutPIDLockFile")
    @mock.patch("airflow.cli.commands.daemon_utils.setup_locations")
    @mock.patch("airflow.cli.commands.daemon_utils.daemon")
    @mock.patch("airflow.cli.commands.kerberos_command.krb")
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def test_run_command_daemon(self, mock_krb, mock_daemon, mock_setup_locations, mock_pid_file):
        mock_setup_locations.return_value = (
            mock.MagicMock(name="pidfile"),
            mock.MagicMock(name="stdout"),
            mock.MagicMock(name="stderr"),
            mock.MagicMock(name="INVALID"),
        )
        args = self.parser.parse_args(
            [
                "kerberos",
                "PRINCIPAL",
                "--keytab",
                "/tmp/airflow.keytab",
                "--log-file",
                "/tmp/kerberos.log",
                "--pid",
                "/tmp/kerberos.pid",
                "--stderr",
                "/tmp/kerberos-stderr.log",
                "--stdout",
                "/tmp/kerberos-stdout.log",
                "--daemon",
            ]
        )
        mock_open = mock.mock_open()
        with mock.patch("airflow.cli.commands.daemon_utils.open", mock_open):
            kerberos_command.kerberos(args)

        mock_krb.run.assert_called_once_with(
            keytab="/tmp/airflow.keytab", principal="PRINCIPAL", mode=KerberosMode.STANDARD
        )
        assert mock_daemon.mock_calls[:3] == [
            mock.call.DaemonContext(
                pidfile=mock_pid_file.return_value,
                files_preserve=None,
                stderr=mock_open.return_value,
                stdout=mock_open.return_value,
                umask=0o077,
            ),
            mock.call.DaemonContext().__enter__(),
            mock.call.DaemonContext().__exit__(None, None, None),
        ]

        assert mock_setup_locations.mock_calls[0] == mock.call(
            process="kerberos",
            pid="/tmp/kerberos.pid",
            stdout="/tmp/kerberos-stdout.log",
            stderr="/tmp/kerberos-stderr.log",
            log="/tmp/kerberos.log",
        )

        mock_pid_file.mock_calls[0] = mock.call(mock_setup_locations.return_value[0], -1)
        assert mock_open.mock_calls == [
            mock.call(mock_setup_locations.return_value[1], "a"),
            mock.call().__enter__(),
            mock.call(mock_setup_locations.return_value[2], "a"),
            mock.call().__enter__(),
            mock.call().truncate(0),
            mock.call().truncate(0),
            mock.call().__exit__(None, None, None),
            mock.call().__exit__(None, None, None),
        ]

    @mock.patch("airflow.cli.commands.kerberos_command.krb")
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def test_run_command_with_mode_standard(self, mock_krb):
        args = self.parser.parse_args(["kerberos", "PRINCIPAL", "--keytab", "/tmp/airflow.keytab"])

        kerberos_command.kerberos(args)
        mock_krb.run.assert_called_once_with(
            keytab="/tmp/airflow.keytab", principal="PRINCIPAL", mode=KerberosMode.STANDARD
        )

    @mock.patch("airflow.cli.commands.kerberos_command.krb")
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def test_run_command_with_mode_one_time(self, mock_krb):
        args = self.parser.parse_args(
            ["kerberos", "PRINCIPAL", "--keytab", "/tmp/airflow.keytab", "--one-time"]
        )

        kerberos_command.kerberos(args)
        mock_krb.run.assert_called_once_with(
            keytab="/tmp/airflow.keytab", principal="PRINCIPAL", mode=KerberosMode.ONE_TIME
        )
