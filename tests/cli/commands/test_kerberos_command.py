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

from airflow.cli import cli_parser
from airflow.cli.commands import kerberos_command
from tests.test_utils.config import conf_vars


class TestKerberosCommand(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch('airflow.cli.commands.kerberos_command.krb')
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def test_run_command(self, mock_krb):
        args = self.parser.parse_args(['kerberos', 'PRINCIPAL', '--keytab', '/tmp/airflow.keytab'])

        kerberos_command.kerberos(args)
        mock_krb.run.assert_called_once_with(keytab='/tmp/airflow.keytab', principal='PRINCIPAL')

    @mock.patch('airflow.cli.commands.kerberos_command.TimeoutPIDLockFile')
    @mock.patch('airflow.cli.commands.kerberos_command.setup_locations')
    @mock.patch('airflow.cli.commands.kerberos_command.daemon')
    @mock.patch('airflow.cli.commands.kerberos_command.krb')
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def test_run_command_daemon(self, mock_krb, mock_daemon, mock_setup_locations, mock_pid_file):
        mock_setup_locations.return_value = (
            mock.MagicMock(name='pidfile'),
            mock.MagicMock(name='stdout'),
            mock.MagicMock(name='stderr'),
            mock.MagicMock(name="INVALID"),
        )
        args = self.parser.parse_args(
            [
                'kerberos',
                'PRINCIPAL',
                '--keytab',
                '/tmp/airflow.keytab',
                '--log-file',
                '/tmp/kerberos.log',
                '--pid',
                '/tmp/kerberos.pid',
                '--stderr',
                '/tmp/kerberos-stderr.log',
                '--stdout',
                '/tmp/kerberos-stdout.log',
                '--daemon',
            ]
        )
        mock_open = mock.mock_open()
        with mock.patch('airflow.cli.commands.kerberos_command.open', mock_open):
            kerberos_command.kerberos(args)

        mock_krb.run.assert_called_once_with(keytab='/tmp/airflow.keytab', principal='PRINCIPAL')
        assert mock_daemon.mock_calls == [
            mock.call.DaemonContext(
                pidfile=mock_pid_file.return_value,
                stderr=mock_open.return_value,
                stdout=mock_open.return_value,
            ),
            mock.call.DaemonContext().__enter__(),
            mock.call.DaemonContext().__exit__(None, None, None),
        ]

        mock_setup_locations.assert_has_calls(
            [
                mock.call(
                    'kerberos',
                    '/tmp/kerberos.pid',
                    '/tmp/kerberos-stdout.log',
                    '/tmp/kerberos-stderr.log',
                    '/tmp/kerberos.log',
                )
            ]
        )
        mock_pid_file.assert_has_calls([mock.call(mock_setup_locations.return_value[0], -1)])
        assert mock_open.mock_calls == [
            mock.call(mock_setup_locations.return_value[1], 'a'),
            mock.call().__enter__(),
            mock.call(mock_setup_locations.return_value[2], 'a'),
            mock.call().__enter__(),
            mock.call().truncate(0),
            mock.call().truncate(0),
            mock.call().__exit__(None, None, None),
            mock.call().__exit__(None, None, None),
        ]
