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

import socket
from ftplib import FTP, error_perm, error_temp
from io import StringIO
from unittest import mock

import pytest

from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.ftp.sensors.ftp import FTPSensor


class TestFTPSensor:
    @mock.patch("airflow.providers.ftp.sensors.ftp.FTPHook", spec=FTPHook)
    def test_poke(self, mock_hook):
        op = FTPSensor(path="foobar.json", ftp_conn_id="bob_ftp", task_id="test_task")

        mock_hook.return_value.__enter__.return_value.get_mod_time.side_effect = [
            error_perm("550: Can't check for file existence"),
            error_perm("550: Directory or file does not exist"),
            error_perm("550 - Directory or file does not exist"),
            None,
        ]

        assert not op.poke(None)
        assert not op.poke(None)
        assert not op.poke(None)
        assert op.poke(None)

    @mock.patch("airflow.providers.ftp.sensors.ftp.FTPHook", spec=FTPHook)
    def test_poke_fails_due_error(self, mock_hook):
        op = FTPSensor(path="foobar.json", ftp_conn_id="bob_ftp", task_id="test_task")

        mock_hook.return_value.__enter__.return_value.get_mod_time.side_effect = error_perm(
            "530: Login authentication failed"
        )

        with pytest.raises(error_perm) as ctx:
            op.execute(None)

        assert "530" in str(ctx.value)

    @mock.patch("airflow.providers.ftp.sensors.ftp.FTPHook", spec=FTPHook)
    def test_poke_fail_on_transient_error(self, mock_hook):
        op = FTPSensor(path="foobar.json", ftp_conn_id="bob_ftp", task_id="test_task")

        mock_hook.return_value.__enter__.return_value.get_mod_time.side_effect = error_temp(
            "434: Host unavailable"
        )

        with pytest.raises(error_temp) as ctx:
            op.execute(None)

        assert "434" in str(ctx.value)

    @mock.patch("airflow.providers.ftp.sensors.ftp.FTPHook", spec=FTPHook)
    def test_poke_fail_on_transient_error_and_skip(self, mock_hook):
        op = FTPSensor(path="foobar.json", ftp_conn_id="bob_ftp", task_id="test_task")

        mock_hook.return_value.__enter__.return_value.get_mod_time.side_effect = error_temp(
            "434: Host unavailable"
        )

        with pytest.raises(error_temp):
            op.execute(None)

    @mock.patch("airflow.providers.ftp.sensors.ftp.FTPHook", spec=FTPHook)
    def test_poke_ignore_transient_error(self, mock_hook):
        op = FTPSensor(
            path="foobar.json", ftp_conn_id="bob_ftp", task_id="test_task", fail_on_transient_errors=False
        )

        mock_hook.return_value.__enter__.return_value.get_mod_time.side_effect = [
            error_temp("434: Host unavailable"),
            None,
        ]

        assert not op.poke(None)
        assert op.poke(None)

    @pytest.mark.parametrize(
        ("fail_on_transient_errors", "reply", "expected_error"),
        [
            (False, "421 Service unavailable, closing control connection", None),
            (True, "421 Service unavailable, closing control connection", error_temp),
            (False, "430 Unknown temporary reply", error_temp),
            (False, "530 Authentication failed", error_perm),
        ],
    )
    def test_poke_after_disconnected_control_connection(
        self, fail_on_transient_errors, reply, expected_error
    ):
        sensor = FTPSensor(task_id="check", path="file", fail_on_transient_errors=fail_on_transient_errors)
        hook = FTPHook()
        client = FTP()
        sock = mock.create_autospec(socket.socket, instance=True)
        client.sock = sock
        client.file = StringIO(reply + "\r\n")
        hook.conn = client
        try:
            with mock.patch.object(FTPSensor, "_create_hook", autospec=True, return_value=hook):
                if expected_error is None:
                    assert sensor.poke({}) is False
                else:
                    with pytest.raises(expected_error, match=reply):
                        sensor.poke({})

            sock.sendall.assert_has_calls([mock.call(b"MDTM file\r\n"), mock.call(b"QUIT\r\n")])
            sock.close.assert_called_once_with()
            assert client.sock is None
            assert client.file is None
            assert hook.conn is None
        finally:
            client.close()
