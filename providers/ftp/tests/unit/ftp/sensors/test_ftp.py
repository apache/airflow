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

import ftplib
import socket
from ftplib import error_perm
from io import StringIO
from unittest import mock

import pytest

from airflow.providers.ftp.hooks.ftp import FTPHook, FTPSHook
from airflow.providers.ftp.sensors.ftp import FTPSensor, FTPSSensor


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

        mock_hook.return_value.__enter__.return_value.get_mod_time.side_effect = error_perm(
            "434: Host unavailable"
        )

        with pytest.raises(error_perm) as ctx:
            op.execute(None)

        assert "434" in str(ctx.value)

    @mock.patch("airflow.providers.ftp.sensors.ftp.FTPHook", spec=FTPHook)
    def test_poke_fail_on_transient_error_and_skip(self, mock_hook):
        op = FTPSensor(path="foobar.json", ftp_conn_id="bob_ftp", task_id="test_task")

        mock_hook.return_value.__enter__.return_value.get_mod_time.side_effect = error_perm(
            "434: Host unavailable"
        )

        with pytest.raises(error_perm):
            op.execute(None)

    @mock.patch("airflow.providers.ftp.sensors.ftp.FTPHook", spec=FTPHook)
    def test_poke_ignore_transient_error(self, mock_hook):
        op = FTPSensor(
            path="foobar.json", ftp_conn_id="bob_ftp", task_id="test_task", fail_on_transient_errors=False
        )

        mock_hook.return_value.__enter__.return_value.get_mod_time.side_effect = [
            error_perm("434: Host unavailable"),
            None,
        ]

        assert not op.poke(None)
        assert op.poke(None)

    @pytest.mark.parametrize(("sensor_cls", "hook_cls"), [(FTPSensor, FTPHook), (FTPSSensor, FTPSHook)])
    @pytest.mark.parametrize("code", [425, 450, 451])
    @mock.patch("airflow.providers.ftp.sensors.ftp.FTPHook", autospec=True)
    @mock.patch("airflow.providers.ftp.sensors.ftp.FTPSHook", autospec=True)
    def test_temporary_reply_respects_retry_policy(
        self, mock_ftps_hook, mock_ftp_hook, sensor_cls, hook_cls, code
    ):
        hook = mock.create_autospec(hook_cls, instance=True)
        hook.__enter__.return_value = hook
        hook_factory = mock_ftps_hook if sensor_cls is FTPSSensor else mock_ftp_hook
        hook_factory.return_value = hook
        hook.get_mod_time.side_effect = [ftplib.error_temp(f"{code} temporary failure"), "20260101000000"]
        sensor = sensor_cls(task_id="check", path="file", fail_on_transient_errors=False)
        assert sensor.poke({}) is False
        assert sensor.poke({}) is True
        hook_factory.assert_has_calls([mock.call(ftp_conn_id="ftp_default")] * 2, any_order=True)
        assert hook.get_mod_time.call_count == 2

    @pytest.mark.parametrize(("sensor_cls", "hook_cls"), [(FTPSensor, FTPHook), (FTPSSensor, FTPSHook)])
    @pytest.mark.parametrize(("fail_on_transient_errors", "code"), [(True, 425), (False, 430)])
    @mock.patch("airflow.providers.ftp.sensors.ftp.FTPHook", autospec=True)
    @mock.patch("airflow.providers.ftp.sensors.ftp.FTPSHook", autospec=True)
    def test_temporary_reply_keeps_raise_policy(
        self, mock_ftps_hook, mock_ftp_hook, sensor_cls, hook_cls, fail_on_transient_errors, code
    ):
        hook = mock.create_autospec(hook_cls, instance=True)
        hook.__enter__.return_value = hook
        hook_factory = mock_ftps_hook if sensor_cls is FTPSSensor else mock_ftp_hook
        hook_factory.return_value = hook
        hook.get_mod_time.side_effect = ftplib.error_temp(f"{code} temporary failure")
        sensor = sensor_cls(task_id="check", path="file", fail_on_transient_errors=fail_on_transient_errors)
        with pytest.raises(ftplib.error_temp, match=str(code)):
            sensor.poke({})

    @pytest.mark.parametrize(
        ("sensor_cls", "hook_cls", "client_cls"),
        [(FTPSensor, FTPHook, ftplib.FTP), (FTPSSensor, FTPSHook, ftplib.FTP_TLS)],
    )
    def test_poke_retries_temporary_reply_from_ftp_client(self, sensor_cls, hook_cls, client_cls):
        sensor = sensor_cls(task_id="check", path="file", fail_on_transient_errors=False)
        hook = hook_cls()
        for reply, expected in [("450 File unavailable", False), ("213 20261003120000", True)]:
            with client_cls() as client:
                sock = mock.create_autospec(socket.socket, instance=True)
                client.sock = sock
                client.file = StringIO(reply + "\r\n221 Goodbye\r\n")
                hook.conn = client
                with mock.patch.object(sensor_cls, "_create_hook", autospec=True, return_value=hook):
                    assert sensor.poke({}) is expected
                sock.sendall.assert_has_calls([mock.call(b"MDTM file\r\n"), mock.call(b"QUIT\r\n")])
                assert hook.conn is None
