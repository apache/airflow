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

import logging
import os
import shlex
from contextlib import nullcontext
from unittest import mock

import pytest

from airflow.security import kerberos
from airflow.security.kerberos import renew_from_kt
from tests.test_utils.config import conf_vars


@pytest.mark.integration("kerberos")
class TestKerberosIntegration:
    @classmethod
    def setup_class(cls):
        assert "KRB5_KTNAME" in os.environ, "Missing KRB5_KTNAME environment variable"
        cls.keytab = os.environ["KRB5_KTNAME"]

    @pytest.mark.parametrize(
        "kerberos_config",
        [
            pytest.param({}, id="default-config"),
            pytest.param({("kerberos", "include_ip"): "True"}, id="explicit-include-ip"),
            pytest.param({("kerberos", "include_ip"): "False"}, id="explicit-not-include-ip"),
            pytest.param({("kerberos", "forwardable"): "True"}, id="explicit-forwardable"),
            pytest.param({("kerberos", "forwardable"): "False"}, id="explicit-not-forwardable"),
        ],
    )
    def test_renew_from_kt(self, kerberos_config):
        """We expect return 0 (exit code) and successful run."""
        with conf_vars(kerberos_config):
            assert renew_from_kt(principal=None, keytab=self.keytab) == 0

    @pytest.mark.parametrize(
        "exit_on_fail, expected_context",
        [
            pytest.param(True, pytest.raises(SystemExit), id="exit-on-fail"),
            pytest.param(False, nullcontext(), id="return-code-of-fail"),
        ],
    )
    def test_args_from_cli(self, exit_on_fail, expected_context, caplog):
        """Test exit code if keytab not exist."""
        keytab = "/not/exists/keytab"
        result = None

        with mock.patch.dict(os.environ, KRB5_KTNAME=keytab), conf_vars({("kerberos", "keytab"): keytab}):
            with expected_context as ctx:
                with caplog.at_level(logging.ERROR, logger=kerberos.log.name):
                    caplog.clear()
                    result = renew_from_kt(principal=None, keytab=keytab, exit_on_fail=exit_on_fail)

        # If `exit_on_fail` set to True than exit code in exception, otherwise in function return
        exit_code = ctx.value.code if exit_on_fail else result
        assert exit_code == 1
        assert caplog.record_tuples


class TestKerberos:
    @pytest.mark.parametrize(
        "kerberos_config, expected_cmd",
        [
            (
                {("kerberos", "reinit_frequency"): "42"},
                [
                    "kinit",
                    "-f",
                    "-a",
                    "-r",
                    "42m",
                    "-k",
                    "-t",
                    "keytab",
                    "-c",
                    "/tmp/airflow_krb5_ccache",
                    "test-principal",
                ],
            ),
            (
                {("kerberos", "forwardable"): "True", ("kerberos", "include_ip"): "True"},
                [
                    "kinit",
                    "-f",
                    "-a",
                    "-r",
                    "3600m",
                    "-k",
                    "-t",
                    "keytab",
                    "-c",
                    "/tmp/airflow_krb5_ccache",
                    "test-principal",
                ],
            ),
            (
                {("kerberos", "forwardable"): "False", ("kerberos", "include_ip"): "False"},
                [
                    "kinit",
                    "-F",
                    "-A",
                    "-r",
                    "3600m",
                    "-k",
                    "-t",
                    "keytab",
                    "-c",
                    "/tmp/airflow_krb5_ccache",
                    "test-principal",
                ],
            ),
        ],
    )
    @mock.patch("time.sleep", return_value=None)
    @mock.patch("airflow.security.kerberos.open", mock.mock_open(read_data=b"X-CACHECONF:"))
    @mock.patch("airflow.security.kerberos.NEED_KRB181_WORKAROUND", None)
    @mock.patch("airflow.security.kerberos.subprocess")
    def test_renew_from_kt(self, mock_subprocess, mock_sleep, kerberos_config, expected_cmd, caplog):
        expected_cmd_text = " ".join(shlex.quote(f) for f in expected_cmd)

        with conf_vars(kerberos_config), caplog.at_level(logging.INFO, logger=kerberos.log.name):
            caplog.clear()
            mock_subprocess.Popen.return_value.__enter__.return_value.returncode = 0
            mock_subprocess.call.return_value = 0
            renew_from_kt(principal="test-principal", keytab="keytab")

        assert caplog.messages == [
            f"Re-initialising kerberos from keytab: {expected_cmd_text}",
            "Renewing kerberos ticket to work around kerberos 1.8.1: kinit -c /tmp/airflow_krb5_ccache -R",
        ]

        assert mock_subprocess.Popen.call_args[0][0] == expected_cmd
        assert mock_subprocess.mock_calls == [
            mock.call.Popen(
                expected_cmd,
                bufsize=-1,
                close_fds=True,
                stderr=mock_subprocess.PIPE,
                stdout=mock_subprocess.PIPE,
                universal_newlines=True,
            ),
            mock.call.Popen().__enter__(),
            mock.call.Popen().__enter__().wait(),
            mock.call.Popen().__exit__(None, None, None),
            mock.call.call(["kinit", "-c", "/tmp/airflow_krb5_ccache", "-R"], close_fds=True),
        ]

    @mock.patch("airflow.security.kerberos.subprocess")
    @mock.patch("airflow.security.kerberos.NEED_KRB181_WORKAROUND", None)
    @mock.patch("airflow.security.kerberos.open", mock.mock_open(read_data=b""))
    def test_renew_from_kt_without_workaround(self, mock_subprocess, caplog):
        mock_subprocess.Popen.return_value.__enter__.return_value.returncode = 0
        mock_subprocess.call.return_value = 0

        with caplog.at_level(logging.INFO, logger=kerberos.log.name):
            caplog.clear()
            renew_from_kt(principal="test-principal", keytab="keytab")
            assert caplog.messages == [
                "Re-initialising kerberos from keytab: "
                "kinit -f -a -r 3600m -k -t keytab -c /tmp/airflow_krb5_ccache test-principal"
            ]

        assert mock_subprocess.mock_calls == [
            mock.call.Popen(
                [
                    "kinit",
                    "-f",
                    "-a",
                    "-r",
                    "3600m",
                    "-k",
                    "-t",
                    "keytab",
                    "-c",
                    "/tmp/airflow_krb5_ccache",
                    "test-principal",
                ],
                bufsize=-1,
                close_fds=True,
                stderr=mock_subprocess.PIPE,
                stdout=mock_subprocess.PIPE,
                universal_newlines=True,
            ),
            mock.call.Popen().__enter__(),
            mock.call.Popen().__enter__().wait(),
            mock.call.Popen().__exit__(None, None, None),
        ]

    @mock.patch("airflow.security.kerberos.subprocess")
    @mock.patch("airflow.security.kerberos.NEED_KRB181_WORKAROUND", None)
    def test_renew_from_kt_failed(self, mock_subprocess, caplog):
        mock_subp = mock_subprocess.Popen.return_value.__enter__.return_value
        mock_subp.returncode = 1
        mock_subp.stdout = mock.MagicMock(name="stdout", **{"readlines.return_value": ["STDOUT"]})
        mock_subp.stderr = mock.MagicMock(name="stderr", **{"readlines.return_value": ["STDERR"]})

        with pytest.raises(SystemExit) as ctx:
            caplog.clear()
            renew_from_kt(principal="test-principal", keytab="keytab")
        assert ctx.value.code == 1

        log_records = [record for record in caplog.record_tuples if record[0] == kerberos.log.name]
        assert len(log_records) == 2, log_records
        assert [lr[1] for lr in log_records] == [logging.INFO, logging.ERROR]
        assert [lr[2] for lr in log_records] == [
            "Re-initialising kerberos from keytab: "
            "kinit -f -a -r 3600m -k -t keytab -c /tmp/airflow_krb5_ccache test-principal",
            "Couldn't reinit from keytab! `kinit' exited with 1.\nSTDOUT\nSTDERR",
        ]

        assert mock_subprocess.mock_calls == [
            mock.call.Popen(
                [
                    "kinit",
                    "-f",
                    "-a",
                    "-r",
                    "3600m",
                    "-k",
                    "-t",
                    "keytab",
                    "-c",
                    "/tmp/airflow_krb5_ccache",
                    "test-principal",
                ],
                bufsize=-1,
                close_fds=True,
                stderr=mock_subprocess.PIPE,
                stdout=mock_subprocess.PIPE,
                universal_newlines=True,
            ),
            mock.call.Popen().__enter__(),
            mock.call.Popen().__enter__().wait(),
            mock.call.Popen().__exit__(mock.ANY, mock.ANY, mock.ANY),
        ]

    @mock.patch("airflow.security.kerberos.subprocess")
    @mock.patch("airflow.security.kerberos.NEED_KRB181_WORKAROUND", None)
    @mock.patch("airflow.security.kerberos.open", mock.mock_open(read_data=b"X-CACHECONF:"))
    @mock.patch("airflow.security.kerberos.get_hostname", return_value="HOST")
    @mock.patch("time.sleep", return_value=None)
    def test_renew_from_kt_failed_workaround(self, mock_sleep, mock_getfqdn, mock_subprocess, caplog):
        mock_subprocess.Popen.return_value.__enter__.return_value.returncode = 0
        mock_subprocess.call.return_value = 1

        with pytest.raises(SystemExit) as ctx:
            caplog.clear()
            renew_from_kt(principal="test-principal", keytab="keytab")
        assert ctx.value.code == 1

        log_records = [record for record in caplog.record_tuples if record[0] == kerberos.log.name]
        assert len(log_records) == 3, log_records
        assert [lr[1] for lr in log_records] == [logging.INFO, logging.INFO, logging.ERROR]
        assert [lr[2] for lr in log_records] == [
            "Re-initialising kerberos from keytab: "
            "kinit -f -a -r 3600m -k -t keytab -c /tmp/airflow_krb5_ccache test-principal",
            "Renewing kerberos ticket to work around kerberos 1.8.1: kinit -c /tmp/airflow_krb5_ccache -R",
            "Couldn't renew kerberos ticket in order to work around "
            "Kerberos 1.8.1 issue. Please check that the ticket for 'test-principal/HOST' is still "
            "renewable:\n  $ kinit -f -c /tmp/airflow_krb5_ccache\n"
            "If the 'renew until' date is the same as the 'valid starting' date, the ticket cannot be "
            "renewed. Please check your KDC configuration, and the ticket renewal policy (maxrenewlife) for "
            "the 'test-principal/HOST' and `krbtgt' principals.",
        ]

        assert mock_subprocess.mock_calls == [
            mock.call.Popen(
                [
                    "kinit",
                    "-f",
                    "-a",
                    "-r",
                    "3600m",
                    "-k",
                    "-t",
                    "keytab",
                    "-c",
                    "/tmp/airflow_krb5_ccache",
                    "test-principal",
                ],
                bufsize=-1,
                close_fds=True,
                stderr=mock_subprocess.PIPE,
                stdout=mock_subprocess.PIPE,
                universal_newlines=True,
            ),
            mock.call.Popen().__enter__(),
            mock.call.Popen().__enter__().wait(),
            mock.call.Popen().__exit__(None, None, None),
            mock.call.call(["kinit", "-c", "/tmp/airflow_krb5_ccache", "-R"], close_fds=True),
        ]

    def test_run_without_keytab(self, caplog):
        with pytest.raises(SystemExit) as ctx:
            with caplog.at_level(logging.WARNING, logger=kerberos.log.name):
                caplog.clear()
                kerberos.run(principal="test-principal", keytab=None)
        assert ctx.value.code == 0
        assert caplog.messages == ["Keytab renewer not starting, no keytab configured"]

    @mock.patch("airflow.security.kerberos.renew_from_kt")
    @mock.patch("time.sleep", return_value=None)
    def test_run(self, mock_sleep, mock_renew_from_kt):
        mock_renew_from_kt.side_effect = [1, 1, SystemExit(42)]
        with pytest.raises(SystemExit) as ctx:
            kerberos.run(principal="test-principal", keytab="/tmp/keytab")
        assert ctx.value.code == 42
        assert mock_renew_from_kt.mock_calls == [
            mock.call("test-principal", "/tmp/keytab"),
            mock.call("test-principal", "/tmp/keytab"),
            mock.call("test-principal", "/tmp/keytab"),
        ]
