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

import os
import shlex
import unittest
from argparse import Namespace
from unittest import mock

import pytest
from parameterized import parameterized

from airflow.security import kerberos
from airflow.security.kerberos import renew_from_kt
from tests.test_utils.config import conf_vars

KRB5_KTNAME = os.environ.get("KRB5_KTNAME")


@unittest.skipIf(KRB5_KTNAME is None, "Skipping Kerberos API tests due to missing KRB5_KTNAME")
class TestKerberos(unittest.TestCase):
    def setUp(self):
        self.args = Namespace(
            keytab=KRB5_KTNAME, principal=None, pid=None, daemon=None, stdout=None, stderr=None, log_file=None
        )

    @conf_vars({("kerberos", "keytab"): KRB5_KTNAME})
    def test_renew_from_kt(self):
        """
        We expect no result, but a successful run. No more TypeError
        """
        assert renew_from_kt(principal=self.args.principal, keytab=self.args.keytab) is None

    @conf_vars({("kerberos", "keytab"): KRB5_KTNAME, ("kerberos", "include_ip"): ""})
    def test_renew_from_kt_include_ip_empty(self):
        """
        We expect no result, but a successful run.
        """
        assert renew_from_kt(principal=self.args.principal, keytab=self.args.keytab) is None

    @conf_vars({("kerberos", "keytab"): KRB5_KTNAME, ("kerberos", "include_ip"): "False"})
    def test_renew_from_kt_include_ip_false(self):
        """
        We expect no result, but a successful run.
        """
        assert renew_from_kt(principal=self.args.principal, keytab=self.args.keytab) is None

    @conf_vars({("kerberos", "keytab"): KRB5_KTNAME, ("kerberos", "include_ip"): "True"})
    def test_renew_from_kt_include_ip_true(self):
        """
        We expect no result, but a successful run.
        """
        assert renew_from_kt(principal=self.args.principal, keytab=self.args.keytab) is None

    # Validate forwardable kerberos option
    @conf_vars({("kerberos", "keytab"): KRB5_KTNAME, ("kerberos", "forwardable"): ""})
    def test_renew_from_kt_forwardable_empty(self):
        """
        We expect no result, but a successful run.
        """
        assert renew_from_kt(principal=self.args.principal, keytab=self.args.keytab) is None

    @conf_vars({("kerberos", "keytab"): KRB5_KTNAME, ("kerberos", "forwardable"): "False"})
    def test_renew_from_kt_forwardable_false(self):
        """
        We expect no result, but a successful run.
        """
        assert renew_from_kt(principal=self.args.principal, keytab=self.args.keytab) is None

    @conf_vars({("kerberos", "keytab"): KRB5_KTNAME, ("kerberos", "forwardable"): "True"})
    def test_renew_from_kt_forwardable_true(self):
        """
        We expect no result, but a successful run.
        """
        assert renew_from_kt(principal=self.args.principal, keytab=self.args.keytab) is None

    @conf_vars({("kerberos", "keytab"): ""})
    def test_args_from_cli(self):
        """
        We expect no result, but a run with sys.exit(1) because keytab not exist.
        """
        with pytest.raises(SystemExit) as ctx:
            renew_from_kt(principal=self.args.principal, keytab=self.args.keytab)

            with self.assertLogs(kerberos.log) as log:
                assert (
                    f"kinit: krb5_init_creds_set_keytab: Failed to find airflow@LUPUS.GRIDDYNAMICS.NET in "
                    f"keytab FILE:{self.args.keytab} (unknown enctype)" in log.output
                )

            assert ctx.value.code == 1


class TestKerberosUnit(unittest.TestCase):
    @parameterized.expand(
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
        ]
    )
    def test_renew_from_kt(self, kerberos_config, expected_cmd):
        with self.assertLogs(kerberos.log) as log_ctx, conf_vars(kerberos_config), mock.patch(
            "airflow.security.kerberos.subprocess"
        ) as mock_subprocess, mock.patch(
            "airflow.security.kerberos.NEED_KRB181_WORKAROUND", None
        ), mock.patch(
            "airflow.security.kerberos.open", mock.mock_open(read_data=b"X-CACHECONF:")
        ), mock.patch(
            "time.sleep", return_value=None
        ):
            mock_subprocess.Popen.return_value.__enter__.return_value.returncode = 0
            mock_subprocess.call.return_value = 0
            renew_from_kt(principal="test-principal", keytab="keytab")

        assert mock_subprocess.Popen.call_args[0][0] == expected_cmd

        expected_cmd_text = " ".join(shlex.quote(f) for f in expected_cmd)
        assert log_ctx.output == [
            f"INFO:airflow.security.kerberos:Re-initialising kerberos from keytab: {expected_cmd_text}",
            "INFO:airflow.security.kerberos:Renewing kerberos ticket to work around kerberos 1.8.1: "
            "kinit -c /tmp/airflow_krb5_ccache -R",
        ]

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
    def test_renew_from_kt_without_workaround(self, mock_subprocess):
        mock_subprocess.Popen.return_value.__enter__.return_value.returncode = 0
        mock_subprocess.call.return_value = 0

        with self.assertLogs(kerberos.log) as log_ctx:
            renew_from_kt(principal="test-principal", keytab="keytab")

        assert log_ctx.output == [
            "INFO:airflow.security.kerberos:Re-initialising kerberos from keytab: "
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
    def test_renew_from_kt_failed(self, mock_subprocess):
        mock_subp = mock_subprocess.Popen.return_value.__enter__.return_value
        mock_subp.returncode = 1
        mock_subp.stdout = mock.MagicMock(name="stdout", **{"readlines.return_value": ["STDOUT"]})
        mock_subp.stderr = mock.MagicMock(name="stderr", **{"readlines.return_value": ["STDERR"]})

        with self.assertLogs(kerberos.log) as log_ctx, self.assertRaises(SystemExit):
            renew_from_kt(principal="test-principal", keytab="keytab")

        assert log_ctx.output == [
            "INFO:airflow.security.kerberos:Re-initialising kerberos from keytab: "
            "kinit -f -a -r 3600m -k -t keytab -c /tmp/airflow_krb5_ccache test-principal",
            "ERROR:airflow.security.kerberos:Couldn't reinit from keytab! `kinit' exited with 1.\n"
            "STDOUT\n"
            "STDERR",
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
    def test_renew_from_kt_failed_workaround(self, mock_sleep, mock_getfqdn, mock_subprocess):
        mock_subprocess.Popen.return_value.__enter__.return_value.returncode = 0
        mock_subprocess.call.return_value = 1

        with self.assertLogs(kerberos.log) as log_ctx, self.assertRaises(SystemExit):
            renew_from_kt(principal="test-principal", keytab="keytab")

        assert log_ctx.output == [
            "INFO:airflow.security.kerberos:Re-initialising kerberos from keytab: "
            "kinit -f -a -r 3600m -k -t keytab -c /tmp/airflow_krb5_ccache test-principal",
            "INFO:airflow.security.kerberos:Renewing kerberos ticket to work around kerberos 1.8.1: "
            "kinit -c /tmp/airflow_krb5_ccache -R",
            "ERROR:airflow.security.kerberos:Couldn't renew kerberos ticket in order to work around "
            "Kerberos 1.8.1 issue. Please check that the ticket for 'test-principal/HOST' is still "
            "renewable:\n"
            "  $ kinit -f -c /tmp/airflow_krb5_ccache\n"
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

    def test_run_without_keytab(self):
        with self.assertLogs(kerberos.log) as log_ctx, self.assertRaises(SystemExit):
            kerberos.run(principal="test-principal", keytab=None)
        assert log_ctx.output == [
            "WARNING:airflow.security.kerberos:Keytab renewer not starting, no keytab configured"
        ]

    @mock.patch("airflow.security.kerberos.renew_from_kt")
    @mock.patch("time.sleep", return_value=None)
    def test_run(self, mock_sleep, mock_renew_from_kt):
        mock_renew_from_kt.side_effect = [1, 1, SystemExit(42)]
        with self.assertRaises(SystemExit):
            kerberos.run(principal="test-principal", keytab="/tmp/keytab")
        assert mock_renew_from_kt.mock_calls == [
            mock.call("test-principal", "/tmp/keytab"),
            mock.call("test-principal", "/tmp/keytab"),
            mock.call("test-principal", "/tmp/keytab"),
        ]
