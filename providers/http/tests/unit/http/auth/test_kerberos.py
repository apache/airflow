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
import subprocess
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.http.auth.kerberos import KerberosAuth


class TestKerberosAuth:
    """Test KerberosAuth utility."""

    def test_init_requires_principal(self):
        """Test that principal is required."""
        with pytest.raises(AirflowException, match="Kerberos principal is required"):
            KerberosAuth(principal="", keytab="/path/to/keytab")

    def test_init_requires_keytab(self):
        """Test that keytab is required."""
        with pytest.raises(AirflowException, match="Kerberos keytab path is required"):
            KerberosAuth(principal="user@REALM", keytab="")

    def test_from_connection_params_success(self):
        """Test creating KerberosAuth from connection parameters."""
        auth = KerberosAuth.from_connection_params(
            principal="user@REALM",
            keytab="/path/to/keytab",
            ccache_dir="/tmp",
        )
        assert auth.principal == "user@REALM"
        assert auth.keytab == "/path/to/keytab"
        assert auth.ccache_dir == "/tmp"

    def test_from_connection_params_missing_principal(self):
        """Test from_connection_params fails without principal."""
        with pytest.raises(
            AirflowException,
            match="Both 'kerberos_principal' and 'kerberos_keytab' must be provided",
        ):
            KerberosAuth.from_connection_params(keytab="/path/to/keytab")

    def test_from_connection_params_missing_keytab(self):
        """Test from_connection_params fails without keytab."""
        with pytest.raises(
            AirflowException,
            match="Both 'kerberos_principal' and 'kerberos_keytab' must be provided",
        ):
            KerberosAuth.from_connection_params(principal="user@REALM")

    def test_generate_ccache_path(self):
        """Test credential cache path generation."""
        auth = KerberosAuth(principal="user@REALM", keytab="/path/to/keytab")
        path = auth._generate_ccache_path()

        assert path.startswith(tempfile.gettempdir())
        assert "krb5cc_airflow_task_" in path
        # Verify it's unique (contains UUID)
        assert len(os.path.basename(path)) > len("krb5cc_airflow_task_")

    def test_generate_ccache_path_with_custom_dir(self):
        """Test credential cache path generation with custom directory."""
        custom_dir = "/custom/tmp"
        auth = KerberosAuth(
            principal="user@REALM",
            keytab="/path/to/keytab",
            ccache_dir=custom_dir,
        )
        path = auth._generate_ccache_path()

        assert path.startswith(custom_dir)
        assert "krb5cc_airflow_task_" in path

    @mock.patch("subprocess.run")
    @mock.patch("os.path.isfile")
    def test_initialize_success(self, mock_isfile, mock_run):
        """Test successful Kerberos ticket initialization."""
        mock_isfile.return_value = True
        mock_run.return_value = mock.Mock(returncode=0, stdout="", stderr="")

        auth = KerberosAuth(
            principal="user@REALM",
            keytab="/path/to/keytab",
            forwardable=True,
            include_ip=False,
        )

        original_krb5ccname = os.environ.get("KRB5CCNAME")
        try:
            auth.initialize()

            # Verify kinit was called
            assert mock_run.called
            call_args = mock_run.call_args
            cmd = call_args[0][0]

            assert cmd[0] == "kinit"
            assert "-f" in cmd  # forwardable
            assert "-A" in cmd  # no IP addresses
            assert "-k" in cmd  # use keytab
            assert "-t" in cmd
            assert "/path/to/keytab" in cmd
            assert "-c" in cmd  # credential cache
            assert "user@REALM" in cmd

            # Verify ccache path was generated and environment variable set
            assert auth._ccache_path is not None
            assert os.environ.get("KRB5CCNAME") == auth._ccache_path

        finally:
            # Restore original environment
            if original_krb5ccname:
                os.environ["KRB5CCNAME"] = original_krb5ccname
            elif "KRB5CCNAME" in os.environ:
                del os.environ["KRB5CCNAME"]

    @mock.patch("os.path.isfile")
    def test_initialize_keytab_not_found(self, mock_isfile):
        """Test initialization fails when keytab file doesn't exist."""
        mock_isfile.return_value = False

        auth = KerberosAuth(principal="user@REALM", keytab="/nonexistent/keytab")

        with pytest.raises(AirflowException, match="Keytab file not found"):
            auth.initialize()

    @mock.patch("subprocess.run")
    @mock.patch("os.path.isfile")
    def test_initialize_kinit_failure(self, mock_isfile, mock_run):
        """Test initialization fails when kinit returns non-zero exit code."""
        mock_isfile.return_value = True
        mock_run.return_value = mock.Mock(
            returncode=1,
            stdout="",
            stderr="kinit: Cannot find KDC for realm",
        )

        auth = KerberosAuth(principal="user@REALM", keytab="/path/to/keytab")

        with pytest.raises(AirflowException, match="kinit failed with exit code 1"):
            auth.initialize()

    @mock.patch("subprocess.run")
    @mock.patch("os.path.isfile")
    def test_initialize_kinit_timeout(self, mock_isfile, mock_run):
        """Test initialization fails when kinit times out."""
        mock_isfile.return_value = True
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="kinit", timeout=30)

        auth = KerberosAuth(principal="user@REALM", keytab="/path/to/keytab")

        with pytest.raises(AirflowException, match="kinit command timed out"):
            auth.initialize()

    @mock.patch("subprocess.run")
    @mock.patch("os.path.isfile")
    def test_initialize_kinit_not_found(self, mock_isfile, mock_run):
        """Test initialization fails when kinit binary not found."""
        mock_isfile.return_value = True
        mock_run.side_effect = FileNotFoundError()

        auth = KerberosAuth(principal="user@REALM", keytab="/path/to/keytab")

        with pytest.raises(AirflowException, match="kinit binary not found"):
            auth.initialize()

    @mock.patch("os.remove")
    @mock.patch("os.path.exists")
    def test_cleanup(self, mock_exists, mock_remove):
        """Test credential cache cleanup."""
        mock_exists.return_value = True

        auth = KerberosAuth(principal="user@REALM", keytab="/path/to/keytab")
        auth._ccache_path = "/tmp/krb5cc_test"

        # Set environment variable
        os.environ["KRB5CCNAME"] = auth._ccache_path
        auth._original_krb5ccname = None

        auth.cleanup()

        # Verify cache file was removed
        mock_remove.assert_called_once_with("/tmp/krb5cc_test")

        # Verify environment variable was cleaned up
        assert "KRB5CCNAME" not in os.environ

    @mock.patch("os.remove")
    @mock.patch("os.path.exists")
    def test_cleanup_restores_original_krb5ccname(self, mock_exists, mock_remove):
        """Test cleanup restores original KRB5CCNAME."""
        mock_exists.return_value = True

        auth = KerberosAuth(principal="user@REALM", keytab="/path/to/keytab")
        auth._ccache_path = "/tmp/krb5cc_test"

        # Simulate having an original KRB5CCNAME
        original_value = "/tmp/krb5cc_original"
        auth._original_krb5ccname = original_value
        os.environ["KRB5CCNAME"] = auth._ccache_path

        auth.cleanup()

        # Verify original value was restored
        assert os.environ.get("KRB5CCNAME") == original_value

    @mock.patch("os.remove")
    @mock.patch("os.path.exists")
    def test_cleanup_handles_missing_file(self, mock_exists, mock_remove):
        """Test cleanup handles missing cache file gracefully."""
        mock_exists.return_value = False

        auth = KerberosAuth(principal="user@REALM", keytab="/path/to/keytab")
        auth._ccache_path = "/tmp/krb5cc_test"

        # Should not raise an exception
        auth.cleanup()

        # Verify remove was not called
        mock_remove.assert_not_called()

    @mock.patch("subprocess.run")
    @mock.patch("os.path.isfile")
    @mock.patch("os.remove")
    @mock.patch("os.path.exists")
    def test_context_manager(self, mock_exists, mock_remove, mock_isfile, mock_run):
        """Test context manager properly initializes and cleans up."""
        mock_isfile.return_value = True
        mock_run.return_value = mock.Mock(returncode=0, stdout="", stderr="")
        mock_exists.return_value = True

        auth = KerberosAuth(principal="user@REALM", keytab="/path/to/keytab")

        original_krb5ccname = os.environ.get("KRB5CCNAME")
        try:
            with auth.ticket() as ctx_auth:
                # Verify we're in initialized state
                assert ctx_auth._ccache_path is not None
                assert os.environ.get("KRB5CCNAME") == ctx_auth._ccache_path
                assert ctx_auth is auth

            # Verify cleanup was called
            mock_remove.assert_called_once()

        finally:
            # Restore original environment
            if original_krb5ccname:
                os.environ["KRB5CCNAME"] = original_krb5ccname
            elif "KRB5CCNAME" in os.environ:
                del os.environ["KRB5CCNAME"]

    @mock.patch("subprocess.run")
    @mock.patch("os.path.isfile")
    @mock.patch("os.remove")
    @mock.patch("os.path.exists")
    def test_context_manager_cleans_up_on_exception(
        self, mock_exists, mock_remove, mock_isfile, mock_run
    ):
        """Test context manager cleans up even when exception occurs."""
        mock_isfile.return_value = True
        mock_run.return_value = mock.Mock(returncode=0, stdout="", stderr="")
        mock_exists.return_value = True

        auth = KerberosAuth(principal="user@REALM", keytab="/path/to/keytab")

        original_krb5ccname = os.environ.get("KRB5CCNAME")
        try:
            with pytest.raises(ValueError, match="test exception"):
                with auth.ticket():
                    raise ValueError("test exception")

            # Verify cleanup was still called
            mock_remove.assert_called_once()

        finally:
            # Restore original environment
            if original_krb5ccname:
                os.environ["KRB5CCNAME"] = original_krb5ccname
            elif "KRB5CCNAME" in os.environ:
                del os.environ["KRB5CCNAME"]

    @mock.patch("airflow.providers.http.auth.kerberos.HTTPKerberosAuth")
    def test_get_requests_auth(self, mock_kerberos_auth):
        """Test getting requests-kerberos auth handler."""
        auth = KerberosAuth(principal="user@REALM", keytab="/path/to/keytab")

        result = auth.get_requests_auth()

        # Verify HTTPKerberosAuth was called with correct parameters
        mock_kerberos_auth.assert_called_once()
        call_kwargs = mock_kerberos_auth.call_args.kwargs
        assert "mutual_authentication" in call_kwargs

    @mock.patch("airflow.providers.http.auth.kerberos.HTTPKerberosAuth", None)
    def test_get_requests_auth_import_error(self):
        """Test get_requests_auth raises ImportError if requests-kerberos not installed."""
        auth = KerberosAuth(principal="user@REALM", keytab="/path/to/keytab")

        with mock.patch.dict("sys.modules", {"requests_kerberos": None}):
            with pytest.raises(ImportError, match="requests-kerberos is required"):
                auth.get_requests_auth()

    def test_ccache_path_property(self):
        """Test ccache_path property."""
        auth = KerberosAuth(principal="user@REALM", keytab="/path/to/keytab")

        # Initially None
        assert auth.ccache_path is None

        # After setting
        auth._ccache_path = "/tmp/krb5cc_test"
        assert auth.ccache_path == "/tmp/krb5cc_test"
