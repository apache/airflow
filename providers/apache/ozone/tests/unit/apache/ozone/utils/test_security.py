#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.apache.ozone.utils.connection_schema import OzoneConnSnapshot
from airflow.providers.apache.ozone.utils.security import (
    KerberosConfig,
    SecretResolver,
    SSLConfig,
)


class TestGetSecretValue:
    """Tests for get_secret_value."""

    @patch("airflow.providers.apache.ozone.utils.security.ensure_secrets_loaded")
    def test_resolves_secret_uri_from_backend(self, mock_ensure_loaded):
        mock_backend = MagicMock()
        mock_backend.get_config.return_value = "resolved_secret"
        mock_ensure_loaded.return_value = [mock_backend]

        result = SecretResolver.get_secret_value("secret://vault/ozone/password", conn_id="ozone_default")

        assert result == "resolved_secret"
        mock_backend.get_config.assert_called_once_with("secret://vault/ozone/password")

    @patch("airflow.providers.apache.ozone.utils.security.ensure_secrets_loaded")
    def test_secret_not_found_raises(self, mock_ensure_loaded):
        mock_backend = MagicMock()
        mock_backend.get_config.return_value = None
        mock_ensure_loaded.return_value = [mock_backend]

        with pytest.raises(ValueError, match="Secret not found"):
            SecretResolver.get_secret_value("secret://missing/uri")

    def test_get_secret_value_masks_plain_value_by_default(self, monkeypatch):
        masked: dict[str, object] = {}

        def _capture_masked(value: object) -> None:
            masked["value"] = value

        monkeypatch.setattr("airflow.providers.apache.ozone.utils.security.mask_secret", _capture_masked)
        result = SecretResolver.get_secret_value("plain_secret")
        assert result == "plain_secret"
        assert masked["value"] == "plain_secret"


class TestGetKerberosEnvVars:
    """Tests for KerberosConfig.get_env_vars."""

    def test_empty_snapshot_returns_empty(self):
        snapshot = OzoneConnSnapshot(host="om", port=9862)
        assert KerberosConfig.get_env_vars(snapshot, scope="ozone") == {}

    def test_ozone_kerberos_from_snapshot(self):
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            hadoop_security_authentication="kerberos",
            kerberos_principal="user@REALM",
            kerberos_keytab="/etc/keytab/user.keytab",
        )
        result = KerberosConfig.get_env_vars(snapshot, scope="ozone")
        assert result["HADOOP_SECURITY_AUTHENTICATION"] == "kerberos"
        assert result["KERBEROS_PRINCIPAL"] == "user@REALM"
        assert result["KERBEROS_KEYTAB"] == "/etc/keytab/user.keytab"

    def test_ozone_kerberos_password_is_not_exported_to_env(self):
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            hadoop_security_authentication="kerberos",
            kerberos_principal="user@REALM",
            kerberos_password="secret_password",
        )
        result = KerberosConfig.get_env_vars(snapshot, scope="ozone")
        assert result["HADOOP_SECURITY_AUTHENTICATION"] == "kerberos"
        assert result["KERBEROS_PRINCIPAL"] == "user@REALM"
        assert "KERBEROS_PASSWORD" not in result

    def test_ozone_scope_does_not_include_hdfs_kerberos_env(self):
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            hadoop_security_authentication="kerberos",
            kerberos_principal="user@REALM",
            kerberos_keytab="/etc/keytab/user.keytab",
            hdfs_kerberos_enabled=True,
            hdfs_kerberos_principal="hdfs@REALM",
            hdfs_kerberos_keytab="/etc/keytab/hdfs.keytab",
        )
        result = KerberosConfig.get_env_vars(snapshot, scope="ozone")
        assert "HDFS_KERBEROS_PRINCIPAL" not in result
        assert "HDFS_KERBEROS_KEYTAB" not in result

    def test_hdfs_scope_does_not_include_ozone_kerberos_env(self):
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            hadoop_security_authentication="kerberos",
            kerberos_principal="user@REALM",
            kerberos_keytab="/etc/keytab/user.keytab",
            hdfs_kerberos_enabled=True,
            hdfs_kerberos_principal="hdfs@REALM",
            hdfs_kerberos_keytab="/etc/keytab/hdfs.keytab",
        )
        result = KerberosConfig.get_env_vars(snapshot, scope="hdfs")
        assert "HADOOP_SECURITY_AUTHENTICATION" not in result
        assert "KERBEROS_PRINCIPAL" not in result
        assert result["HDFS_KERBEROS_PRINCIPAL"] == "hdfs@REALM"
        assert result["HDFS_KERBEROS_KEYTAB"] == "/etc/keytab/hdfs.keytab"

    def test_hdfs_kerberos_password_is_not_exported_to_env(self):
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            hdfs_kerberos_enabled=True,
            hdfs_kerberos_principal="hdfs@REALM",
            hdfs_kerberos_password="secret_password",
        )
        result = KerberosConfig.get_env_vars(snapshot, scope="hdfs")
        assert result["HDFS_KERBEROS_PRINCIPAL"] == "hdfs@REALM"
        assert "HDFS_KERBEROS_PASSWORD" not in result


class TestApplySslEnvVars:
    """Tests for SSLConfig.apply_ssl_env_vars."""

    def test_merges_into_existing(self):
        overrides = {"A": "1"}
        existing = {"B": "2"}
        result = SSLConfig.apply_ssl_env_vars(overrides, existing)
        assert result == {"A": "1", "B": "2"}

    def test_scope_ozone_does_not_include_hdfs_ssl_env(self):
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            ozone_security_enabled=True,
            ozone_om_https_port="9879",
            hdfs_ssl_enabled=True,
            hdfs_ssl_keystore_location="/etc/hdfs.ks",
        )
        config = SSLConfig.from_snapshot(snapshot, scope="ozone")
        env = config.as_env()
        assert env["OZONE_SECURITY_ENABLED"] == "true"
        assert "HDFS_SSL_ENABLED" not in env

    def test_scope_hdfs_does_not_include_ozone_ssl_env(self):
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            ozone_security_enabled=True,
            ozone_om_https_port="9879",
            hdfs_ssl_enabled=True,
            hdfs_ssl_keystore_location="/etc/hdfs.ks",
        )
        config = SSLConfig.from_snapshot(snapshot, scope="hdfs")
        env = config.as_env()
        assert env["HDFS_SSL_ENABLED"] == "true"
        assert "OZONE_SECURITY_ENABLED" not in env


class TestApplyKerberosEnvVars:
    """Tests for KerberosConfig.apply_env_vars."""

    def test_sets_hadoop_opts_when_kerberos_enabled(self):
        env_vars = {"HADOOP_SECURITY_AUTHENTICATION": "kerberos"}
        result = KerberosConfig.apply_env_vars(env_vars)
        assert "-Dhadoop.security.authentication=kerberos" in result.get("HADOOP_OPTS", "")
        assert "-Dhadoop.security.authentication=kerberos" in result.get("OZONE_OPTS", "")

    def test_reuses_existing_jvm_opts_when_kerberos_enabled(self):
        env_vars = {"HADOOP_SECURITY_AUTHENTICATION": "kerberos"}
        existing = {
            "HADOOP_OPTS": "-Dexisting.hadoop=true",
            "OZONE_OPTS": "-Dexisting.ozone=true",
        }
        result = KerberosConfig.apply_env_vars(env_vars, existing)
        assert "-Dexisting.hadoop=true" in result["HADOOP_OPTS"]
        assert "-Dhadoop.security.authentication=kerberos" in result["HADOOP_OPTS"]
        assert "-Dexisting.ozone=true" in result["OZONE_OPTS"]
        assert "-Dhadoop.security.authentication=kerberos" in result["OZONE_OPTS"]
        assert "-Dozone.security.enabled=true" in result["OZONE_OPTS"]

    def test_reuses_explicit_existing_ozone_conf_dir_when_kerberos_enabled(self):
        env_vars = {"HADOOP_SECURITY_AUTHENTICATION": "kerberos"}
        result = KerberosConfig.apply_env_vars(
            env_vars,
            existing_env={"OZONE_CONF_DIR": "/opt/airflow/ozone-conf"},
        )
        assert result["OZONE_CONF_DIR"] == "/opt/airflow/ozone-conf"
        assert result["HADOOP_CONF_DIR"] == "/opt/airflow/ozone-conf"

    def test_reuses_process_env_when_existing_env_is_not_provided(self, monkeypatch):
        env_vars = {"HADOOP_SECURITY_AUTHENTICATION": "kerberos"}
        monkeypatch.setenv("OZONE_CONF_DIR", "/env/ozone-conf")
        monkeypatch.setenv("HADOOP_OPTS", "-Dexisting.hadoop=true")
        monkeypatch.setenv("OZONE_OPTS", "-Dexisting.ozone=true")

        result = KerberosConfig.apply_env_vars(env_vars)

        assert result["OZONE_CONF_DIR"] == "/env/ozone-conf"
        assert result["HADOOP_CONF_DIR"] == "/env/ozone-conf"
        assert "-Dexisting.hadoop=true" in result["HADOOP_OPTS"]
        assert "-Dhadoop.security.authentication=kerberos" in result["HADOOP_OPTS"]
        assert "-Dexisting.ozone=true" in result["OZONE_OPTS"]
        assert "-Dhadoop.security.authentication=kerberos" in result["OZONE_OPTS"]

    def test_kerberos_helpers_detect_enabled_state_and_config_dir(self):
        kerberos_env = {
            "HADOOP_SECURITY_AUTHENTICATION": "kerberos",
            "OZONE_CONF_DIR": "/opt/airflow/ozone-conf",
        }

        assert KerberosConfig.is_enabled(kerberos_env)
        assert KerberosConfig.resolve_config_dir(kerberos_env) == "/opt/airflow/ozone-conf"
        assert not KerberosConfig.is_enabled(None)
        assert KerberosConfig.resolve_config_dir(None) is None


class TestSnapshotDrivenKerberosRuntime:
    @patch("airflow.providers.apache.ozone.utils.security.FileHelper.is_readable_file", return_value=True)
    @patch("airflow.providers.apache.ozone.utils.security.KerberosCliRunner.run_kerberos")
    def test_kinit_timeout_from_snapshot(self, mock_run_kerberos, _mock_readable):
        mock_run_kerberos.return_value = True
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            kinit_timeout_seconds=42,
        )
        assert KerberosConfig.kinit_with_keytab(
            "user@REALM",
            "/tmp/user.keytab",
            "/tmp/krb5.conf",
            snapshot=snapshot,
        )
        assert mock_run_kerberos.call_args.kwargs["timeout"] == 42

    @patch("airflow.providers.apache.ozone.utils.security.FileHelper.is_readable_file", return_value=True)
    @patch("airflow.providers.apache.ozone.utils.security.KerberosCliRunner.run_kerberos")
    def test_kinit_with_password_uses_stdin(self, mock_run_kerberos, _mock_readable):
        mock_run_kerberos.return_value = True
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            kinit_timeout_seconds=42,
        )
        assert KerberosConfig.kinit_with_password(
            "user@REALM",
            "secret_password",
            "/tmp/krb5.conf",
            snapshot=snapshot,
        )
        mock_run_kerberos.assert_called_once_with(
            ["kinit", "user@REALM"],
            env_overrides={"KRB5_CONFIG": "/tmp/krb5.conf"},
            timeout=42,
            input_text="secret_password\n",
        )

    @patch("airflow.providers.apache.ozone.utils.security.KerberosConfig.kinit_with_password")
    @patch("airflow.providers.apache.ozone.utils.security.KerberosConfig.kinit_with_keytab")
    def test_kinit_from_snapshot_prefers_keytab_when_both_are_configured(
        self,
        mock_kinit_with_keytab: MagicMock,
        mock_kinit_with_password: MagicMock,
    ):
        mock_kinit_with_keytab.return_value = True
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            kerberos_principal="user@REALM",
            kerberos_keytab="/tmp/user.keytab",
            kerberos_password="secret_password",
        )
        assert KerberosConfig.kinit_from_snapshot(
            snapshot=snapshot,
        )
        mock_kinit_with_keytab.assert_called_once_with(
            "user@REALM",
            "/tmp/user.keytab",
            None,
            snapshot=snapshot,
        )
        mock_kinit_with_password.assert_not_called()

    @patch("airflow.providers.apache.ozone.utils.security.KerberosConfig.kinit_with_password")
    def test_kinit_from_snapshot_uses_password_without_keytab(
        self,
        mock_kinit_with_password: MagicMock,
    ):
        mock_kinit_with_password.return_value = True
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            kerberos_principal="user@REALM",
            kerberos_password="secret_password",
        )
        assert KerberosConfig.kinit_from_snapshot(
            snapshot=snapshot,
        )
        mock_kinit_with_password.assert_called_once_with(
            "user@REALM",
            "secret_password",
            None,
            snapshot=snapshot,
        )

    @patch("airflow.providers.apache.ozone.utils.security.KerberosConfig.kinit_with_keytab")
    def test_kinit_from_snapshot_uses_snapshot_keytab_and_krb5_conf(
        self,
        mock_kinit_with_keytab: MagicMock,
    ):
        mock_kinit_with_keytab.return_value = True
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            kerberos_principal="user@REALM",
            kerberos_keytab="/tmp/user.keytab",
            krb5_conf="/tmp/krb5.conf",
        )
        assert KerberosConfig.kinit_from_snapshot(snapshot=snapshot)
        mock_kinit_with_keytab.assert_called_once_with(
            "user@REALM",
            "/tmp/user.keytab",
            "/tmp/krb5.conf",
            snapshot=snapshot,
        )

    @patch("airflow.providers.apache.ozone.utils.security.KerberosConfig.kinit_with_keytab")
    def test_kinit_hdfs_from_snapshot_uses_hdfs_snapshot_credentials(
        self,
        mock_kinit_with_keytab: MagicMock,
    ):
        mock_kinit_with_keytab.return_value = True
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            hdfs_kerberos_enabled=True,
            hdfs_kerberos_principal="hdfs@REALM",
            hdfs_kerberos_keytab="/tmp/hdfs.keytab",
            krb5_conf="/tmp/krb5.conf",
        )
        assert KerberosConfig.kinit_hdfs_from_snapshot(snapshot=snapshot, conn_id="hdfs_admin_default")
        mock_kinit_with_keytab.assert_called_once_with(
            "hdfs@REALM",
            "/tmp/hdfs.keytab",
            "/tmp/krb5.conf",
            snapshot=snapshot,
        )

    @patch("airflow.providers.apache.ozone.utils.security.KerberosConfig.kinit_with_password")
    @patch("airflow.providers.apache.ozone.utils.security.KerberosConfig.kinit_with_keytab")
    def test_kinit_hdfs_from_snapshot_prefers_keytab_when_both_are_configured(
        self,
        mock_kinit_with_keytab: MagicMock,
        mock_kinit_with_password: MagicMock,
    ):
        mock_kinit_with_keytab.return_value = True
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            hdfs_kerberos_enabled=True,
            hdfs_kerberos_principal="hdfs@REALM",
            hdfs_kerberos_keytab="/tmp/hdfs.keytab",
            hdfs_kerberos_password="secret_password",
        )
        assert KerberosConfig.kinit_hdfs_from_snapshot(snapshot=snapshot, conn_id="hdfs_admin_default")
        mock_kinit_with_keytab.assert_called_once_with(
            "hdfs@REALM",
            "/tmp/hdfs.keytab",
            None,
            snapshot=snapshot,
        )
        mock_kinit_with_password.assert_not_called()

    @patch("airflow.providers.apache.ozone.utils.security.KerberosConfig.kinit_with_password")
    def test_kinit_hdfs_from_snapshot_uses_password_without_keytab(
        self,
        mock_kinit_with_password: MagicMock,
    ):
        mock_kinit_with_password.return_value = True
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            hdfs_kerberos_enabled=True,
            hdfs_kerberos_principal="hdfs@REALM",
            hdfs_kerberos_password="secret_password",
            krb5_conf="/tmp/krb5.conf",
        )
        assert KerberosConfig.kinit_hdfs_from_snapshot(snapshot=snapshot, conn_id="hdfs_admin_default")
        mock_kinit_with_password.assert_called_once_with(
            "hdfs@REALM",
            "secret_password",
            "/tmp/krb5.conf",
            snapshot=snapshot,
        )

    @patch("airflow.providers.apache.ozone.utils.security.KerberosConfig.kinit_with_password")
    def test_kinit_from_snapshot_uses_snapshot_password_and_krb5_conf(
        self,
        mock_kinit_with_password: MagicMock,
    ):
        mock_kinit_with_password.return_value = True
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            kerberos_principal="user@REALM",
            kerberos_password="secret_password",
            krb5_conf="/tmp/krb5.conf",
        )
        assert KerberosConfig.kinit_from_snapshot(snapshot=snapshot)
        mock_kinit_with_password.assert_called_once_with(
            "user@REALM",
            "secret_password",
            "/tmp/krb5.conf",
            snapshot=snapshot,
        )

    def test_check_config_files_exist_uses_snapshot_names(self, tmp_path: Path):
        (tmp_path / "custom-core.xml").write_text("", encoding="utf-8")
        (tmp_path / "custom-ozone.xml").write_text("", encoding="utf-8")
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            core_site_xml="custom-core.xml",
            ozone_site_xml="custom-ozone.xml",
        )
        assert KerberosConfig.check_config_files_exist(str(tmp_path), snapshot=snapshot)

    @patch("airflow.providers.apache.ozone.utils.security.KerberosCliRunner.run_kerberos")
    def test_has_valid_ticket_uses_klist(self, mock_run_kerberos: MagicMock):
        mock_run_kerberos.return_value = True
        snapshot = OzoneConnSnapshot(host="om", port=9862, kinit_timeout_seconds=44)
        assert KerberosConfig.has_valid_ticket(snapshot=snapshot)
        mock_run_kerberos.assert_called_once_with(
            ["klist", "-s"],
            timeout=44,
            log_output=False,
        )

    @patch("airflow.providers.apache.ozone.utils.security.KerberosConfig.has_valid_ticket")
    @patch("airflow.providers.apache.ozone.utils.security.KerberosConfig.kinit_from_snapshot")
    def test_ensure_ticket_rechecks_lifetime_when_cached(
        self,
        mock_kinit_from_snapshot: MagicMock,
        mock_has_valid_ticket: MagicMock,
    ):
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            hadoop_security_authentication="kerberos",
            kerberos_principal="user@REALM",
            kerberos_keytab="/tmp/user.keytab",
        )
        mock_has_valid_ticket.return_value = False
        mock_kinit_from_snapshot.return_value = True

        assert KerberosConfig.ensure_ticket(
            snapshot=snapshot,
            conn_id="ozone_default",
            kerberos_ticket_ready=True,
        )
        mock_has_valid_ticket.assert_called_once_with(snapshot=snapshot)
        mock_kinit_from_snapshot.assert_called_once_with(snapshot=snapshot, conn_id="ozone_default")

    def test_ensure_ticket_fails_fast_when_kerberos_credentials_missing(self):
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            hadoop_security_authentication="kerberos",
            kerberos_principal="user@REALM",
        )
        with pytest.raises(AirflowException, match="neither 'kerberos_keytab' nor 'kerberos_password'"):
            KerberosConfig.ensure_ticket(
                snapshot=snapshot,
                conn_id="ozone_default",
                kerberos_ticket_ready=False,
            )
