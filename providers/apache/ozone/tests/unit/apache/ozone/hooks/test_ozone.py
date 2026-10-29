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

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.apache.ozone.hooks.ozone import (
    ExistingTargetPolicy,
    OzoneAdminExtraHook,
    OzoneAdminHook,
    OzoneCliHook,
    OzoneFsHook,
)
from airflow.providers.apache.ozone.utils.errors import OzoneCliError

MOCK_CLI_PATH = "airflow.providers.apache.ozone.hooks.ozone.OzoneCliHook.run_cli"
MOCK_RUN_PATH = "airflow.providers.apache.ozone.hooks.ozone.OzoneCliRunner.run_ozone_once"
MOCK_RUN_RETRY_PATH = "airflow.providers.apache.ozone.hooks.ozone.OzoneCliRunner.run_ozone"


@pytest.fixture
def admin_hook():
    hook = OzoneAdminHook(ozone_conn_id="test_admin_conn")
    conn = MagicMock()
    conn.host = "ozone-om"
    conn.port = 9862
    conn.extra_dejson = {}
    with patch.object(hook, "get_connection", return_value=conn):
        yield hook


@pytest.fixture
def admin_extra_hook():
    hook = OzoneAdminExtraHook(ozone_conn_id="test_admin_conn")
    conn = MagicMock()
    conn.host = "ozone-om"
    conn.port = 9862
    conn.extra_dejson = {}
    with patch.object(hook, "get_connection", return_value=conn):
        yield hook


@pytest.fixture
def ozone_fs_hook():
    hook = OzoneFsHook(ozone_conn_id="test_conn")
    conn = MagicMock()
    conn.host = "ozone-om"
    conn.port = 9862
    conn.extra_dejson = {}
    with patch.object(hook, "get_connection", return_value=conn):
        yield hook


class TestOzoneCliHookConnectionSnapshot:
    def test_connection_snapshot_requires_host(self):
        conn = MagicMock()
        conn.host = None
        conn.port = 9862
        conn.extra_dejson = {}
        hook = OzoneCliHook(ozone_conn_id="ozone_default")
        with (
            patch.object(hook, "get_connection", return_value=conn),
            pytest.raises(AirflowException, match="must define host"),
        ):
            _ = hook.connection_snapshot

    def test_connection_snapshot_requires_port(self):
        conn = MagicMock()
        conn.host = "om-host"
        conn.port = None
        conn.extra_dejson = {}
        hook = OzoneCliHook(ozone_conn_id="ozone_default")
        with (
            patch.object(hook, "get_connection", return_value=conn),
            pytest.raises(AirflowException, match="must define port"),
        ):
            _ = hook.connection_snapshot

    def test_prepared_cli_env_uses_ozone_conf_dir_from_connection_extra(self):
        conn = MagicMock()
        conn.host = "om-host"
        conn.port = 9862
        conn.extra_dejson = {"ozone_conf_dir": "/opt/airflow/ozone-conf"}
        hook = OzoneCliHook(ozone_conn_id="ozone_default")
        with patch.object(hook, "get_connection", return_value=conn):
            env = hook._prepared_cli_env()
        assert env["OZONE_CONF_DIR"] == "/opt/airflow/ozone-conf"
        assert env["HADOOP_CONF_DIR"] == "/opt/airflow/ozone-conf"

    def test_prepared_cli_env_uses_ozone_conf_dir_from_environment(self, monkeypatch, caplog):
        conn = MagicMock()
        conn.host = "om-host"
        conn.port = 9862
        conn.extra_dejson = {}
        hook = OzoneCliHook(ozone_conn_id="ozone_default")
        monkeypatch.setenv("OZONE_CONF_DIR", "/env/ozone-conf")

        with patch.object(hook, "get_connection", return_value=conn), caplog.at_level("INFO"):
            env = hook._prepared_cli_env()

        assert env["OZONE_CONF_DIR"] == "/env/ozone-conf"
        assert env["HADOOP_CONF_DIR"] == "/env/ozone-conf"
        assert "Using ozone_conf_dir from OZONE_CONF_DIR environment variable" in caplog.text

    def test_prepared_cli_env_raises_when_kerberos_conf_dir_missing(self, monkeypatch):
        conn = MagicMock()
        conn.host = "om-host"
        conn.port = 9862
        conn.extra_dejson = {"hadoop_security_authentication": "kerberos"}
        hook = OzoneCliHook(ozone_conn_id="ozone_default")
        monkeypatch.delenv("OZONE_CONF_DIR", raising=False)
        monkeypatch.delenv("HADOOP_CONF_DIR", raising=False)

        with (
            patch.object(hook, "get_connection", return_value=conn),
            pytest.raises(AirflowException, match="Kerberos is enabled but ozone_conf_dir is not configured"),
        ):
            hook._prepared_cli_env()

    @pytest.mark.parametrize(
        ("extra", "expected_mode"),
        [
            ({}, "plain"),
            ({"ozone_security_enabled": "true"}, "ssl"),
            (
                {
                    "hadoop_security_authentication": "kerberos",
                    "kerberos_principal": "airflow@EXAMPLE.COM",
                    "kerberos_keytab": "/tmp/airflow.keytab",
                    "krb5_conf": "/etc/krb5.conf",
                    "ozone_conf_dir": "/opt/airflow/ozone-conf",
                },
                "kerberos",
            ),
            (
                {
                    "ozone_security_enabled": "true",
                    "hadoop_security_authentication": "kerberos",
                    "kerberos_principal": "airflow@EXAMPLE.COM",
                    "kerberos_keytab": "/tmp/airflow.keytab",
                    "krb5_conf": "/etc/krb5.conf",
                    "ozone_conf_dir": "/opt/airflow/ozone-conf",
                },
                "ssl+kerberos",
            ),
        ],
    )
    @patch("airflow.providers.apache.ozone.hooks.ozone.OzoneCliRunner.run_ozone")
    @patch("airflow.providers.apache.ozone.hooks.ozone.KerberosConfig.ensure_ticket")
    def test_run_cli_logs_runtime_mode(
        self,
        mock_ensure_ticket: MagicMock,
        mock_run_ozone: MagicMock,
        extra: dict[str, str],
        expected_mode: str,
        caplog,
    ) -> None:
        conn = MagicMock()
        conn.host = "om-host"
        conn.port = 9862
        conn.extra_dejson = extra
        hook = OzoneCliHook(ozone_conn_id="ozone_default")
        mock_ensure_ticket.return_value = False
        mock_run_ozone.return_value = subprocess.CompletedProcess(
            args=["ozone", "sh", "volume", "list", "/"],
            returncode=0,
            stdout="[]",
            stderr="",
        )

        with patch.object(hook, "get_connection", return_value=conn), caplog.at_level("INFO"):
            hook.run_cli(["ozone", "sh", "volume", "list", "/"], log_output=False)

        assert f"mode: {expected_mode}" in caplog.text


class TestOzoneAdminHook:
    @patch(MOCK_RUN_RETRY_PATH)
    def test_create_volume_uses_run_with_retry(
        self, mock_run_with_retry: MagicMock, admin_hook: OzoneAdminHook
    ):
        mock_run_with_retry.return_value = MagicMock(returncode=0, stdout="", stderr="")
        admin_hook.create_volume(volume_name="test_vol")
        mock_run_with_retry.assert_called_once()
        assert mock_run_with_retry.call_args.args[0] == ["ozone", "sh", "volume", "create", "/test_vol"]

    @patch(MOCK_RUN_RETRY_PATH)
    def test_create_volume_already_exists_is_idempotent(
        self, mock_run_with_retry: MagicMock, admin_hook: OzoneAdminHook
    ):
        mock_run_with_retry.return_value = subprocess.CompletedProcess(
            args=["ozone", "sh", "volume", "create", "/test_vol"],
            returncode=255,
            stdout="",
            stderr=(
                "log4j:WARN No appenders could be found for logger (org.apache.hadoop.util.Shell).\n"
                "VOLUME_ALREADY_EXISTS Volume already exists"
            ),
        )
        admin_hook.create_volume(volume_name="test_vol")
        mock_run_with_retry.assert_called_once()

    @patch(MOCK_RUN_RETRY_PATH)
    def test_create_volume_already_exists_can_fail_fast(
        self, mock_run_with_retry: MagicMock, admin_hook: OzoneAdminHook
    ):
        mock_run_with_retry.return_value = subprocess.CompletedProcess(
            args=["ozone", "sh", "volume", "create", "/test_vol"],
            returncode=255,
            stdout="",
            stderr="VOLUME_ALREADY_EXISTS Volume already exists",
        )
        with pytest.raises(AirflowException, match="Volume already exists"):
            admin_hook.create_volume(volume_name="test_vol", if_exists="error")
        mock_run_with_retry.assert_called_once()

    @patch(MOCK_RUN_RETRY_PATH)
    def test_create_bucket_raises_on_non_idempotent_failure(
        self, mock_run_with_retry: MagicMock, admin_hook: OzoneAdminHook
    ):
        mock_run_with_retry.return_value = subprocess.CompletedProcess(
            args=["ozone", "sh", "bucket", "create", "/test_vol/test_bkt", "--space-quota", "100GB"],
            returncode=1,
            stdout="",
            stderr="ACCESS_DENIED",
        )
        with pytest.raises(AirflowException, match="Ozone command failed"):
            admin_hook.create_bucket(volume_name="test_vol", bucket_name="test_bkt", quota="100GB")

    @patch(MOCK_RUN_RETRY_PATH)
    def test_delete_bucket_missing_is_idempotent(
        self, mock_run_with_retry: MagicMock, admin_hook: OzoneAdminHook
    ):
        mock_run_with_retry.return_value = subprocess.CompletedProcess(
            args=["ozone", "sh", "bucket", "delete", "/test_vol/missing_bkt"],
            returncode=255,
            stdout="",
            stderr=(
                "log4j:WARN No appenders could be found for logger (org.apache.hadoop.util.Shell).\n"
                "BUCKET_NOT_FOUND Bucket not exists"
            ),
        )
        admin_hook.delete_bucket(volume_name="test_vol", bucket_name="missing_bkt")
        mock_run_with_retry.assert_called_once()

    @patch(MOCK_RUN_RETRY_PATH)
    def test_delete_volume_missing_is_idempotent(
        self, mock_run_with_retry: MagicMock, admin_hook: OzoneAdminHook
    ):
        mock_run_with_retry.return_value = subprocess.CompletedProcess(
            args=["ozone", "sh", "volume", "delete", "/missing_vol"],
            returncode=255,
            stdout="",
            stderr=(
                "log4j:WARN No appenders could be found for logger (org.apache.hadoop.util.Shell).\n"
                "VOLUME_NOT_FOUND Volume missing_vol is not found"
            ),
        )
        admin_hook.delete_volume(volume_name="missing_vol")
        mock_run_with_retry.assert_called_once()

    @patch(MOCK_CLI_PATH)
    def test_get_container_report(self, mock_run_cli: MagicMock, admin_extra_hook: OzoneAdminExtraHook):
        mock_run_cli.return_value = {"total": 1, "containers": [{"id": 1}]}
        result = admin_extra_hook.get_container_report()
        mock_run_cli.assert_called_once_with(
            ["ozone", "admin", "container", "report", "--json"],
            timeout=3600,
            return_json_result=True,
        )
        assert isinstance(result, dict)
        containers = result["containers"]
        assert isinstance(containers, list)
        assert result["total"] == 1
        assert len(containers) == 1


class TestOzoneFsHook:
    @patch(MOCK_RUN_RETRY_PATH)
    def test_exists_false(self, mock_run_cli: MagicMock, ozone_fs_hook: OzoneFsHook):
        mock_run_cli.return_value = subprocess.CompletedProcess(
            args=["ozone", "fs", "-test", "-e", "ofs://path/does_not_exist"],
            returncode=1,
            stdout="",
            stderr="not found",
        )
        assert ozone_fs_hook.exists("ofs://path/does_not_exist") is False
        mock_run_cli.assert_called_once()
        command = mock_run_cli.call_args.args[0]
        assert command[:4] == ["ozone", "fs", "-test", "-e"]
        assert command[-1] == "ofs://path/does_not_exist"

    @patch(MOCK_RUN_RETRY_PATH)
    def test_exists_false_when_returncode_one_has_only_noise_stderr(
        self, mock_run_cli: MagicMock, ozone_fs_hook: OzoneFsHook
    ):
        mock_run_cli.return_value = subprocess.CompletedProcess(
            args=["ozone", "fs", "-test", "-e", "ofs://path/does_not_exist"],
            returncode=1,
            stdout="",
            stderr=(
                "log4j:WARN No appenders could be found for logger "
                "(org.apache.hadoop.metrics2.lib.MutableMetricsFactory).\n"
                "log4j:WARN Please initialize the log4j system properly.\n"
                "log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info."
            ),
        )
        assert ozone_fs_hook.exists("ofs://path/does_not_exist") is False

    @patch(MOCK_RUN_RETRY_PATH)
    def test_exists_raises_on_meaningful_non_not_found_error(
        self, mock_run_cli: MagicMock, ozone_fs_hook: OzoneFsHook
    ):
        mock_run_cli.return_value = subprocess.CompletedProcess(
            args=["ozone", "fs", "-test", "-e", "ofs://path/forbidden"],
            returncode=1,
            stdout="",
            stderr="Permission denied",
        )
        with pytest.raises(OzoneCliError, match="existence check failed"):
            ozone_fs_hook.exists("ofs://path/forbidden")

    @patch(MOCK_RUN_RETRY_PATH)
    def test_exists_raises_when_cli_missing(self, mock_run_cli: MagicMock, ozone_fs_hook: OzoneFsHook):
        mock_run_cli.side_effect = OzoneCliError("Ozone CLI not found.", retryable=False)
        with pytest.raises(AirflowException):
            ozone_fs_hook.exists("ofs://path/any")

    @patch(MOCK_CLI_PATH)
    def test_upload_key_raises_when_file_missing(
        self, mock_run_cli: MagicMock, ozone_fs_hook: OzoneFsHook, tmp_path
    ):
        missing_path = tmp_path / "missing.txt"
        with pytest.raises(AirflowException):
            ozone_fs_hook.upload_key(str(missing_path), "ofs://vol1/bucket1/file.txt")
        mock_run_cli.assert_not_called()

    @patch(MOCK_RUN_RETRY_PATH)
    def test_upload_key_uses_plain_put_for_new_target(
        self, mock_run_cli: MagicMock, ozone_fs_hook: OzoneFsHook, tmp_path
    ):
        local_path = tmp_path / "payload.txt"
        local_path.write_text("payload", encoding="utf-8")
        mock_run_cli.side_effect = [
            subprocess.CompletedProcess(
                args=["ozone", "fs", "-test", "-e", "ofs://vol1/bucket1/file.txt"],
                returncode=1,
                stdout="",
                stderr="not found",
            ),
            subprocess.CompletedProcess(
                args=["ozone", "sh", "key", "put", "o3://vol1/bucket1/file.txt", str(local_path)],
                returncode=0,
                stdout="",
                stderr="",
            ),
        ]

        ozone_fs_hook.upload_key(str(local_path), "ofs://vol1/bucket1/file.txt")

        assert mock_run_cli.call_count == 2
        assert mock_run_cli.call_args_list[1].args[0] == [
            "ozone",
            "sh",
            "key",
            "put",
            "o3://vol1/bucket1/file.txt",
            str(local_path),
        ]

    @patch(MOCK_RUN_RETRY_PATH)
    def test_make_path_existing_target_policy_error(
        self, mock_run_cli: MagicMock, ozone_fs_hook: OzoneFsHook
    ):
        mock_run_cli.return_value = subprocess.CompletedProcess(
            args=["ozone", "fs", "-test", "-e", "ofs://vol1/bucket1/dir"],
            returncode=0,
            stdout="",
            stderr="",
        )

        with pytest.raises(AirflowException, match="Destination path already exists"):
            ozone_fs_hook.make_path("ofs://vol1/bucket1/dir", if_exists=ExistingTargetPolicy.ERROR)

        mock_run_cli.assert_called_once()

    @patch(MOCK_RUN_RETRY_PATH)
    def test_make_path_existing_target_policy_ignore(
        self, mock_run_cli: MagicMock, ozone_fs_hook: OzoneFsHook
    ):
        mock_run_cli.return_value = subprocess.CompletedProcess(
            args=["ozone", "fs", "-test", "-e", "ofs://vol1/bucket1/dir"],
            returncode=0,
            stdout="",
            stderr="",
        )

        ozone_fs_hook.make_path("ofs://vol1/bucket1/dir", if_exists=ExistingTargetPolicy.IGNORE)

        mock_run_cli.assert_called_once()

    @pytest.mark.parametrize(
        ("fail_if_exists", "expected_if_exists"),
        [
            (True, ExistingTargetPolicy.ERROR),
            (False, ExistingTargetPolicy.IGNORE),
        ],
    )
    def test_create_path_deprecated_wrapper_warns(
        self,
        ozone_fs_hook: OzoneFsHook,
        caplog,
        fail_if_exists: bool,
        expected_if_exists: ExistingTargetPolicy,
    ):
        with patch.object(ozone_fs_hook, "make_path") as mock_make_path:
            with caplog.at_level("WARNING"):
                ozone_fs_hook.create_path(
                    "ofs://vol1/bucket1/dir",
                    timeout=123,
                    recursive=False,
                    fail_if_exists=fail_if_exists,
                )

        assert "OzoneFsHook.create_path(..., fail_if_exists=...) is deprecated" in caplog.text
        mock_make_path.assert_called_once_with(
            "ofs://vol1/bucket1/dir",
            timeout=123,
            recursive=False,
            if_exists=expected_if_exists,
        )

    @pytest.mark.parametrize(
        "if_exists",
        [ExistingTargetPolicy.ERROR, "ignore", ExistingTargetPolicy.OVERWRITE],
    )
    @patch(MOCK_RUN_RETRY_PATH)
    def test_upload_key_existing_target_policy(
        self,
        mock_run_cli: MagicMock,
        ozone_fs_hook: OzoneFsHook,
        tmp_path,
        if_exists: ExistingTargetPolicy | str,
    ):
        local_path = tmp_path / "payload.txt"
        local_path.write_text("payload", encoding="utf-8")
        exists_result = subprocess.CompletedProcess(
            args=["ozone", "fs", "-test", "-e", "ofs://vol1/bucket1/file.txt"],
            returncode=0,
            stdout="",
            stderr="",
        )
        put_result = subprocess.CompletedProcess(
            args=["ozone", "sh", "key", "put", "o3://vol1/bucket1/file.txt", str(local_path)],
            returncode=0,
            stdout="",
            stderr="",
        )
        mock_run_cli.side_effect = [exists_result, put_result]

        if if_exists == "error":
            with pytest.raises(AirflowException, match="Remote path already exists"):
                ozone_fs_hook.upload_key(
                    str(local_path),
                    "ofs://vol1/bucket1/file.txt",
                    if_exists=if_exists,
                )
            assert mock_run_cli.call_count == 1
            return

        with patch.object(ozone_fs_hook, "delete_key") as mock_delete_key:
            ozone_fs_hook.upload_key(
                str(local_path),
                "ofs://vol1/bucket1/file.txt",
                if_exists=if_exists,
            )
        expected_calls = 1 if if_exists == "ignore" else 3
        expected_delete_calls = 0 if if_exists == "ignore" else 1
        assert mock_run_cli.call_count == expected_calls - expected_delete_calls
        assert mock_delete_key.call_count == expected_delete_calls
        if if_exists == "overwrite":
            mock_delete_key.assert_called_once_with("ofs://vol1/bucket1/file.txt", timeout=3600)
            assert mock_run_cli.call_args_list[1].args[0] == [
                "ozone",
                "sh",
                "key",
                "put",
                "o3://vol1/bucket1/file.txt",
                str(local_path),
            ]

    @patch(MOCK_RUN_RETRY_PATH)
    def test_copy_path_existing_destination_fails_fast(
        self, mock_run_cli: MagicMock, ozone_fs_hook: OzoneFsHook
    ):
        mock_run_cli.side_effect = [
            subprocess.CompletedProcess(
                args=["ozone", "fs", "-test", "-e", "ofs://vol1/bucket1"],
                returncode=0,
                stdout="",
                stderr="",
            ),
            subprocess.CompletedProcess(
                args=["ozone", "fs", "-test", "-e", "ofs://vol1/bucket1/dst.txt"],
                returncode=0,
                stdout="",
                stderr="",
            ),
        ]

        with pytest.raises(AirflowException, match="Destination path already exists"):
            ozone_fs_hook.copy_path("ofs://vol1/bucket1/src.txt", "ofs://vol1/bucket1/dst.txt")

        assert mock_run_cli.call_count == 2

    @patch(MOCK_RUN_RETRY_PATH)
    def test_test_connection_failure(self, mock_run_cli_check: MagicMock, ozone_fs_hook: OzoneFsHook):
        mock_run_cli_check.return_value = MagicMock(returncode=1, stdout="", stderr="auth failed")
        ok, message = ozone_fs_hook.test_connection()
        assert ok is False
        assert "auth failed" in message

    @patch(MOCK_RUN_RETRY_PATH)
    def test_test_connection_timeout(self, mock_run_cli_check: MagicMock, ozone_fs_hook: OzoneFsHook):
        mock_run_cli_check.side_effect = OzoneCliError("Ozone command timed out", retryable=True)
        ok, message = ozone_fs_hook.test_connection()
        assert ok is False
        assert "connection test failed" in message

    def test_get_key_info_converts_ofs_uri_for_key_cli(self, ozone_fs_hook: OzoneFsHook):
        with patch.object(ozone_fs_hook, "run_cli", return_value={"name": "src.txt"}) as mock_run_cli:
            assert ozone_fs_hook.get_key_info("ofs://vol1/b1/src.txt") == {"name": "src.txt"}

        mock_run_cli.assert_called_once_with(
            ["ozone", "sh", "key", "info", "o3://vol1/b1/src.txt"],
            timeout=300,
            return_json_result=True,
        )

    def test_get_key_property_warns_when_replication_config_missing(self, ozone_fs_hook: OzoneFsHook, caplog):
        with patch.object(
            ozone_fs_hook,
            "get_key_info",
            return_value={"name": "file.txt", "replicationType": "RATIS", "replicationFactor": 3},
        ):
            with caplog.at_level("WARNING"):
                result = ozone_fs_hook.get_key_property("ofs://vol1/b1/file.txt")
        assert result["replication_type"] == "RATIS"
        assert result["replication"] == 3
        assert "does not contain 'replicationConfig'" in caplog.text
