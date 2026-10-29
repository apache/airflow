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

from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.apache.ozone.transfers.hdfs_to_ozone import HdfsToOzoneOperator


class TestHdfsToOzoneOperator:
    def test_template_fields_cover_runtime_params(self):
        assert HdfsToOzoneOperator.template_fields == ("source_path", "dest_path", "hdfs_conn_id")

    @patch(
        "airflow.providers.apache.ozone.transfers.hdfs_to_ozone.shutil.which",
        return_value="/usr/bin/hadoop",
    )
    @patch("airflow.providers.apache.ozone.transfers.hdfs_to_ozone.CliRunner.run_process")
    def test_execute(self, mock_run_with_retry: MagicMock, _mock_which: MagicMock):
        operator = HdfsToOzoneOperator(
            task_id="hdfs_to_ozone_test",
            source_path="hdfs://nn:8020/user/data",
            dest_path="ofs://om:9862/vol1/bucket1/data",
        )
        operator.execute(context={})
        expected_cmd = [
            "hadoop",
            "distcp",
            "-update",
            "-skipcrccheck",
            "hdfs://nn:8020/user/data",
            "ofs://om:9862/vol1/bucket1/data",
        ]
        mock_run_with_retry.assert_called_once()
        call_args = mock_run_with_retry.call_args.args[0]
        assert call_args == expected_cmd

    @patch("airflow.providers.apache.ozone.transfers.hdfs_to_ozone.shutil.which", return_value=None)
    @patch("airflow.providers.apache.ozone.transfers.hdfs_to_ozone.CliRunner.run_process")
    def test_execute_missing_hadoop_fails_fast(self, mock_run_process: MagicMock, _mock_which: MagicMock):
        operator = HdfsToOzoneOperator(
            task_id="hdfs_to_ozone_missing_hadoop",
            source_path="hdfs://nn:8020/user/data",
            dest_path="ofs://om:9862/vol1/bucket1/data",
        )
        with pytest.raises(AirflowException, match="executable 'hadoop' was not found in PATH"):
            operator.execute(context={})
        mock_run_process.assert_not_called()

    @patch(
        "airflow.providers.apache.ozone.transfers.hdfs_to_ozone.shutil.which",
        return_value="/usr/bin/hadoop",
    )
    @patch("airflow.providers.apache.ozone.transfers.hdfs_to_ozone.CliRunner.run_process")
    def test_execute_empty_source_fails_before_runner(
        self, mock_run_process: MagicMock, _mock_which: MagicMock
    ):
        operator = HdfsToOzoneOperator(
            task_id="hdfs_to_ozone_empty_source",
            source_path="   ",
            dest_path="ofs://om:9862/vol1/bucket1/data",
        )
        with pytest.raises(AirflowException, match="requires non-empty source_path"):
            operator.execute(context={})
        mock_run_process.assert_not_called()

    @patch(
        "airflow.providers.apache.ozone.transfers.hdfs_to_ozone.shutil.which",
        return_value="/usr/bin/hadoop",
    )
    @patch("airflow.providers.apache.ozone.transfers.hdfs_to_ozone.CliRunner.run_process")
    def test_execute_empty_dest_fails_before_runner(
        self, mock_run_process: MagicMock, _mock_which: MagicMock
    ):
        operator = HdfsToOzoneOperator(
            task_id="hdfs_to_ozone_empty_dest",
            source_path="hdfs://nn:8020/user/data",
            dest_path="",
        )
        with pytest.raises(AirflowException, match="requires non-empty dest_path"):
            operator.execute(context={})
        mock_run_process.assert_not_called()

    def test_init_invalid_optional_conn_type(self):
        with pytest.raises(ValueError, match="hdfs_conn_id must be a string or None"):
            HdfsToOzoneOperator(
                task_id="hdfs_to_ozone_test_invalid",
                source_path="hdfs://nn:8020/user/data",
                dest_path="ofs://om:9862/vol1/bucket1/data",
                hdfs_conn_id=123,  # type: ignore[arg-type]
            )

    @patch(
        "airflow.providers.apache.ozone.transfers.hdfs_to_ozone.shutil.which",
        return_value="/usr/bin/hadoop",
    )
    @patch("airflow.providers.apache.ozone.transfers.hdfs_to_ozone.BaseHook.get_connection")
    @patch("airflow.providers.apache.ozone.transfers.hdfs_to_ozone.CliRunner.run_process")
    def test_execute_wires_distcp_mapreduce_options(
        self,
        mock_run_process: MagicMock,
        mock_get_connection: MagicMock,
        _mock_which: MagicMock,
    ):
        conn = MagicMock()
        conn.extra_dejson = {
            "hdfs_distcp_mapreduce_local": "true",
            "hdfs_distcp_renewer_principal": "slyubarsky@AD.RANGER-TEST",
        }
        mock_get_connection.return_value = conn

        operator = HdfsToOzoneOperator(
            task_id="hdfs_to_ozone_distcp_options",
            source_path="hdfs://nn:8020/user/data",
            dest_path="ofs://om:9862/vol1/bucket1/data",
            hdfs_conn_id="hdfs_admin_default",
        )
        operator.execute(context={})

        assert mock_run_process.call_args.args[0] == [
            "hadoop",
            "distcp",
            "-Dmapreduce.framework.name=local",
            "-Dyarn.resourcemanager.principal=slyubarsky@AD.RANGER-TEST",
            "-Dmapreduce.jobtracker.kerberos.principal=slyubarsky@AD.RANGER-TEST",
            "-update",
            "-skipcrccheck",
            "hdfs://nn:8020/user/data",
            "ofs://om:9862/vol1/bucket1/data",
        ]

    @patch(
        "airflow.providers.apache.ozone.transfers.hdfs_to_ozone.shutil.which",
        return_value="/usr/bin/hadoop",
    )
    @patch("airflow.providers.apache.ozone.transfers.hdfs_to_ozone.KerberosConfig.kinit_hdfs_from_snapshot")
    @patch("airflow.providers.apache.ozone.transfers.hdfs_to_ozone.BaseHook.get_connection")
    @patch("airflow.providers.apache.ozone.transfers.hdfs_to_ozone.CliRunner.run_process")
    def test_execute_wires_hdfs_kerberos_env(
        self,
        mock_run_process: MagicMock,
        mock_get_connection: MagicMock,
        mock_kinit: MagicMock,
        _mock_which: MagicMock,
    ):
        mock_kinit.return_value = True
        conn = MagicMock()
        conn.extra_dejson = {
            "hdfs_ssl_enabled": "true",
            "hdfs_ssl_keystore_location": "/etc/security/server.jks",
            "hdfs_kerberos_enabled": "true",
            "hdfs_kerberos_principal": "hdfs@EXAMPLE.COM",
            "hdfs_kerberos_keytab": "/etc/security/keytabs/hdfs.keytab",
            "krb5_conf": "/etc/krb5.conf",
        }
        mock_get_connection.return_value = conn

        operator = HdfsToOzoneOperator(
            task_id="hdfs_to_ozone_kerberos",
            source_path="hdfs://nn:8020/user/data",
            dest_path="ofs://om:9862/vol1/bucket1/data",
            hdfs_conn_id="hdfs_admin_default",
        )
        operator.execute(context={})

        mock_kinit.assert_called_once_with(
            snapshot=operator._hdfs_connection_snapshot,
            conn_id="hdfs_admin_default",
        )
        env_overrides = mock_run_process.call_args.kwargs["env_overrides"]
        assert env_overrides["HADOOP_SECURITY_AUTHENTICATION"] == "kerberos"
        assert env_overrides["HDFS_KERBEROS_PRINCIPAL"] == "hdfs@EXAMPLE.COM"
        assert env_overrides["HDFS_KERBEROS_KEYTAB"] == "/etc/security/keytabs/hdfs.keytab"
        assert env_overrides["KRB5_CONFIG"] == "/etc/krb5.conf"
        assert env_overrides["HDFS_SSL_ENABLED"] == "true"

    @patch(
        "airflow.providers.apache.ozone.transfers.hdfs_to_ozone.shutil.which",
        return_value="/usr/bin/hadoop",
    )
    @patch("airflow.providers.apache.ozone.transfers.hdfs_to_ozone.KerberosConfig.kinit_hdfs_from_snapshot")
    @patch("airflow.providers.apache.ozone.transfers.hdfs_to_ozone.BaseHook.get_connection")
    @patch("airflow.providers.apache.ozone.transfers.hdfs_to_ozone.CliRunner.run_process")
    def test_execute_wires_hdfs_kerberos_password_without_exporting_secret(
        self,
        mock_run_process: MagicMock,
        mock_get_connection: MagicMock,
        mock_kinit: MagicMock,
        _mock_which: MagicMock,
    ):
        mock_kinit.return_value = True
        conn = MagicMock()
        conn.extra_dejson = {
            "hdfs_kerberos_enabled": "true",
            "hdfs_kerberos_principal": "hdfs@EXAMPLE.COM",
            "hdfs_kerberos_password": "secret_password",
            "krb5_conf": "/etc/krb5.conf",
        }
        mock_get_connection.return_value = conn

        operator = HdfsToOzoneOperator(
            task_id="hdfs_to_ozone_kerberos_password",
            source_path="hdfs://nn:8020/user/data",
            dest_path="ofs://om:9862/vol1/bucket1/data",
            hdfs_conn_id="hdfs_admin_default",
        )
        operator.execute(context={})

        mock_kinit.assert_called_once_with(
            snapshot=operator._hdfs_connection_snapshot,
            conn_id="hdfs_admin_default",
        )
        env_overrides = mock_run_process.call_args.kwargs["env_overrides"]
        assert env_overrides["HADOOP_SECURITY_AUTHENTICATION"] == "kerberos"
        assert env_overrides["HDFS_KERBEROS_PRINCIPAL"] == "hdfs@EXAMPLE.COM"
        assert env_overrides["KRB5_CONFIG"] == "/etc/krb5.conf"
        assert "HDFS_KERBEROS_PASSWORD" not in env_overrides
        assert "HDFS_KERBEROS_KEYTAB" not in env_overrides

    @patch(
        "airflow.providers.apache.ozone.transfers.hdfs_to_ozone.shutil.which",
        return_value="/usr/bin/hadoop",
    )
    @patch("airflow.providers.apache.ozone.transfers.hdfs_to_ozone.KerberosConfig.kinit_hdfs_from_snapshot")
    @patch("airflow.providers.apache.ozone.transfers.hdfs_to_ozone.BaseHook.get_connection")
    @patch("airflow.providers.apache.ozone.transfers.hdfs_to_ozone.CliRunner.run_process")
    def test_execute_fails_when_hdfs_kerberos_kinit_fails(
        self,
        mock_run_process: MagicMock,
        mock_get_connection: MagicMock,
        mock_kinit: MagicMock,
        _mock_which: MagicMock,
    ):
        mock_kinit.return_value = False
        conn = MagicMock()
        conn.extra_dejson = {
            "hdfs_kerberos_enabled": "true",
            "hdfs_kerberos_principal": "hdfs@EXAMPLE.COM",
            "hdfs_kerberos_keytab": "/etc/security/keytabs/hdfs.keytab",
        }
        mock_get_connection.return_value = conn

        operator = HdfsToOzoneOperator(
            task_id="hdfs_to_ozone_kerberos_fail",
            source_path="hdfs://nn:8020/user/data",
            dest_path="ofs://om:9862/vol1/bucket1/data",
            hdfs_conn_id="hdfs_admin_default",
        )
        with pytest.raises(AirflowException, match="HDFS Kerberos authentication failed"):
            operator.execute(context={})
        mock_run_process.assert_not_called()
