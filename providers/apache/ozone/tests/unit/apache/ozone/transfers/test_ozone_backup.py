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

from airflow.providers.apache.ozone.transfers.ozone_backup import OzoneBackupOperator
from airflow.providers.apache.ozone.utils.errors import OzoneCliError


class TestOzoneBackupOperator:
    def test_template_fields_cover_runtime_params(self):
        assert OzoneBackupOperator.template_fields == ("volume", "bucket", "snapshot_name", "ozone_conn_id")

    @patch("airflow.providers.apache.ozone.transfers.ozone_backup.OzoneAdminHook")
    def test_execute_create_snapshot(self, mock_admin_hook: MagicMock):
        mock_hook_instance = mock_admin_hook.return_value
        operator = OzoneBackupOperator(
            task_id="test_backup", volume="test_vol", bucket="test_bucket", snapshot_name="snapshot_20240101"
        )
        operator.execute(context={})
        mock_admin_hook.assert_called_once()
        assert mock_admin_hook.call_args.kwargs["ozone_conn_id"] == "ozone_admin_default"
        mock_hook_instance.run_cli.assert_called_once()
        snapshot_cmd = mock_hook_instance.run_cli.call_args.args[0]
        assert snapshot_cmd[:4] == ["ozone", "sh", "snapshot", "create"]
        assert "/test_vol/test_bucket" in snapshot_cmd
        assert "snapshot_20240101" in snapshot_cmd

    @patch("airflow.providers.apache.ozone.transfers.ozone_backup.OzoneAdminHook")
    def test_execute_idempotent_snapshot_exists(self, mock_admin_hook: MagicMock):
        mock_hook_instance = mock_admin_hook.return_value
        mock_hook_instance.run_cli.side_effect = OzoneCliError(
            "Command failed: snapshot already exists",
            stderr="FILE_ALREADY_EXISTS Snapshot already exists",
            returncode=255,
        )
        operator = OzoneBackupOperator(
            task_id="test_backup", volume="test_vol", bucket="test_bucket", snapshot_name="snapshot_20240101"
        )
        operator.execute(context={})
        mock_hook_instance.run_cli.assert_called_once()

    @patch("airflow.providers.apache.ozone.transfers.ozone_backup.OzoneAdminHook")
    def test_execute_idempotent_snapshot_exists_human_message(self, mock_admin_hook: MagicMock):
        mock_hook_instance = mock_admin_hook.return_value
        mock_hook_instance.run_cli.side_effect = OzoneCliError(
            "Command failed: snapshot already exists",
            stderr="snapshot already exists",
            returncode=255,
        )
        operator = OzoneBackupOperator(
            task_id="test_backup",
            volume="test_vol",
            bucket="test_bucket",
            snapshot_name="snapshot_20240101",
        )
        operator.execute(context={})
        mock_hook_instance.run_cli.assert_called_once()
