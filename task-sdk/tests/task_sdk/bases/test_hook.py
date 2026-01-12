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

import pytest

from airflow.sdk import BaseHook
from airflow.sdk.exceptions import AirflowNotFoundException
from airflow.sdk.execution_time.comms import ConnectionResult, GetConnection

from tests_common.test_utils.config import conf_vars


class TestBaseHook:
    def test_hook_has_default_logger_name(self):
        hook = BaseHook()
        assert hook.log.name == "airflow.task.hooks.airflow.sdk.bases.hook.BaseHook"

    def test_custom_logger_name_is_correctly_set(self):
        hook = BaseHook(logger_name="airflow.custom.logger")
        assert hook.log.name == "airflow.task.hooks.airflow.custom.logger"

    def test_empty_string_as_logger_name(self):
        hook = BaseHook(logger_name="")
        assert hook.log.name == "airflow.task.hooks"

    def test_get_connection(self, mock_supervisor_comms):
        conn = ConnectionResult(
            conn_id="test_conn",
            conn_type="mysql",
            host="mysql",
            schema="airflow",
            login="root",
            password="password",
            port=1234,
            extra='{"extra_key": "extra_value"}',
        )

        mock_supervisor_comms.send.return_value = conn

        hook = BaseHook(logger_name="")
        hook.get_connection(conn_id="test_conn")
        mock_supervisor_comms.send.assert_any_call(
            msg=GetConnection(conn_id="test_conn"),
        )

    @pytest.mark.asyncio
    async def test_aget_connection(self, mock_supervisor_comms):
        """Test async connection retrieval in task sdk context."""
        conn = ConnectionResult(
            conn_id="test_conn",
            conn_type="mysql",
            host="mysql",
            schema="airflow",
            login="login",
            password="password",
            port=1234,
            extra='{"extra_key": "extra_value"}',
        )

        mock_supervisor_comms.asend.return_value = conn

        hook = BaseHook(logger_name="")
        await hook.aget_connection(conn_id="test_conn")
        mock_supervisor_comms.asend.assert_called_once_with(
            msg=GetConnection(conn_id="test_conn"),
        )

    def test_get_connection_not_found(self, sdk_connection_not_found):
        conn_id = "test_conn"
        hook = BaseHook()

        with pytest.raises(AirflowNotFoundException, match="The conn_id `test_conn` isn't defined"):
            hook.get_connection(conn_id=conn_id)

    @pytest.mark.asyncio
    async def test_aget_connection_not_found(self, sdk_connection_not_found):
        """Test async connection not found error."""
        conn_id = "test_conn"
        hook = BaseHook()

        with pytest.raises(AirflowNotFoundException, match="The conn_id `test_conn` isn't defined"):
            await hook.aget_connection(conn_id=conn_id)

    def test_get_connection_secrets_backend_configured(self, mock_supervisor_comms, tmp_path):
        path = tmp_path / "conn.env"
        path.write_text("CONN_A=mysql://host_a")

        with conf_vars(
            {
                ("secrets", "backend"): "airflow.secrets.local_filesystem.LocalFilesystemBackend",
                ("secrets", "backend_kwargs"): f'{{"connections_file_path": "{path}"}}',
            }
        ):
            hook = BaseHook(logger_name="")
            retrieved_conn = hook.get_connection(conn_id="CONN_A")

            assert retrieved_conn.conn_id == "CONN_A"

            mock_supervisor_comms.send.assert_not_called()
