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

from unittest import mock

import pytest

from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time.comms import ConnectionResult, ErrorResponse, GetConnection

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def mock_supervisor_comms():
    with mock.patch(
        "airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS", create=True
    ) as supervisor_comms:
        yield supervisor_comms


class TestBaseHook:
    def test_hook_has_default_logger_name(self):
        hook = BaseHook()
        assert hook.log.name == "airflow.task.hooks.airflow.hooks.base.BaseHook"

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

        mock_supervisor_comms.get_message.return_value = conn

        hook = BaseHook(logger_name="")
        hook.get_connection(conn_id="test_conn")
        mock_supervisor_comms.send_request.assert_called_once_with(
            msg=GetConnection(conn_id="test_conn"), log=mock.ANY
        )

    def test_get_connection_not_found(self, mock_supervisor_comms):
        conn_id = "test_conn"
        hook = BaseHook()
        mock_supervisor_comms.get_message.return_value = ErrorResponse(error=ErrorType.CONNECTION_NOT_FOUND)

        with pytest.raises(AirflowNotFoundException, match=rf".*{conn_id}.*"):
            hook.get_connection(conn_id=conn_id)

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

            mock_supervisor_comms.send_request.assert_not_called()
            mock_supervisor_comms.get_message.assert_not_called()
