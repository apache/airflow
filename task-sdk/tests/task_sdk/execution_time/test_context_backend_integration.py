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

from unittest.mock import patch

import pytest

from airflow.sdk.execution_time.context import _get_connection
from airflow.sdk.execution_time.secrets.execution_api import ExecutionAPISecretsBackend


class TestBackendIntegration:
    """Test that connection resolution uses the backend chain correctly."""

    def test_execution_api_backend_in_worker_chain(self):
        """Test that ExecutionAPISecretsBackend is in the worker search path."""
        from airflow.secrets import DEFAULT_SECRETS_SEARCH_PATH_WORKERS

        assert (
            "airflow.sdk.execution_time.secrets.execution_api.ExecutionAPISecretsBackend"
            in DEFAULT_SECRETS_SEARCH_PATH_WORKERS
        )

    def test_metastore_backend_in_server_chain(self):
        """Test that MetastoreBackend is in the API server search path."""
        from airflow.secrets import DEFAULT_SECRETS_SEARCH_PATH

        assert "airflow.secrets.metastore.MetastoreBackend" in DEFAULT_SECRETS_SEARCH_PATH
        assert (
            "airflow.sdk.execution_time.secrets.execution_api.ExecutionAPISecretsBackend"
            not in DEFAULT_SECRETS_SEARCH_PATH
        )

    def test_get_connection_uses_backend_chain(self, mock_supervisor_comms):
        """Test that _get_connection properly iterates through backends."""
        from airflow.sdk.api.datamodels._generated import ConnectionResponse
        from airflow.sdk.execution_time.comms import ConnectionResult

        # Mock connection response
        conn_response = ConnectionResponse(
            conn_id="test_conn",
            conn_type="http",
            host="example.com",
            port=443,
        )
        conn_result = ConnectionResult.from_conn_response(conn_response)
        mock_supervisor_comms.send.return_value = conn_result

        # Mock the backend loading to include our SupervisorComms backend
        supervisor_backend = ExecutionAPISecretsBackend()

        with patch("airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded") as mock_load:
            mock_load.return_value = [supervisor_backend]

            conn = _get_connection("test_conn")

            assert conn is not None
            assert conn.conn_id == "test_conn"
            assert conn.host == "example.com"
            mock_supervisor_comms.send.assert_called_once()

    def test_get_connection_backend_fallback(self, mock_supervisor_comms):
        """Test that _get_connection falls through backends correctly."""
        from airflow.sdk.api.datamodels._generated import ConnectionResponse
        from airflow.sdk.execution_time.comms import ConnectionResult

        # First backend returns nothing (simulating env var backend with no env var)
        class EmptyBackend:
            def get_connection(self, conn_id):
                return None

        # Second backend returns the connection
        conn_response = ConnectionResponse(
            conn_id="test_conn",
            conn_type="postgres",
            host="db.example.com",
        )
        conn_result = ConnectionResult.from_conn_response(conn_response)
        mock_supervisor_comms.send.return_value = conn_result

        supervisor_backend = ExecutionAPISecretsBackend()

        with patch("airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded") as mock_load:
            mock_load.return_value = [EmptyBackend(), supervisor_backend]

            conn = _get_connection("test_conn")

            assert conn is not None
            assert conn.conn_id == "test_conn"
            # SupervisorComms backend was called (first backend returned None)
            mock_supervisor_comms.send.assert_called_once()

    def test_get_connection_not_found_raises_error(self, mock_supervisor_comms):
        """Test that _get_connection raises error when no backend finds connection."""
        from airflow.exceptions import AirflowNotFoundException

        # Backend returns None (not found)
        class EmptyBackend:
            def get_connection(self, conn_id):
                return None

        with patch("airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded") as mock_load:
            mock_load.return_value = [EmptyBackend()]

            with pytest.raises(AirflowNotFoundException, match="isn't defined"):
                _get_connection("nonexistent_conn")
