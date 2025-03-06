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

from airflow.exceptions import AirflowException
from airflow.sdk import Connection


class TestConnections:
    @pytest.fixture
    def mock_providers_manager(self):
        """Mock the ProvidersManager to return predefined hooks."""
        with mock.patch("airflow.providers_manager.ProvidersManager") as mock_manager:
            yield mock_manager

    @mock.patch("airflow.utils.module_loading.import_string")
    def test_get_hook(self, mock_import_string, mock_providers_manager):
        """Test that get_hook returns the correct hook instance."""

        mock_hook_class = mock.MagicMock()
        mock_hook_class.return_value = "mock_hook_instance"
        mock_import_string.return_value = mock_hook_class

        mock_hook = mock.MagicMock()
        mock_hook.hook_class_name = "airflow.providers.mysql.hooks.mysql.MySqlHook"
        mock_hook.connection_id_attribute_name = "conn_id"

        mock_providers_manager.return_value.hooks = {"mysql": mock_hook}

        conn = Connection(
            conn_id="test_conn",
            conn_type="mysql",
        )

        hook_instance = conn.get_hook(hook_params={"param1": "value1"})

        mock_import_string.assert_called_once_with("airflow.providers.mysql.hooks.mysql.MySqlHook")

        mock_hook_class.assert_called_once_with(conn_id="test_conn", param1="value1")

        assert hook_instance == "mock_hook_instance"

    def test_get_hook_invalid_type(self, mock_providers_manager):
        """Test that get_hook raises AirflowException for unknown hook type."""
        mock_providers_manager.return_value.hooks = {}

        conn = Connection(
            conn_id="test_conn",
            conn_type="unknown_type",
        )

        with pytest.raises(AirflowException, match='Unknown hook type "unknown_type"'):
            conn.get_hook()
