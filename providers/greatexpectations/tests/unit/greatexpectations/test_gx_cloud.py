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

"""
Unit tests for GX Cloud Hook.
"""

from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from airflow.providers.greatexpectations.hooks.gx_cloud import (
    GXCloudConfig,
    GXCloudHook,
    IncompleteGXCloudConfigError,
)


class TestGXCloudHookGetConn:
    """Test class for GXCloudHook.get_conn method."""

    @patch("great_expectations_provider.hooks.gx_cloud.BaseHook.get_connection")
    def test_get_conn_success(self, mock_get_connection: Mock) -> None:
        """Test successful connection retrieval with all required parameters."""
        mock_conn = Mock()
        mock_conn.password = "test_token"
        mock_conn.login = "test_org_id"
        mock_conn.schema = "test_workspace_id"

        mock_get_connection.return_value = mock_conn

        hook = GXCloudHook("test_conn")
        result = hook.get_conn()

        assert isinstance(result, GXCloudConfig)
        assert result.cloud_access_token == "test_token"
        assert result.cloud_organization_id == "test_org_id"
        assert result.cloud_workspace_id == "test_workspace_id"
        mock_get_connection.assert_called_once_with("test_conn")

    @patch("great_expectations_provider.hooks.gx_cloud.BaseHook.get_connection")
    @pytest.mark.parametrize(
        "password,login,schema,expected_missing_keys",
        [
            (
                None,
                "test_org_id",
                '{"cloud_workspace_id": "test_workspace"}',
                ["GX Cloud Access Token"],
            ),
            (
                "test_token",
                None,
                '{"cloud_workspace_id": "test_workspace"}',
                ["GX Cloud Organization ID"],
            ),
            (
                None,
                None,
                '{"cloud_workspace_id": "test_workspace"}',
                ["GX Cloud Access Token", "GX Cloud Organization ID"],
            ),
            (
                "",
                "test_org_id",
                '{"cloud_workspace_id": "test_workspace"}',
                ["GX Cloud Access Token"],
            ),
            (
                "test_token",
                "",
                '{"cloud_workspace_id": "test_workspace"}',
                ["GX Cloud Organization ID"],
            ),
            ("test_token", "test_org_id", None, ["GX Cloud Workspace ID"]),
            ("test_token", "test_org_id", "", ["GX Cloud Workspace ID"]),
        ],
    )
    def test_get_conn_error(
        self,
        mock_get_connection: Mock,
        password: str | None,
        login: str | None,
        schema: str | None,
        expected_missing_keys: list[str],
    ) -> None:
        """Test that IncompleteGXCloudConfigError is raised when params are not provided."""
        mock_conn = Mock()
        mock_conn.password = password
        mock_conn.login = login
        mock_conn.schema = schema

        mock_get_connection.return_value = mock_conn

        hook = GXCloudHook("test_conn")

        with pytest.raises(IncompleteGXCloudConfigError) as exc_info:
            hook.get_conn()

        assert exc_info.value.missing_keys == expected_missing_keys
        for key in expected_missing_keys:
            assert key in str(exc_info.value)
