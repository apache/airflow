# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for AkeylessHook."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from airflow.models.connection import Connection

MOCK_ACCESS_ID = "p-test123"
MOCK_ACCESS_KEY = "test-key-secret"
MOCK_API_URL = "https://api.akeyless.io"


def _make_connection(**overrides):
    defaults = dict(
        conn_id="akeyless_default",
        conn_type="akeyless",
        host=MOCK_API_URL,
        login=MOCK_ACCESS_ID,
        password=MOCK_ACCESS_KEY,
        extra='{"access_type": "api_key"}',
    )
    defaults.update(overrides)
    return Connection(**defaults)


class TestAkeylessHook:
    @patch(
        "airflow.providers.akeyless.hooks.akeyless.AkeylessHook.get_connection",
        return_value=_make_connection(),
    )
    @patch(
        "airflow.providers.akeyless._internal_client.akeyless_client._AkeylessClient.authenticate",
        return_value="mock-token",
    )
    def test_test_connection_success(self, mock_auth, mock_conn):
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        ok, msg = hook.test_connection()
        assert ok is True
        assert "successfully" in msg.lower()
        mock_auth.assert_called_once()

    @patch(
        "airflow.providers.akeyless.hooks.akeyless.AkeylessHook.get_connection",
        return_value=_make_connection(),
    )
    @patch(
        "airflow.providers.akeyless._internal_client.akeyless_client._AkeylessClient.authenticate",
        side_effect=Exception("Auth failed"),
    )
    def test_test_connection_failure(self, mock_auth, mock_conn):
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        ok, msg = hook.test_connection()
        assert ok is False
        assert "Auth failed" in msg

    @patch(
        "airflow.providers.akeyless.hooks.akeyless.AkeylessHook.get_connection",
        return_value=_make_connection(),
    )
    @patch(
        "airflow.providers.akeyless._internal_client.akeyless_client._AkeylessClient.get_secret_value",
        return_value="s3cr3t",
    )
    def test_get_secret_value(self, mock_get, mock_conn):
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        val = hook.get_secret_value("/path/to/secret")
        assert val == "s3cr3t"
        mock_get.assert_called_once_with("/path/to/secret")

    @patch(
        "airflow.providers.akeyless.hooks.akeyless.AkeylessHook.get_connection",
        return_value=_make_connection(),
    )
    @patch(
        "airflow.providers.akeyless._internal_client.akeyless_client._AkeylessClient.get_secret_value",
        return_value=None,
    )
    def test_get_secret_value_not_found(self, mock_get, mock_conn):
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        val = hook.get_secret_value("/nonexistent")
        assert val is None

    @patch(
        "airflow.providers.akeyless.hooks.akeyless.AkeylessHook.get_connection",
        return_value=_make_connection(),
    )
    @patch(
        "airflow.providers.akeyless._internal_client.akeyless_client._AkeylessClient.create_secret",
    )
    def test_create_secret(self, mock_create, mock_conn):
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        hook.create_secret("/new/secret", "val", description="test desc")
        mock_create.assert_called_once_with("/new/secret", "val", description="test desc")

    @patch(
        "airflow.providers.akeyless.hooks.akeyless.AkeylessHook.get_connection",
        return_value=_make_connection(),
    )
    @patch(
        "airflow.providers.akeyless._internal_client.akeyless_client._AkeylessClient.list_items",
        return_value=[{"item_name": "/a"}, {"item_name": "/b"}],
    )
    def test_list_items(self, mock_list, mock_conn):
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        items = hook.list_items("/")
        assert len(items) == 2
        mock_list.assert_called_once_with("/")

    @patch(
        "airflow.providers.akeyless.hooks.akeyless.AkeylessHook.get_connection",
        return_value=_make_connection(
            extra='{"access_type": "uid", "uid_token": "uid-tok-123"}'
        ),
    )
    @patch(
        "airflow.providers.akeyless._internal_client.akeyless_client._AkeylessClient.authenticate",
        return_value="uid-tok-123",
    )
    def test_uid_auth_type(self, mock_auth, mock_conn):
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        ok, _ = hook.test_connection()
        assert ok is True

    @patch(
        "airflow.providers.akeyless.hooks.akeyless.AkeylessHook.get_connection",
        return_value=_make_connection(),
    )
    @patch(
        "airflow.providers.akeyless._internal_client.akeyless_client._AkeylessClient.get_dynamic_secret_value",
        return_value={"username": "admin", "password": "pw"},
    )
    def test_get_dynamic_secret(self, mock_dyn, mock_conn):
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        result = hook.get_dynamic_secret_value("/dynamic/db-producer")
        assert result["username"] == "admin"

    @patch(
        "airflow.providers.akeyless.hooks.akeyless.AkeylessHook.get_connection",
        return_value=_make_connection(),
    )
    @patch(
        "airflow.providers.akeyless._internal_client.akeyless_client._AkeylessClient.get_rotated_secret_value",
        return_value={"value": {"username": "rotated-user", "password": "rotated-pw"}},
    )
    def test_get_rotated_secret(self, mock_rot, mock_conn):
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        result = hook.get_rotated_secret_value("/rotated/db-creds")
        assert "value" in result

    @patch(
        "airflow.providers.akeyless.hooks.akeyless.AkeylessHook.get_connection",
        return_value=_make_connection(),
    )
    def test_get_conn_returns_client(self, mock_conn):
        from airflow.providers.akeyless._internal_client.akeyless_client import _AkeylessClient
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        client = hook.get_conn()
        assert isinstance(client, _AkeylessClient)
