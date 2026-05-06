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
"""Tests for AkeylessHook."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

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


HOOK_MODULE = "airflow.providers.akeyless.hooks.akeyless"


class TestAkeylessHook:
    @patch(f"{HOOK_MODULE}.AkeylessHook.get_connection", return_value=_make_connection())
    @patch(f"{HOOK_MODULE}.akeyless")
    def test_test_connection_success(self, mock_sdk, mock_conn):
        mock_sdk.V2Api.return_value.auth.return_value = MagicMock(token="mock-token")
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        ok, msg = hook.test_connection()
        assert ok is True
        assert "successfully" in msg.lower()

    @patch(f"{HOOK_MODULE}.AkeylessHook.get_connection", return_value=_make_connection())
    @patch(f"{HOOK_MODULE}.akeyless")
    def test_test_connection_failure(self, mock_sdk, mock_conn):
        mock_sdk.V2Api.return_value.auth.side_effect = Exception("Auth failed")
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        ok, msg = hook.test_connection()
        assert ok is False
        assert "Auth failed" in msg

    @patch(f"{HOOK_MODULE}.AkeylessHook.get_connection", return_value=_make_connection())
    @patch(f"{HOOK_MODULE}.akeyless")
    def test_get_secret_value(self, mock_sdk, mock_conn):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {"/path/to/secret": "s3cr3t"}
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        val = hook.get_secret_value("/path/to/secret")
        assert val == "s3cr3t"

    @patch(f"{HOOK_MODULE}.AkeylessHook.get_connection", return_value=_make_connection())
    @patch(f"{HOOK_MODULE}.akeyless")
    def test_get_secret_value_not_found(self, mock_sdk, mock_conn):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {}
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        val = hook.get_secret_value("/nonexistent")
        assert val is None

    @patch(f"{HOOK_MODULE}.AkeylessHook.get_connection", return_value=_make_connection())
    @patch(f"{HOOK_MODULE}.akeyless")
    def test_create_secret(self, mock_sdk, mock_conn):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        hook.create_secret("/new/secret", "val", description="test desc")
        api.create_secret.assert_called_once()

    @patch(f"{HOOK_MODULE}.AkeylessHook.get_connection", return_value=_make_connection())
    @patch(f"{HOOK_MODULE}.akeyless")
    def test_list_items(self, mock_sdk, mock_conn):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        item_a = MagicMock()
        item_a.to_dict.return_value = {"item_name": "/a"}
        item_b = MagicMock()
        item_b.to_dict.return_value = {"item_name": "/b"}
        api.list_items.return_value = MagicMock(items=[item_a, item_b])
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        items = hook.list_items("/")
        assert len(items) == 2
        assert items[0]["item_name"] == "/a"

    @patch(
        f"{HOOK_MODULE}.AkeylessHook.get_connection",
        return_value=_make_connection(extra='{"access_type": "uid", "uid_token": "uid-tok-123"}'),
    )
    @patch(f"{HOOK_MODULE}.akeyless")
    def test_uid_auth_type(self, mock_sdk, mock_conn):
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        token = hook.authenticate()
        assert token == "uid-tok-123"
        mock_sdk.V2Api.return_value.auth.assert_not_called()

    @patch(f"{HOOK_MODULE}.AkeylessHook.get_connection", return_value=_make_connection())
    @patch(f"{HOOK_MODULE}.akeyless")
    def test_get_dynamic_secret(self, mock_sdk, mock_conn):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_dynamic_secret_value.return_value = {"username": "admin", "password": "pw"}
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        result = hook.get_dynamic_secret_value("/dynamic/db-producer")
        assert result["username"] == "admin"

    @patch(f"{HOOK_MODULE}.AkeylessHook.get_connection", return_value=_make_connection())
    @patch(f"{HOOK_MODULE}.akeyless")
    def test_get_rotated_secret(self, mock_sdk, mock_conn):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        res = MagicMock()
        res.to_dict.return_value = {"value": {"username": "rotated-user"}}
        api.get_rotated_secret_value.return_value = res
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        result = hook.get_rotated_secret_value("/rotated/db-creds")
        assert "value" in result

    @patch(f"{HOOK_MODULE}.AkeylessHook.get_connection", return_value=_make_connection())
    @patch(f"{HOOK_MODULE}.akeyless")
    def test_get_conn_returns_v2api(self, mock_sdk, mock_conn):
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        client = hook.get_conn()
        assert client == mock_sdk.V2Api.return_value

    @patch(f"{HOOK_MODULE}.AkeylessHook.get_connection", return_value=_make_connection())
    @patch(f"{HOOK_MODULE}.akeyless")
    def test_delete_item(self, mock_sdk, mock_conn):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        hook.delete_item("/path/to/delete")
        api.delete_item.assert_called_once()

    @patch(
        f"{HOOK_MODULE}.AkeylessHook.get_connection",
        return_value=_make_connection(extra='{"access_type": "invalid_type"}'),
    )
    @patch(f"{HOOK_MODULE}.akeyless")
    def test_invalid_access_type_raises(self, mock_sdk, mock_conn):
        import pytest

        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        with pytest.raises(ValueError, match="Unsupported access_type"):
            hook.authenticate()

    @patch(f"{HOOK_MODULE}.AkeylessHook.get_connection", return_value=_make_connection())
    @patch(f"{HOOK_MODULE}.akeyless")
    def test_get_rotated_secret_passes_list(self, mock_sdk, mock_conn):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        res = MagicMock()
        res.to_dict.return_value = {"value": {"user": "rotated"}}
        api.get_rotated_secret_value.return_value = res
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook()
        hook.get_rotated_secret_value("/rotated/creds")
        mock_sdk.GetRotatedSecretValue.assert_called_once_with(names=["/rotated/creds"], token="t")
