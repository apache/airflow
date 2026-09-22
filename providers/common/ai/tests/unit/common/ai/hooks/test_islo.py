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

import sys
from types import SimpleNamespace
from unittest import mock

import pytest

from airflow.providers.common.ai.hooks.islo import IsloHook


def _connection(password="secret-key", host=None, extra=None):
    return SimpleNamespace(password=password, host=host, extra_dejson=extra or {})


class TestIsloHook:
    def test_connection_type_metadata(self):
        assert IsloHook.conn_type == "islo"
        assert IsloHook.default_conn_name == "islo_default"
        assert IsloHook.conn_name_attr == "islo_conn_id"
        assert IsloHook.hook_name == "Islo"

    @pytest.mark.parametrize(("conn_id", "expected"), [(None, "islo_default"), ("my_islo", "my_islo")])
    def test_connection_id_defaults(self, conn_id, expected):
        assert IsloHook(islo_conn_id=conn_id).islo_conn_id == expected

    def test_ui_field_behaviour_names_the_fields_the_backend_reads(self):
        behaviour = IsloHook.get_ui_field_behaviour()

        assert behaviour["hidden_fields"] == ["schema", "port", "login"]
        assert behaviour["relabeling"] == {"password": "API Key", "host": "Compute URL"}
        assert "base_url" in behaviour["placeholders"]["extra"]
        assert "timeout" in behaviour["placeholders"]["extra"]

    @mock.patch.object(IsloHook, "get_connection")
    def test_allowlisted_connection_fields_become_client_kwargs(self, get_connection):
        get_connection.return_value = _connection(
            password=" key ",
            host="https://compute",
            extra={"base_url": "https://api", "timeout": 12, "ignored": "value"},
        )

        kwargs = IsloHook(islo_conn_id="my_islo").build_client_kwargs()

        get_connection.assert_called_once_with("my_islo")
        assert kwargs == {
            "api_key": "key",
            "compute_url": "https://compute",
            "base_url": "https://api",
            "timeout": 12.0,
        }

    @mock.patch.object(IsloHook, "get_connection")
    def test_optional_fields_are_omitted_rather_than_sent_empty(self, get_connection):
        get_connection.return_value = _connection(host="", extra={"base_url": ""})

        assert IsloHook().build_client_kwargs() == {"api_key": "secret-key"}

    @mock.patch.object(IsloHook, "get_connection")
    def test_missing_api_key_is_rejected(self, get_connection):
        get_connection.return_value = _connection(password="")

        with pytest.raises(ValueError, match="has no password"):
            IsloHook().build_client_kwargs()

    @pytest.mark.parametrize("timeout", ["never", 0, -1, "inf"])
    @mock.patch.object(IsloHook, "get_connection")
    def test_invalid_timeout_is_rejected(self, get_connection, timeout):
        get_connection.return_value = _connection(extra={"timeout": timeout})

        with pytest.raises(ValueError, match="timeout must be a positive finite number"):
            IsloHook().build_client_kwargs()

    @mock.patch.object(IsloHook, "get_connection")
    def test_get_conn_builds_the_sdk_client_from_the_connection(self, get_connection):
        get_connection.return_value = _connection(password="key")
        islo_module = mock.MagicMock()
        with mock.patch.dict(sys.modules, {"islo": islo_module}):
            client = IsloHook().get_conn()

        islo_module.Islo.assert_called_once_with(api_key="key")
        assert client is islo_module.Islo.return_value

    @mock.patch.object(IsloHook, "get_conn")
    def test_test_connection_lists_one_sandbox(self, get_conn):
        ok, message = IsloHook().test_connection()

        assert (ok, message) == (True, "Connection successfully tested")
        get_conn.return_value.sandboxes.list_sandboxes.assert_called_once_with(limit=1)

    @mock.patch.object(IsloHook, "get_conn")
    def test_test_connection_reports_the_failure(self, get_conn):
        get_conn.return_value.sandboxes.list_sandboxes.side_effect = RuntimeError("401 Unauthorized")

        assert IsloHook().test_connection() == (False, "401 Unauthorized")
