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

from airflow.models import Connection
from airflow.providers.common.ai.utils.config import ConnectionConfig
from airflow.providers.common.ai.utils.mixins import CommonAIHookMixin
from airflow.providers.common.compat.sdk import BaseHook


class TestCommonAIHookMixin:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="aws_default",
                conn_type="aws",
                login="fake_id",
                password="fake_secret",
                extra='{"region": "us-east-1"}',
            )
        )

    def test_remove_none_values(self):
        result = CommonAIHookMixin.remove_none_values({"a": 1, "b": None, "c": 2})
        assert result == {"a": 1, "c": 2}

    def test_get_conn_config_from_airflow_connection_success(self):

        with patch.object(CommonAIHookMixin, "_convert_airflow_connection") as mock_convert:
            mock_config = ConnectionConfig(conn_id="aws_default", credentials={}, extra_config={})
            mock_convert.return_value = mock_config

            mixin = CommonAIHookMixin()
            result = mixin.get_conn_config_from_airflow_connection("aws_default")

            assert result == mock_config
            mock_convert.assert_called_once()
            called_conn = mock_convert.call_args[0][0]
            assert called_conn.conn_id == "aws_default"

    @patch("airflow.providers.common.ai.utils.mixins.BaseHook.get_connection")
    def test_get_conn_config_from_airflow_connection_failure(self, mock_get_conn):
        mock_get_conn.side_effect = Exception("Connection error")

        mixin = CommonAIHookMixin()
        with pytest.raises(Exception, match="Connection error"):
            mixin.get_conn_config_from_airflow_connection("aws_default")

    def test_convert_airflow_connection(self):

        mixin = CommonAIHookMixin()
        conn = BaseHook.get_connection("aws_default")

        result = mixin._convert_airflow_connection(conn)
        expected = ConnectionConfig(
            conn_id="aws_default",
            credentials={
                "access_key_id": "fake_id",
                "secret_access_key": "fake_secret",
                "region": "us-east-1",
            },
            extra_config={"region": "us-east-1"},
        )
        assert result.conn_id == expected.conn_id
        assert result.credentials == expected.credentials
        assert result.extra_config == expected.extra_config

    def test_get_credentials_unknown_type(self):
        mock_conn = MagicMock()
        mock_conn.conn_type = "dummy"

        mixin = CommonAIHookMixin()
        with pytest.raises(ValueError, match="Unknown connection type dummy"):
            mixin._get_credentials(mock_conn)
