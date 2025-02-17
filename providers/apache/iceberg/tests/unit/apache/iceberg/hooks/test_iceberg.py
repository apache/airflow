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

from unittest.mock import Mock, patch

import pytest
import requests_mock

from airflow.providers.apache.iceberg.hooks.iceberg import IcebergHook

pytestmark = pytest.mark.db_test


def test_iceberg_hook():
    access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSU"
    with patch("airflow.models.Connection.get_connection_from_secrets") as mock_get_connection:
        mock_conn = Mock()
        mock_conn.conn_id = "iceberg_default"
        mock_conn.host = "https://api.iceberg.io/ws/v1"
        mock_conn.extra_dejson = {}
        mock_get_connection.return_value = mock_conn
        with requests_mock.Mocker() as m:
            m.post(
                "https://api.iceberg.io/ws/v1/oauth/tokens",
                json={
                    "access_token": access_token,
                    "token_type": "Bearer",
                    "expires_in": 86400,
                    "warehouse_id": "fadc4c31-e81f-48cd-9ce8-64cd5ce3fa5d",
                    "region": "us-west-2",
                    "catalog_url": "warehouses/fadc4c31-e81f-48cd-9ce8-64cd5ce3fa5d",
                },
            )
            assert IcebergHook().get_conn() == access_token
