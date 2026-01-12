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

from typing import Any, cast

import requests
from requests import HTTPError

from airflow.providers.common.compat.sdk import BaseHook

TOKENS_ENDPOINT = "oauth/tokens"


class IcebergHook(BaseHook):
    """
    This hook acts as a base hook for iceberg services.

    It offers the ability to generate temporary, short-lived
    session tokens to use within Airflow submitted jobs.

    :param iceberg_conn_id: The :ref:`Iceberg connection id<howto/connection:iceberg>`
        which refers to the information to connect to the Iceberg.
    """

    conn_name_attr = "iceberg_conn_id"
    default_conn_name = "iceberg_default"
    conn_type = "iceberg"
    hook_name = "Iceberg"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for Iceberg connection."""
        return {
            "hidden_fields": ["schema", "port"],
            "relabeling": {
                "host": "Base URL",
                "login": "Client ID",
                "password": "Client Secret",
            },
            "placeholders": {
                "login": "client_id (token credentials auth)",
                "password": "secret (token credentials auth)",
            },
        }

    def __init__(self, iceberg_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn_id = iceberg_conn_id

    def test_connection(self) -> tuple[bool, str]:
        """Test the Iceberg connection."""
        try:
            self.get_conn()
            return True, "Successfully fetched token from Iceberg"
        except HTTPError as e:
            return False, f"HTTP Error: {e}: {e.response.text}"
        except Exception as e:
            return False, str(e)

    def get_conn(self) -> str:
        """Obtain a short-lived access token via a client_id and client_secret."""
        conn = self.get_connection(self.conn_id)
        base_url = cast("str", conn.host)
        base_url = base_url.rstrip("/")
        client_id = conn.login
        client_secret = conn.password
        data = {"client_id": client_id, "client_secret": client_secret, "grant_type": "client_credentials"}

        response = requests.post(f"{base_url}/{TOKENS_ENDPOINT}", data=data)
        response.raise_for_status()

        return response.json()["access_token"]

    def get_token_macro(self):
        return f"{{{{ conn.{self.conn_id}.get_hook().get_conn() }}}}"
