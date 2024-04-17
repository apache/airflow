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

from typing import Any

from pyiceberg.catalog import Catalog, load_catalog
from requests import HTTPError

from airflow.hooks.base import BaseHook

DEFAULT_TABULAR_URL = "https://api.tabular.io/ws"

TOKENS_ENDPOINT = "oauth/tokens"


class TabularHook(BaseHook):
    """
    This hook acts as a base hook for tabular services.

    It offers the ability to generate temporary, short-lived
    session tokens to use within Airflow submitted jobs.

    :param tabular_conn_id: The :ref:`Tabular connection id<howto/connection:tabular>`
        which refers to the information to connect to the Tabular OAuth service.
    """

    conn_name_attr = "tabular_conn_id"
    default_conn_name = "tabular_default"
    conn_type = "tabular"
    hook_name = "Tabular"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for Tabular connection."""
        return {
            "hidden_fields": ["port"],
            "relabeling": {
                "host": "Base URL",
                "login": "Client ID",
                "password": "Client Secret",
                "schema": "Warehouse",
            },
            "placeholders": {
                "host": DEFAULT_TABULAR_URL,
                "login": "client_id (token credentials auth)",
                "password": "secret (token credentials auth)",
                "schema": "Warehouse",
            },
        }

    def __init__(self, tabular_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn_id = tabular_conn_id

    def test_connection(self) -> tuple[bool, str]:
        """Test the Tabular connection."""
        try:
            self.get_conn()
            return True, "Successfully fetched token from Tabular"
        except HTTPError as e:
            return False, f"HTTP Error: {e}: {e.response.text}"
        except Exception as e:
            return False, str(e)

    def load_rest_catalog(self) -> Catalog:
        conn = self.get_connection(self.conn_id)
        base_url = conn.host if conn.host else DEFAULT_TABULAR_URL
        base_url = base_url.rstrip("/")
        client_id = conn.login
        client_secret = conn.password

        properties = {
            "type": "rest",
            "uri": base_url,
            "credential": f"{client_id}:{client_secret}",
            "warehouse": conn.schema,
        }
        return load_catalog(self.conn_id, **properties)

    def get_conn(self) -> str:
        """Obtain a short-lived access token via a client_id and client_secret."""
        cat = self.load_rest_catalog()
        return cat.properties["token"]

    def get_token_macro(self):
        return f"{{{{ conn.{self.conn_id}.get_hook().get_conn() }}}}"
