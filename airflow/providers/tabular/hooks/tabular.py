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

from typing import Any, Dict, Tuple

import requests
from requests import HTTPError

from airflow.hooks.base import BaseHook

DEFAULT_TABULAR_URL = "https://api.tabulardata.io/ws/v1"

TOKENS_ENDPOINT = "oauth/tokens"


class TabularHook(BaseHook):
    """
    This hook acts as a base hook for tabular services. It offers the ability to generate temporary,
    short-lived session tokens to use within Airflow submitted jobs.

    :param tabular_conn_id: The :ref:`Tabular connection id<howto/connection:tabular>`
        which refers to the information to connect to the Tabular OAuth service.
    """

    conn_name_attr = 'tabular_conn_id'
    default_conn_name = "tabular_default"
    conn_type = "tabular"
    hook_name = "Tabular"

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "extra__tabular__baseUrl": StringField(
                lazy_gettext("Tabular Base URL"), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["schema", "port", "host"],
            "relabeling": {
                "login": "Tabular Client ID",
                "password": "Tabular Client Secret",
            },
            "placeholders": {
                "login": "client_id (token credentials auth)",
                "password": "secret (token credentials auth)",
                "extra__tabular__baseUrl": DEFAULT_TABULAR_URL,
            },
        }

    def __init__(self, tabular_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn_id = tabular_conn_id

    def test_connection(self) -> Tuple[bool, str]:
        """Test the Tabular connection."""
        try:
            self.get_conn()
            return True, "Successfully fetched token from Tabular"
        except HTTPError as e:
            return False, f"HTTP Error: {e}"
        except Exception as e:
            return False, str(e)

    def get_conn(self) -> str:
        """Obtain a short-lived access token via a client_id and client_secret."""
        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson
        client_id = conn.login
        client_secret = conn.password

        base_url = extras.get("extra__tabular__baseUrl", DEFAULT_TABULAR_URL)
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {"client_id": client_id, "client_secret": client_secret}

        response = requests.post(f"{base_url}/{TOKENS_ENDPOINT}", data=data, headers=headers)
        response.raise_for_status()

        return response.json()["access_token"]

    def get_token_macro(self):
        return f'{{{{ conn.{self.conn_id}.get_hook().get_conn() }}}}'
