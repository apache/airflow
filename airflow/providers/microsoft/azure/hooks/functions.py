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

import requests
from azure.identity import ClientSecretCredential

from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.microsoft.azure.utils import get_field


class AzureFunctionsHook(HttpHook):
    """
    Invokes an Azure functions. You can invoke a function in azure by making http request.

    :param azure_functions_conn_id: The azure function connection ID to use
    """

    conn_name_attr = "azure_functions_conn_id"
    default_conn_name = "azure_functions_default"
    conn_type = "azure_functions"
    hook_name = "Azure Functions"

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "tenant_id": StringField(
                lazy_gettext("Tenant Id (Active Directory Auth)"), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ["port", "extra"],
            "relabeling": {
                "host": "Function URL",
                "login": "Client Id",
                "password": "Client Secret",
                "schema": "Scope",
            },
            "placeholders": {
                "host": "https://<APP_NAME>.azurewebsites.net",
            },
        }

    def __init__(
        self,
        azure_functions_conn_id: str = default_conn_name,
    ) -> None:
        super().__init__()
        self.azure_functions_conn_id = azure_functions_conn_id
        self.conn_id = azure_functions_conn_id

    def get_conn(self, *args, **kwargs) -> requests.Session:
        """Returns http session for use with requests."""
        session = requests.Session()
        auth_type = "client_key_type"
        conn = self.get_connection(self.azure_functions_conn_id)

        if conn.host and "://" in conn.host:
            self.base_url = conn.host

        tenant = get_field(conn_id=self.conn_id, conn_type=self.conn_type, extras={}, field_name="tenant_id")
        if tenant:
            # use Active Directory auth
            app_id = conn.login
            app_secret = conn.password
            scopes = conn.schema
            token_credential = ClientSecretCredential(tenant, app_id, app_secret).get_token(scopes).token
        elif conn.login and tenant is None:
            token_credential = conn.login
            auth_type = "functions_key_client"
        else:
            raise ValueError("Need client id or (tenant, client id, client secret) to authenticate")
        headers = self.get_headers(auth_type, token_credential)
        session.headers.update(headers)
        return session

    @staticmethod
    def get_headers(auth_type: str, token_key: str) -> dict[str, Any]:
        """Get Headers with auth keys."""
        headers: dict[str, Any] = {"Content-Type": "application/json"}
        if auth_type == "functions_key_type":
            headers["x-functions-key"] = token_key
        elif auth_type == "functions_key_client":
            headers["x-functions-client"] = token_key
        else:
            headers["Authorization"] = f"Bearer {token_key}"
        return headers
