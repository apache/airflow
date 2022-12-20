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
from requests import Response

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class AzureFunctionsHook(BaseHook):
    """
    Invokes an Azure function. You can invoke a function in azure by making http request

    :param method: request type of the Azure function HTTPTrigger type
    :param azure_function_conn_id: The azure function connection ID to use
    """

    conn_name_attr = "azure_functions_conn_id"
    default_conn_name = "azure_functions_default"
    conn_type = "azure_functions"
    hook_name = "Azure Functions"

    def __init__(
        self,
        method: str = "POST",
        azure_function_conn_id: str = default_conn_name,
        tcp_keep_alive: bool = True,
        tcp_keep_alive_idle: int = 120,
        tcp_keep_alive_count: int = 20,
        tcp_keep_alive_interval: int = 30,
    ) -> None:
        super().__init__()
        self.azure_function_conn_id = azure_function_conn_id
        self.method = method.upper()
        self.base_url: str = ""
        self.tcp_keep_alive = tcp_keep_alive
        self.keep_alive_idle = tcp_keep_alive_idle
        self.keep_alive_count = tcp_keep_alive_count
        self.keep_alive_interval = tcp_keep_alive_interval

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "tenant_id": StringField(
                lazy_gettext("Tenant Id (Active Directory Auth)"), widget=BS3TextFieldWidget()
            )
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["port", "extra"],
            "relabeling": {
                "host": "Function URL",
                "login": "Client Id",
                "password": "Client Secret",
                "schema": "Scope",
            },
            "placeholders": {
                "login": "client id",
                "password": "client secret",
                "host": "https://<APP_NAME>.azurewebsites.net",
                "schema": "scope",
                "tenant_id": "tenant",
            },
        }

    def get_conn(self, function_key: str | None = None) -> requests.Session:
        """
        Returns http session for use with requests

        :param function_key: function key to authenticate
        """
        session = requests.Session()
        auth_type = "client_key_type"
        conn = self.get_connection(self.azure_function_conn_id)
        extra = conn.extra_dejson or {}

        if conn.host and "://" in conn.host:
            self.base_url = conn.host

        tenant = self._get_field(extra, "tenant_id")
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

    def _get_field(self, extra_dict, field_name):
        prefix = "extra__wasb__"
        if field_name.startswith("extra__"):
            raise ValueError(
                f"Got prefixed name {field_name}; please remove the '{prefix}' prefix "
                f"when using this method."
            )
        if field_name in extra_dict:
            return extra_dict[field_name] or None
        return extra_dict.get(f"{prefix}{field_name}") or None

    @staticmethod
    def get_headers(auth_type: str, token_key: str) -> dict[str, Any]:
        """Get Headers with auth keys"""
        headers: dict[str, Any] = {"Content-Type": "application/json"}
        if auth_type == "functions_key_type":
            headers["x-functions-key"] = token_key
        elif auth_type == "functions_key_client":
            headers["x-functions-client"] = token_key
        else:
            headers["Authorization"] = f"Bearer {token_key}"
        return headers

    def invoke_function(
        self,
        function_name: str,
        endpoint: str | None = None,
        function_key: str | None = None,
        payload: dict[str, Any] | str | None = None,
    ) -> Response:
        """Invoke Azure Function by making http request with function name and url"""
        session = self.get_conn(function_key)
        if not endpoint:
            endpoint = f"/api/{function_name}"
        url = self.url_from_endpoint(endpoint)
        if self.method == "GET":
            # GET uses params
            req = requests.Request(self.method, url, params=payload)
        else:
            req = requests.Request(self.method, url, data=payload)
        prepped_request = session.prepare_request(req)
        response = session.send(prepped_request)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            self.log.error("HTTP error: %s", response.reason)
            self.log.error(response.text)
            raise AirflowException(str(response.status_code) + ":" + response.reason)
        return response

    def url_from_endpoint(self, endpoint: str | None) -> str:
        """Combine base url with endpoint"""
        if self.base_url and not self.base_url.endswith("/") and endpoint and not endpoint.startswith("/"):
            return self.base_url + "/" + endpoint
        return (self.base_url or "") + (endpoint or "")
