from __future__ import annotations

from typing import Any, Dict
import requests
from airflow.providers.amazon.get_provider_info import get_provider_info

from airflow import AirflowException
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

    def get_conn(self, function_key: str | None = None) -> requests.Session:
        """
        Returns http session for use with requests

        :param function_key: function key to authenticate
        """
        session = requests.Session()
        auth_type = "client_key_type"
        if self.azure_function_conn_id:
            conn = self.get_connection(self.azure_function_conn_id)

            if conn.host and "://" in conn.host:
                self.base_url = conn.host

        if function_key:
            auth_type = "functions_key_type"
            token_key = function_key
        headers = self.get_headers(auth_type, token_key)
        session.headers.update(headers)

    @staticmethod
    def get_headers(auth_type: str, token_key: str) -> Dict[str, Any]:
        """Get Headers, tenants from the connection details"""
        headers: Dict[str, Any] = {}
        provider_info = get_provider_info()
        package_name = provider_info["package-name"]
        version = provider_info["versions"]
        headers["User-Agent"] = f"{package_name}-v{version}"
        headers["Content-Type"] = "application/json"
        if auth_type == "functions_key_type":
            headers["x-functions-key"] = token_key
        else:
            headers["x-functions-clientid"] = token_key
        return headers

    def invoke_function(self, function_name: str, endpoint: str, function_key: str,
                        payload: dict[str, Any] | str | None = None):
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

