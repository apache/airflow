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

import time
from enum import Enum
from typing import TYPE_CHECKING, Any
from functools import cached_property
import aiohttp
import requests

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from azure.identity import ClientSecretCredential, ManagedIdentityCredential
import json

if TYPE_CHECKING:
    from airflow.models import Connection

TOKEN_REFRESH_LEAD_TIME = 120
DEFAULT_AAS_SCOPE = "https://*.asazure.windows.net"


class AasModelRefreshState:
    """
    A class to represent the refresh state of an Azure Analysis Services model.

    Attributes
    ----------
    SUCCEEDED : str
        Constant for the 'succeeded' state.
    IN_PROGRESS : str
        Constant for the 'inProgress' state.
    FAILED : str
        Constant for the 'failed' state.
    TIMEOUT : str
        Constant for the 'timedOut' state.
    CANCELLED : str
        Constant for the 'cancelled' state.
    NOT_STARTED : str
        Constant for the 'notStarted' state.
    TERMINAL_STATUSES : set
        Set of states that are considered terminal.
    INTERMEDIATE_STATES : set
        Set of states that are considered intermediate.
    FAILURE_STATES : set
        Set of states that are considered failure states.
    ALL_STATES : set
        Set of all possible states.

    Methods
    -------
    __init__(self, operation_id: str, state: str, errors: list = [])
        Initializes the AasModelRefreshState with the given operation ID, state, and errors.
    __str__(self)
        Returns the state as a string.
    error_message(self)
        Returns the errors as a formatted JSON string.
    """

    SUCCEEDED = "succeeded"
    IN_PROGRESS = "inProgress"
    FAILED = "failed"
    TIMEOUT = "timedOut"
    CANCELLED = "cancelled"
    NOT_STARTED = "notStarted"

    TERMINAL_STATUSES = {SUCCEEDED, FAILED, TIMEOUT, CANCELLED}
    INTERMEDIATE_STATES = {IN_PROGRESS, NOT_STARTED}
    FAILURE_STATES = {FAILED, TIMEOUT, CANCELLED}

    ALL_STATES = TERMINAL_STATUSES | INTERMEDIATE_STATES | FAILURE_STATES

    def __init__(self, operation_id: str, state: str, errors: list = []) -> None:
        """
        Constructs all the necessary attributes for the AasModelRefreshState object.

        Parameters
        ----------
            operation_id : str
                The ID of the operation.
            state : str
                The state of the model refresh.
            errors : list, optional
                A list of errors (default is an empty list).
        """
        if state not in self.ALL_STATES:
            raise AirflowException(f"Unexpected state: {state}")

        self.operation_id = operation_id
        self.state = state
        self.is_terminal = state in self.TERMINAL_STATUSES
        self.is_succeeded = state == self.SUCCEEDED
        self.errors = errors
        
    def __str__(self):
        """
        Returns the state as a string.

        Returns
        -------
        str
            The state of the model refresh.
        """
        return self.state
    
    @property
    def error_message(self):
        """
        Returns the errors as a formatted JSON string.

        Returns
        -------
        str
            The errors formatted as a JSON string.
        """
        return json.dumps(self.errors, indent=2)


class AasModelRefreshException(AirflowException):
    """An exception that indicates a model refresh failed to complete."""


class AasHook(BaseHook):
    """
    An asynchronous hook to interact with Azure Analysis Services.

    Attributes
    ----------
    conn_type : str
        The connection type, which is 'aas' for Azure Analysis Services.
    conn_name_attr : str
        The name of the connection attribute.
    default_conn_name : str
        The default connection name.
    hook_name : str
        The name of the hook.

    Methods
    -------
    __init__(self, conn_id: str = default_conn_name)
        Initializes the AasHook with the given connection ID.
    _is_token_valid(self, token: dict, time_key="expires_on") -> bool
        Checks if the given OAuth token is valid.
    aas_conn(self) -> Connection
        Returns the connection object for the given connection ID.
    """

    conn_type: str = "aas"
    conn_name_attr: str = "aas_conn_id"
    default_conn_name: str = "aas_default"
    hook_name: str = "Azure Analysis Services"

    def __init__(self, conn_id: str = default_conn_name) -> None:
        """
        Constructs all the necessary attributes for the AasHook object.

        Parameters
        ----------
        conn_id : str, optional
            The Azure Analysis Services connection ID (default is 'aas_default').
        """
        super().__init__()
        self.conn_id = conn_id
        self._token = None

    def _is_token_valid(self, token: dict, time_key="expires_on") -> bool:
        """
        Checks if the given OAuth token is valid.

        Parameters
        ----------
        token : dict
            The OAuth token to check.
        time_key : str, optional
            The key in the token dictionary that contains the expiration time (default is 'expires_on').

        Returns
        -------
        bool
            True if the token is valid, False otherwise.

        Raises
        ------
        AirflowException
            If the token does not contain the necessary data.
        """
        if "access_token" not in token or time_key not in token:
            raise AirflowException(
                f"Can't get necessary data from OAuth token: {token}"
            )

        return int(token[time_key]) > (int(time.time()) + TOKEN_REFRESH_LEAD_TIME)

    @cached_property
    def aas_conn(self) -> Connection:
        """
        Returns the connection object for the given connection ID.

        Returns
        -------
        Connection
            The connection object for the given connection ID.
        """
        return self.get_connection(self.conn_id)

    def _get_token(self):
        """
        Authenticate the resource using the connection id passed during initialization.

        This method synchronously obtains an OAuth token for Azure Analysis Services using either
        a managed identity or a client secret, depending on the connection configuration.

        Returns
        -------
        str
            The access token for the authenticated client.

        Raises
        ------
        AirflowException
            If the token does not contain the necessary data or if the token is invalid.
        """
        if self._token and self._is_token_valid(self._token):
            return self._token["access_token"]

        tenant = self.aas_conn.extra_dejson.get("tenantId")
        tenant = "6e93a626-8aca-4dc1-9191-ce291b4b75a1"

        if self.aas_conn.extra_dejson.get("use_azure_managed_identity", False):
            cred = ManagedIdentityCredential()
        else:
            cred = ClientSecretCredential(
                tenant_id=tenant,
                client_id=self.aas_conn.login,
                client_secret=self.aas_conn.password,
            )

        token = cred.get_token(f"{DEFAULT_AAS_SCOPE}/.default")
        self._token = {"access_token": token.token, "expires_on": token.expires_on}
        self._is_token_valid(self._token)
        
        return self._token["access_token"]

    async def _a_get_token(self):
        """
        Asynchronously authenticate the resource using the connection id passed during initialization.

        This method asynchronously obtains an OAuth token for Azure Analysis Services using either
        a managed identity or a client secret, depending on the connection configuration.

        Returns
        -------
        str
            The access token for the authenticated client.

        Raises
        ------
        AirflowException
            If the token does not contain the necessary data or if the token is invalid.
        """
        if self._token and await self._is_token_valid(self._token):
            return self._token["access_token"]

        tenant = self.aas_conn.extra_dejson.get("tenantId")

        if self.aas_conn.extra_dejson.get("use_azure_managed_identity", False):
            cred = await ManagedIdentityCredential()
        else:
            cred = await ClientSecretCredential(
                tenant_id=tenant,
                client_id=self.aas_conn.login,
                client_secret=self.aas_conn.password,
            )

        token = cred.get_token(f"{DEFAULT_AAS_SCOPE}/.default")
        self._token = {"access_token": token.token, "expires_on": token.expires_on}
        await self._is_token_valid(self._token)

        return self._token["access_token"]

    def _api_call(
        self,
        url: str = "",
        method: str = "GET",
        json: dict[str, Any] | str | None = None,
    ):
        """
        Make a synchronous API call to the specified URL with the given method and JSON payload.

        This method sends an HTTP request to the specified URL using the provided HTTP method and
        JSON payload. It includes an authorization header with a bearer token obtained from the
        _get_token method.

        Parameters
        ----------
        url : str, optional
            The URL to send the request to (default is an empty string).
        method : str, optional
            The HTTP method to use for the request (default is 'GET').
        json : dict[str, Any] | str | None, optional
            The JSON payload to include in the request (default is None).

        Returns
        -------
        requests.Response
            The response object resulting from the API call.

        Raises
        ------
        requests.HTTPError
            If the HTTP request returned an unsuccessful status code.
        """
        
        token = self._get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        fn_callmethods = {
            "POST": requests.post,
            "GET": requests.get,
            "DELETE": requests.delete,
        }
        fn_callmethod = fn_callmethods.get(method)
        response = fn_callmethod(url=url, headers=headers, json=json)
        response.raise_for_status()
        return response

    async def _a_api_call(
        self,
        url: str = "",
        method: str = "GET",
        json: dict[str, Any] | str | None = None,
    ):
        """
        Make an asynchronous API call to the specified URL with the given method and JSON payload.

        This method sends an HTTP request to the specified URL using the provided HTTP method and
        JSON payload. It includes an authorization header with a bearer token obtained from the
        _a_get_token method.

        Parameters
        ----------
        url : str, optional
            The URL to send the request to (default is an empty string).
        method : str, optional
            The HTTP method to use for the request (default is 'GET').
        json : dict[str, Any] | str | None, optional
            The JSON payload to include in the request (default is None).

        Returns
        -------
        dict
            The JSON response resulting from the API call.

        Raises
        ------
        aiohttp.ClientResponseError
            If the HTTP request returned an unsuccessful status code.
        """
        
        token = await self._a_get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        async with aiohttp.ClientSession() as session:
            async with session.request(
                method=method, url=url, headers=headers, json=json
            ) as response:
                response.raise_for_status()
                return await response.json()

    def get_refresh_history(
        self,
        server_name: str,
        server_location: str,
        database_name: str,
    ) -> list[dict[str, str]]:
        """
        Retrieve the refresh history of the specified model from the given server and database.

        Parameters
        ----------
        server_name : str
            The name of the Azure Analysis Services server.
        server_location : str
            The location of the Azure Analysis Services server.
        database_name : str
            The name of the database in the Azure Analysis Services server.

        Returns
        -------
        list[dict[str, str]]
            A list of dictionaries containing the operation ID and state of each refresh operation.

        Raises
        ------
        AasModelRefreshException
            If the refresh history could not be retrieved.
        """
        
        try:
            response = self._api_call(
                url=f"https://{server_location}.asazure.windows.net/servers/{server_name}/models/{database_name}/refreshes",
            )
            return [
                {
                    "operation_id": refresh.get("refreshId"),
                    "state": refresh.get("status"),
                }
                for refresh in response.json()
            ]

        except AirflowException:
            raise AasModelRefreshException("Failed to retrieve refresh history")

    def get_refresh_details(
        self,
        server_name: str,
        server_location: str,
        database_name: str,
        operation_id: str,
    ) -> list[dict[str, str]]:
        """
        Retrieve the details of a specific refresh operation from the given server and database.

        Parameters
        ----------
        server_name : str
            The name of the Azure Analysis Services server.
        server_location : str
            The location of the Azure Analysis Services server.
        database_name : str
            The name of the database in the Azure Analysis Services server.
        operation_id : str
            The ID of the refresh operation.

        Returns
        -------
        dict[str, str | list]
            A dictionary containing the operation ID, state, and any errors of the refresh operation.

        Raises
        ------
        AasModelRefreshException
            If the refresh details could not be retrieved.
        """
        
        try:
            refresh = self._api_call(
                url=f"https://{server_location}.asazure.windows.net/servers/{server_name}/models/{database_name}/refreshes/{operation_id}",
            ).json()
            errors = refresh.get("messages")
            return {
                "operation_id": operation_id,
                "state": refresh.get("status"),
                "errors": errors if errors else [],
            }

        except AirflowException:
            raise AasModelRefreshException("Failed to retrieve refresh state")

    async def a_get_refresh_details(
        self,
        server_name: str,
        server_location: str,
        database_name: str,
        operation_id: str,
    ) -> list[dict[str, str | list]]:
        """
        Asynchronously retrieve the details of a specific refresh operation from the given server and database.

        Parameters
        ----------
        server_name : str
            The name of the Azure Analysis Services server.
        server_location : str
            The location of the Azure Analysis Services server.
        database_name : str
            The name of the database in the Azure Analysis Services server.
        operation_id : str
            The ID of the refresh operation.

        Returns
        -------
        dict[str, str | list]
            A dictionary containing the operation ID, state, and any errors of the refresh operation.

        Raises
        ------
        AasModelRefreshException
            If the refresh details could not be retrieved.
        """
        
        try:
            refresh = await self._a_api_call(
                url=f"https://{server_location}.asazure.windows.net/servers/{server_name}/models/{database_name}/refreshes/{operation_id}",
            ).json()
            errors = refresh.get("messages")
            return {
                "operation_id": operation_id,
                "state": refresh.get("status"),
                "errors": errors if errors else [],
            }

        except AirflowException:
            raise AasModelRefreshException("Failed to retrieve refresh state")

    def initiate_refresh(
        self,
        server_name: str,
        server_location: str,
        database_name: str,
        refresh_type: str = "Full",
        commit_mode: str = "transactional",
        max_parallelism: int = 2,
        retry_count: int = 2,
        objects: list[dict[str, str]] = [],
    ) -> str:
        """
        Initiate a refresh operation on the specified model in the given server and database.

        Parameters
        ----------
        server_name : str
            The name of the Azure Analysis Services server.
        server_location : str
            The location of the Azure Analysis Services server.
        database_name : str
            The name of the database in the Azure Analysis Services server.
        refresh_type : str, optional
            The type of refresh to perform (default is 'Full').
        commit_mode : str, optional
            The commit mode for the refresh operation (default is 'transactional').
        max_parallelism : int, optional
            The maximum degree of parallelism for the refresh operation (default is 2).
        retry_count : int, optional
            The number of times to retry the refresh operation in case of failure (default is 2).
        objects : list[dict[str, str]], optional
            A list of objects to be included in the refresh operation (default is an empty list).

        Returns
        -------
        str
            The operation ID of the initiated refresh operation.

        Raises
        ------
        AasModelRefreshException
            If the refresh operation could not be initiated.
        """
        
        try:
            response = self._api_call(
                url=f"https://{server_location}.asazure.windows.net/servers/{server_name}/models/{database_name}/refreshes",
                method="POST",
                json={
                    "Type": refresh_type,
                    "CommitMode": commit_mode,
                    "MaxParallelism": max_parallelism,
                    "RetryCount": retry_count,
                    "Objects": objects,
                },
            )
            return response.json().get("operationId")

        except AirflowException:
            raise AasModelRefreshException("Failed to initiate refresh")

    def cancel_refresh(
        self,
        server_name: str,
        server_location: str,
        database_name: str,
        operation_id: str,
    ) -> None:
        """
        Cancel a specific refresh operation on the specified model in the given server and database.

        Parameters
        ----------
        server_name : str
            The name of the Azure Analysis Services server.
        server_location : str
            The location of the Azure Analysis Services server.
        database_name : str
            The name of the database in the Azure Analysis Services server.
        operation_id : str
            The ID of the refresh operation to be canceled.

        Returns
        -------
        None

        Raises
        ------
        AasModelRefreshException
            If the refresh operation could not be canceled.
        """
    
        try:
            self._api_call(
                url=f"https://{server_location}.asazure.windows.net/servers/{server_name}/models/{database_name}/refreshes/{operation_id}",
                method="DELETE",
            )

        except AirflowException:
            raise AasModelRefreshException("Failed to cancel refresh")
