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
from typing import Any, Callable

import requests
from azure.identity import ClientSecretCredential

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class PowerBIDatasetRefreshFields(Enum):
    """Power BI refresh dataset details."""

    REQUEST_ID = "request_id"
    STATUS = "status"
    END_TIME = "end_time"
    ERROR = "error"


class PowerBIDatasetRefreshStatus:
    """Power BI refresh dataset statuses."""

    # If the completion state is unknown or a refresh is in progress.
    IN_PROGRESS = "In Progress"
    FAILED = "Failed"
    COMPLETED = "Completed"
    DISABLED = "Disabled"

    TERMINAL_STATUSES = {FAILED, COMPLETED}


class PowerBIDatasetRefreshException(AirflowException):
    """An exception that indicates a dataset refresh failed to complete."""


class PowerBIHook(BaseHook):
    """
    A hook to interact with Power BI.

    :param powerbi_conn_id: Airflow Connection ID that contains the connection
        information for the Power BI account used for authentication.
    """

    conn_type: str = "powerbi"
    conn_name_attr: str = "powerbi_conn_id"
    default_conn_name: str = "powerbi_default"
    hook_name: str = "Power BI"

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "tenant_id": StringField(lazy_gettext("Tenant ID"), widget=BS3TextFieldWidget()),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "host", "extra"],
            "relabeling": {
                "login": "Client ID",
                "password": "Secret",
            },
        }

    def __init__(
        self,
        *,
        powerbi_conn_id: str = default_conn_name,
    ):
        self.conn_id = powerbi_conn_id
        self._api_version = "v1.0"
        self._base_url = "https://api.powerbi.com"
        super().__init__()

    def refresh_dataset(self, dataset_id: str, group_id: str) -> str:
        """
        Triggers a refresh for the specified dataset from the given group id.

        :param dataset_id: The dataset id.
        :param group_id: The workspace id.

        :return: Request id of the dataset refresh request.
        """
        url = f"{self._base_url}/{self._api_version}/myorg"

        # add the group id if it is specified
        url += f"/groups/{group_id}"

        # add the dataset key
        url += f"/datasets/{dataset_id}/refreshes"

        response = self._send_request("POST", url=url)

        if response.ok:
            request_id = response.headers["RequestId"]
            return request_id

        raise PowerBIDatasetRefreshException(
            "Failed to trigger dataset refresh. Status code: %s", str(response.status_code)
        )

    def _get_token(self) -> str:
        """Retrieve the access token used to authenticate against the API."""
        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson
        print(extras)
        tenant = extras.get("tenant_id", None)

        if not conn.login or not conn.password:
            raise ValueError("A Client ID and Secret is required to authenticate with Power BI.")

        if not tenant:
            raise ValueError("A Tenant ID is required when authenticating with Client ID and Secret.")

        credential = ClientSecretCredential(
            client_id=conn.login, client_secret=conn.password, tenant_id=tenant
        )

        resource = "https://analysis.windows.net/powerbi/api"

        access_token = credential.get_token(f"{resource}/.default")

        return access_token.token

    def get_refresh_history(
        self,
        dataset_id: str,
        group_id: str,
    ) -> list[dict[str, str]]:
        """
        Retrieve the refresh history of the specified dataset from the given group ID.

        :param dataset_id: The dataset ID.
        :param group_id: The workspace ID.

        :return: Dictionary containing all the refresh histories of the dataset.
        """
        url = f"{self._base_url}/{self._api_version}/myorg"

        # add the group id
        url += f"/groups/{group_id}"

        # add the dataset id
        url += f"/datasets/{dataset_id}/refreshes"

        raw_response = self._send_request("GET", url=url)

        if raw_response.ok:
            response = raw_response.json()
            refresh_histories = response.get("value")
            return [self.raw_to_refresh_details(refresh_history) for refresh_history in refresh_histories]

        raise PowerBIDatasetRefreshException(
            "Failed to retrieve refresh history. Status code: %s", str(response.status_code)
        )

    def raw_to_refresh_details(self, refresh_details: dict) -> dict[str, str]:
        """
        Convert raw refresh details into a dictionary containing required fields.

        :param refresh_details: Raw object of refresh details.
        """
        return {
            PowerBIDatasetRefreshFields.REQUEST_ID.value: str(refresh_details.get("requestId")),
            PowerBIDatasetRefreshFields.STATUS.value: (
                "In Progress"
                if str(refresh_details.get("status")) == "Unknown"
                else str(refresh_details.get("status"))
            ),
            PowerBIDatasetRefreshFields.END_TIME.value: str(refresh_details.get("endTime")),
            PowerBIDatasetRefreshFields.ERROR.value: str(refresh_details.get("serviceExceptionJson")),
        }

    def get_latest_refresh_details(self, dataset_id: str, group_id: str) -> dict[str, str] | None:
        """
        Get the refresh details of the most recent dataset refresh in the refresh history of the data source.

        :return: Dictionary containing refresh status and end time if refresh history exists, otherwise None.
        """
        history = self.get_refresh_history(dataset_id=dataset_id, group_id=group_id)

        if len(history) == 0:
            return None

        refresh_details = history[0]
        return refresh_details

    def get_refresh_details_by_request_id(self, dataset_id: str, group_id: str, request_id) -> dict[str, str]:
        """
        Get the refresh details of the given request Id.

        :param request_id: Request Id of the Dataset refresh.
        """
        refresh_histories = self.get_refresh_history(dataset_id=dataset_id, group_id=group_id)

        if len(refresh_histories) == 0:
            raise PowerBIDatasetRefreshException(
                f"Unable to fetch the details of dataset refresh with Request Id: {request_id}"
            )

        request_ids = [
            refresh_history.get(PowerBIDatasetRefreshFields.REQUEST_ID.value)
            for refresh_history in refresh_histories
        ]

        if request_id not in request_ids:
            raise PowerBIDatasetRefreshException(
                f"Unable to fetch the details of dataset refresh with Request Id: {request_id}"
            )

        request_id_index = request_ids.index(request_id)
        refresh_details = refresh_histories[request_id_index]

        return refresh_details

    def wait_for_dataset_refresh_status(
        self,
        *,
        expected_status: str,
        request_id: str,
        dataset_id: str,
        group_id: str,
        check_interval: int = 60,
        timeout: int = 60 * 60 * 24 * 7,
    ) -> bool:
        """
        Wait until the dataset refresh of given request ID has reached the expected status.

        :param expected_status: The desired status to check against a dataset refresh's current status.
        :param request_id: Request id for the dataset refresh request.
        :param check_interval: Time in seconds to check on a dataset refresh's status.
        :param timeout: Time in seconds to wait for a dataset to reach a terminal status or the expected status.
        :return: Boolean indicating if the dataset refresh has reached the ``expected_status`` before the timeout.
        """
        dataset_refresh_details = self.get_refresh_details_by_request_id(
            dataset_id=dataset_id, group_id=group_id, request_id=request_id
        )
        dataset_refresh_status = dataset_refresh_details.get(PowerBIDatasetRefreshFields.STATUS.value)

        start_time = time.monotonic()

        while (
            dataset_refresh_status not in PowerBIDatasetRefreshStatus.TERMINAL_STATUSES
            and dataset_refresh_status != expected_status
        ):
            # Check if the dataset-refresh duration has exceeded the ``timeout`` configured.
            if start_time + timeout < time.monotonic():
                raise PowerBIDatasetRefreshException(
                    f"Dataset refresh has not reached a terminal status after {timeout} seconds"
                )

            time.sleep(check_interval)

            dataset_refresh_details = self.get_refresh_details_by_request_id(
                dataset_id=dataset_id, group_id=group_id, request_id=request_id
            )
            dataset_refresh_status = dataset_refresh_details.get(PowerBIDatasetRefreshFields.STATUS.value)

        return dataset_refresh_status == expected_status

    def trigger_dataset_refresh(self, *, dataset_id: str, group_id: str) -> str:
        """
        Triggers the Power BI dataset refresh.

        :param dataset_id: The dataset ID.
        :param group_id: The workspace ID.

        :return: Request ID of the dataset refresh request.
        """
        # Start dataset refresh
        self.log.info("Starting dataset refresh.")
        request_id = self.refresh_dataset(dataset_id=dataset_id, group_id=group_id)

        return request_id

    def _send_request(self, request_type: str, url: str, **kwargs) -> requests.Response:
        """
        Send a request to the Power BI REST API.

        :param request_type: The type of the request (GET, POST, PUT, etc.).
        :param url: The URL against which the request needs to be made.
        :param kwargs: Additional keyword arguments to be passed to the request function.
        :return: The response object returned by the request.
        :raises requests.HTTPError: If the request fails (e.g., non-2xx status code).
        """
        self.header: dict[str, str] = {}

        request_funcs: dict[str, Callable[..., requests.Response]] = {
            "GET": requests.get,
            "POST": requests.post,
        }

        func: Callable[..., requests.Response] = request_funcs[request_type.upper()]

        response = func(url=url, headers={"Authorization": f"Bearer {self._get_token()}"}, **kwargs)

        return response
