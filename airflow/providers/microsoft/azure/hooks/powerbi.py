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

import logging
import time
from enum import Enum
from typing import TYPE_CHECKING, Any

from msgraph_core import APIVersion

from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class PowerBIDatasetRefreshFields(Enum):
    """Power BI refresh dataset details."""

    REQUEST_ID = "request_id"
    STATUS = "status"
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


class PowerBIHook(KiotaRequestAdapterHook):
    """
    A hook to interact with Power BI.

    :param conn_id: Airflow Connection ID that contains the connection
        information for the Power BI account used for authentication.
    """

    conn_type: str = "powerbi"
    default_conn_name: str = "powerbi_default"
    hook_name: str = "Power BI"

    def __init__(
        self,
        conn_id: str = default_conn_name,
        timeout: float | None = None,
        proxies: dict | None = None,
        api_version: APIVersion | str | None = None,
    ):
        super().__init__(
            conn_id=conn_id,
            timeout=timeout,
            proxies=proxies,
            host="https://api.powerbi.com",
            api_version=api_version,
        )

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

    async def refresh_dataset(self, dataset_id: str, group_id: str) -> str:
        """
        Triggers a refresh for the specified dataset from the given group id.

        :param dataset_id: The dataset id.
        :param group_id: The workspace id.

        :return: Request id of the dataset refresh request.
        """
        try:
            response = await self.run(
                url="myorg/groups/{group_id}/datasets/{dataset_id}/refreshes",
                method="POST",
                path_parameters={
                    "group_id": group_id,
                    "dataset_id": dataset_id,
                },
            )

            request_id = response.headers["RequestId"]
            return request_id
        except AirflowException:
            raise PowerBIDatasetRefreshException("Failed to trigger dataset refresh")

    async def get_refresh_history(
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
        try:
            response = await self.run(
                url="myorg/groups/{group_id}/datasets/{dataset_id}/refreshes",
                path_parameters={
                    "group_id": group_id,
                    "dataset_id": dataset_id,
                },
            )

            refresh_histories = response.json().get("value")
            return [self.raw_to_refresh_details(refresh_history) for refresh_history in refresh_histories]
        except AirflowException:
            raise PowerBIDatasetRefreshException("Failed to retrieve refresh history")

    @classmethod
    def raw_to_refresh_details(cls, refresh_details: dict) -> dict[str, str]:
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
            PowerBIDatasetRefreshFields.ERROR.value: str(refresh_details.get("serviceExceptionJson")),
        }

    async def get_latest_refresh_details(self, dataset_id: str, group_id: str) -> dict[str, str] | None:
        """
        Get the refresh details of the most recent dataset refresh in the refresh history of the data source.

        :return: Dictionary containing refresh status and end time if refresh history exists, otherwise None.
        """
        history = await self.get_refresh_history(dataset_id=dataset_id, group_id=group_id)

        if len(history) == 0:
            return None

        refresh_details = history[0]
        return refresh_details

    async def get_refresh_details_by_request_id(self, dataset_id: str, group_id: str, request_id) -> dict[str, str]:
        """
        Get the refresh details of the given request Id.

        :param request_id: Request Id of the Dataset refresh.
        """
        refresh_histories = await self.get_refresh_history(dataset_id=dataset_id, group_id=group_id)

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

    # TODO: This should definitely always be executed through a trigger, as this will be blocking otherwise
    async def wait_for_dataset_refresh_status(
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
        start_time = time.monotonic()

        while start_time + timeout > time.monotonic():
            dataset_refresh_details = await self.get_refresh_details_by_request_id(
                dataset_id=dataset_id, group_id=group_id, request_id=request_id
            )
            dataset_refresh_status = dataset_refresh_details.get(PowerBIDatasetRefreshFields.STATUS.value)

            if dataset_refresh_status in PowerBIDatasetRefreshStatus.TERMINAL_STATUSES:
                return dataset_refresh_status == expected_status

            logger.info(
                "Current dataset refresh status is %s. Sleeping for %s",
                dataset_refresh_status,
                check_interval,
            )
            time.sleep(check_interval)

        # Timeout reached
        return False

    # TODO: Why have a method with another name doing the same thing?
    async def trigger_dataset_refresh(self, *, dataset_id: str, group_id: str) -> str:
        """
        Triggers the Power BI dataset refresh.

        :param dataset_id: The dataset ID.
        :param group_id: The workspace ID.

        :return: Request ID of the dataset refresh request.
        """
        # Start dataset refresh
        self.log.info("Starting dataset refresh.")
        return await self.refresh_dataset(dataset_id=dataset_id, group_id=group_id)

    async def get_dataset_refresh_status(
        self, dataset_id: str, group_id: str, dataset_refresh_id: str
    ) -> str:
        """
        Retrieve the refresh status for the specified dataset refresh from the given group id.

        :param dataset_id: The dataset Id.
        :param group_id: The workspace Id.
        :param dataset_refresh_id: The dataset refresh Id.

        :return: Dataset refresh status.
        """
        response = await self.run(
            url="myorg/groups/{group_id}/datasets/{dataset_id}/refreshes",
            response_type=None,
            path_parameters={
                "group_id": group_id,
                "dataset_id": dataset_id,
            },
            method="GET",
        )
        # clean the raw refresh histories fetched from the API.
        raw_refresh_histories = response.get("value")
        clean_refresh_histories = [
            self.raw_to_refresh_details(refresh_history) for refresh_history in raw_refresh_histories
        ]

        for refresh_history in clean_refresh_histories:
            if refresh_history.get(PowerBIDatasetRefreshFields.REQUEST_ID.value) == dataset_refresh_id:
                return str(refresh_history.get(PowerBIDatasetRefreshFields.STATUS.value, "Unknown"))

        raise PowerBIDatasetRefreshException(
            f"Failed to retrieve the status of dataset refresh with Id: {dataset_refresh_id}"
        )

    async def cancel_dataset_refresh(self, dataset_id: str, group_id: str, dataset_refresh_id: str) -> None:
        """
        Cancel the dataset refresh.

        :param dataset_id: The dataset Id.
        :param group_id: The workspace Id.
        :param dataset_refresh_id: The dataset refresh Id.
        """
        await self.run(
            url="myorg/groups/{group_id}/datasets/{dataset_id}/refreshes/{dataset_refresh_id}",
            response_type=None,
            path_parameters={
                "group_id": group_id,
                "dataset_id": dataset_id,
                "dataset_refresh_id": dataset_refresh_id,
            },
            method="DELETE",
        )
