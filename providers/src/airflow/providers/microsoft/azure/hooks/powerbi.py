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

from enum import Enum
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook

if TYPE_CHECKING:
    from msgraph_core import APIVersion


class PowerBIDatasetRefreshFields(Enum):
    """Power BI refresh dataset details."""

    REQUEST_ID = "request_id"
    STATUS = "status"
    ERROR = "error"


class PowerBIDatasetRefreshStatus:
    """Power BI refresh dataset statuses."""

    IN_PROGRESS = "In Progress"
    FAILED = "Failed"
    COMPLETED = "Completed"
    DISABLED = "Disabled"

    TERMINAL_STATUSES = {FAILED, COMPLETED}


class PowerBIDatasetRefreshException(AirflowException):
    """An exception that indicates a dataset refresh failed to complete."""


class PowerBIHook(KiotaRequestAdapterHook):
    """
    A async hook to interact with Power BI.

    :param conn_id: The Power BI connection id.
    """

    conn_type: str = "powerbi"
    conn_name_attr: str = "conn_id"
    default_conn_name: str = "powerbi_default"
    hook_name: str = "Power BI"

    def __init__(
        self,
        conn_id: str = default_conn_name,
        proxies: dict | None = None,
        timeout: float = 60 * 60 * 24 * 7,
        api_version: APIVersion | str | None = None,
    ):
        super().__init__(
            conn_id=conn_id,
            proxies=proxies,
            timeout=timeout,
            host="https://api.powerbi.com",
            scopes=["https://analysis.windows.net/powerbi/api/.default"],
            api_version=api_version,
        )

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "tenant_id": StringField(
                lazy_gettext("Tenant ID"), widget=BS3TextFieldWidget()
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "host", "extra"],
            "relabeling": {
                "login": "Client ID",
                "password": "Client Secret",
            },
        }

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

            refresh_histories = response.get("value")
            return [
                self.raw_to_refresh_details(refresh_history)
                for refresh_history in refresh_histories
            ]

        except AirflowException:
            raise PowerBIDatasetRefreshException("Failed to retrieve refresh history")

    @classmethod
    def raw_to_refresh_details(cls, refresh_details: dict) -> dict[str, str]:
        """
        Convert raw refresh details into a dictionary containing required fields.

        :param refresh_details: Raw object of refresh details.
        """
        return {
            PowerBIDatasetRefreshFields.REQUEST_ID.value: str(
                refresh_details.get("requestId")
            ),
            PowerBIDatasetRefreshFields.STATUS.value: (
                "In Progress"
                if str(refresh_details.get("status")) == "Unknown"
                else str(refresh_details.get("status"))
            ),
            PowerBIDatasetRefreshFields.ERROR.value: str(
                refresh_details.get("serviceExceptionJson")
            ),
        }

    async def get_refresh_details_by_refresh_id(
        self, dataset_id: str, group_id: str, refresh_id: str
    ) -> dict[str, str]:
        """
        Get the refresh details of the given request Id.

        :param refresh_id: Request Id of the Dataset refresh.
        """
        refresh_histories = await self.get_refresh_history(
            dataset_id=dataset_id, group_id=group_id
        )

        if len(refresh_histories) == 0:
            raise PowerBIDatasetRefreshException(
                f"Unable to fetch the details of dataset refresh with Request Id: {refresh_id}"
            )

        refresh_ids = [
            refresh_history.get(PowerBIDatasetRefreshFields.REQUEST_ID.value)
            for refresh_history in refresh_histories
        ]

        if refresh_id not in refresh_ids:
            raise PowerBIDatasetRefreshException(
                f"Unable to fetch the details of dataset refresh with Request Id: {refresh_id}"
            )

        refresh_details = refresh_histories[refresh_ids.index(refresh_id)]

        return refresh_details

    async def trigger_dataset_refresh(self, *, dataset_id: str, group_id: str) -> str:
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

            request_id = response.get("requestid")
            return request_id
        except AirflowException:
            raise PowerBIDatasetRefreshException("Failed to trigger dataset refresh.")

    async def cancel_dataset_refresh(
        self, dataset_id: str, group_id: str, dataset_refresh_id: str
    ) -> None:
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
