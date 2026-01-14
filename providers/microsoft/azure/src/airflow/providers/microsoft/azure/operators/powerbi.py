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

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowException, BaseOperator, BaseOperatorLink
from airflow.providers.microsoft.azure.hooks.powerbi import PowerBIHook
from airflow.providers.microsoft.azure.triggers.powerbi import (
    PowerBIDatasetListTrigger,
    PowerBITrigger,
    PowerBIWorkspaceListTrigger,
)

if TYPE_CHECKING:
    from msgraph_core import APIVersion

    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.sdk import Context


class PowerBILink(BaseOperatorLink):
    """Construct a link to monitor a dataset in Power BI."""

    name = "Monitor PowerBI Dataset"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey):
        url = (
            "https://app.powerbi.com"
            f"/groups/{operator.group_id}/datasets/{operator.dataset_id}"  # type: ignore[attr-defined]
            "/details?experience=power-bi"
        )

        return url


class PowerBIDatasetRefreshOperator(BaseOperator):
    """
    Refreshes a Power BI dataset.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PowerBIDatasetRefreshOperator`

    :param dataset_id: The dataset id.
    :param group_id: The workspace id.
    :param conn_id: Airflow Connection ID that contains the connection information for the Power BI account used for authentication.
    :param timeout: Time in seconds to wait for a dataset to reach a terminal status for asynchronous waits. Used only if ``wait_for_termination`` is True.
    :param check_interval: Number of seconds to wait before rechecking the
        refresh status.
    :param request_body: Additional arguments to pass to the request body, as described in https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/refresh-dataset-in-group#request-body.
    """

    template_fields: Sequence[str] = (
        "dataset_id",
        "group_id",
    )
    template_fields_renderers = {"parameters": "json"}

    operator_extra_links = (PowerBILink(),)

    def __init__(
        self,
        *,
        dataset_id: str,
        group_id: str,
        conn_id: str = PowerBIHook.default_conn_name,
        timeout: float = 60 * 60 * 24 * 7,
        proxies: dict | None = None,
        api_version: APIVersion | str | None = None,
        check_interval: int = 60,
        request_body: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.hook = PowerBIHook(conn_id=conn_id, proxies=proxies, api_version=api_version, timeout=timeout)
        self.dataset_id = dataset_id
        self.group_id = group_id
        self.wait_for_termination = True
        self.conn_id = conn_id
        self.timeout = timeout
        self.check_interval = check_interval
        self.request_body = request_body

    @property
    def proxies(self) -> dict | None:
        return self.hook.proxies

    @property
    def api_version(self) -> str | None:
        return self.hook.api_version

    def execute(self, context: Context):
        """Refresh the Power BI Dataset."""
        if self.wait_for_termination:
            self.defer(
                trigger=PowerBITrigger(
                    conn_id=self.conn_id,
                    group_id=self.group_id,
                    dataset_id=self.dataset_id,
                    timeout=self.timeout,
                    proxies=self.proxies,
                    api_version=self.api_version,
                    check_interval=self.check_interval,
                    wait_for_termination=self.wait_for_termination,
                    request_body=self.request_body,
                ),
                method_name=self.get_refresh_status.__name__,
            )

    def get_refresh_status(self, context: Context, event: dict[str, str] | None = None):
        """Push the refresh Id to XCom then runs the Trigger to wait for refresh completion."""
        if event:
            if event["status"] == "error":
                raise AirflowException(event["message"])

            dataset_refresh_id = event["dataset_refresh_id"]

        if dataset_refresh_id:
            context["ti"].xcom_push(
                key=f"{self.task_id}.powerbi_dataset_refresh_Id",
                value=dataset_refresh_id,
            )
            self.defer(
                trigger=PowerBITrigger(
                    conn_id=self.conn_id,
                    group_id=self.group_id,
                    dataset_id=self.dataset_id,
                    dataset_refresh_id=dataset_refresh_id,
                    timeout=self.timeout,
                    proxies=self.proxies,
                    api_version=self.api_version,
                    check_interval=self.check_interval,
                    wait_for_termination=self.wait_for_termination,
                ),
                method_name=self.execute_complete.__name__,
            )

    def execute_complete(self, context: Context, event: dict[str, str]) -> Any:
        """
        Return immediately - callback for when the trigger fires.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event:
            context["ti"].xcom_push(
                key=f"{self.task_id}.powerbi_dataset_refresh_status",
                value=event["dataset_refresh_status"],
            )
            if event["status"] == "error":
                raise AirflowException(event["message"])


class PowerBIWorkspaceListOperator(BaseOperator):
    """
    Gets a list of workspaces where the service principal from the connection is assigned as admin.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PowerBIWorkspaceListOperator`

    :param conn_id: The connection Id to connect to PowerBI.
    :param timeout: The HTTP timeout being used by the `KiotaRequestAdapter`. Default is 1 week (60s * 60m * 24h * 7d).
        When no timeout is specified or set to None then there is no HTTP timeout on each request.
    :param proxies: A dict defining the HTTP proxies to be used (default is None).
    :param api_version: The API version of the Microsoft Graph API to be used (default is v1).
        You can pass an enum named APIVersion which has 2 possible members v1 and beta,
        or you can pass a string as `v1.0` or `beta`.
    """

    def __init__(
        self,
        *,
        conn_id: str = PowerBIHook.default_conn_name,
        timeout: float = 60 * 60 * 24 * 7,
        proxies: dict | None = None,
        api_version: APIVersion | str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.hook = PowerBIHook(conn_id=conn_id, proxies=proxies, api_version=api_version, timeout=timeout)
        self.conn_id = conn_id
        self.timeout = timeout

    @property
    def proxies(self) -> dict | None:
        return self.hook.proxies

    @property
    def api_version(self) -> str | None:
        return self.hook.api_version

    def execute(self, context: Context):
        """List visible PowerBI Workspaces."""
        self.defer(
            trigger=PowerBIWorkspaceListTrigger(
                conn_id=self.conn_id,
                timeout=self.timeout,
                proxies=self.proxies,
                api_version=self.api_version,
            ),
            method_name=self.execute_complete.__name__,
        )

    def execute_complete(self, context: Context, event: dict[str, str]) -> Any:
        """
        Return immediately - callback for when the trigger fires.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event:
            context["ti"].xcom_push(
                key=f"{self.task_id}.powerbi_workspace_ids",
                value=event["workspace_ids"],
            )
            if event["status"] == "error":
                raise AirflowException(event["message"])


class PowerBIDatasetListOperator(BaseOperator):
    """
    Gets a list of datasets where the service principal from the connection is assigned as admin.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PowerBIDatasetListOperator`

    :param conn_id: The connection Id to connect to PowerBI.
    :param group_id: The group Id to list discoverable datasets.
    :param timeout: The HTTP timeout being used by the `KiotaRequestAdapter`. Default is 1 week (60s * 60m * 24h * 7d).
        When no timeout is specified or set to None then there is no HTTP timeout on each request.
    :param proxies: A dict defining the HTTP proxies to be used (default is None).
    :param api_version: The API version of the Microsoft Graph API to be used (default is v1).
        You can pass an enum named APIVersion which has 2 possible members v1 and beta,
        or you can pass a string as `v1.0` or `beta`.
    """

    def __init__(
        self,
        *,
        group_id: str,
        conn_id: str = PowerBIHook.default_conn_name,
        timeout: float = 60 * 60 * 24 * 7,
        proxies: dict | None = None,
        api_version: APIVersion | str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.hook = PowerBIHook(conn_id=conn_id, proxies=proxies, api_version=api_version, timeout=timeout)
        self.conn_id = conn_id
        self.group_id = group_id
        self.timeout = timeout

    @property
    def proxies(self) -> dict | None:
        return self.hook.proxies

    @property
    def api_version(self) -> str | None:
        return self.hook.api_version

    def execute(self, context: Context):
        """List visible PowerBI datasets within group (Workspace)."""
        self.defer(
            trigger=PowerBIDatasetListTrigger(
                conn_id=self.conn_id,
                timeout=self.timeout,
                proxies=self.proxies,
                api_version=self.api_version,
                group_id=self.group_id,
            ),
            method_name=self.execute_complete.__name__,
        )

    def execute_complete(self, context: Context, event: dict[str, str]) -> Any:
        """
        Return immediately - callback for when the trigger fires.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event:
            context["ti"].xcom_push(
                key=f"{self.task_id}.powerbi_dataset_ids",
                value=event["dataset_ids"],
            )
            if event["status"] == "error":
                raise AirflowException(event["message"])
