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

import asyncio
import time
from typing import TYPE_CHECKING, Any, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink
from airflow.providers.microsoft.azure.hooks.powerbi import (
    PowerBIDatasetRefreshFields,
    PowerBIHook,
)
from airflow.providers.microsoft.azure.triggers.powerbi import PowerBITrigger

if TYPE_CHECKING:
    from msgraph_core import APIVersion

    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.utils.context import Context


class PowerBILink(BaseOperatorLink):
    """Construct a link to monitor a dataset in Power BI."""

    name = "Monitor PowerBI Dataset"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey):
        url = (
            f"https://app.powerbi.com"  # type: ignore[attr-defined]
            f"/groups/{operator.group_id}/datasets/{operator.dataset_id}"  # type: ignore[attr-defined]
            f"/details?experience=power-bi"
        )

        return url


class PowerBIDatasetRefreshOperator(BaseOperator):
    """
    Refreshes a Power BI dataset.

    :param dataset_id: The dataset id.
    :param group_id: The workspace id.
    :param wait_for_termination: Wait until the pre-existing or current triggered refresh completes before exiting.
    :param conn_id: Airflow Connection ID that contains the connection information for the Power BI account used for authentication.
    :param timeout: Time in seconds to wait for a dataset to reach a terminal status for asynchronous waits. Used only if ``wait_for_termination`` is True.
    :param check_interval: Number of seconds to wait before rechecking the
        refresh status.
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
        wait_for_termination: bool = True,
        conn_id: str = PowerBIHook.default_conn_name,
        timeout: float = 60 * 60 * 24 * 7,
        proxies: dict | None = None,
        api_version: APIVersion | None = None,
        check_interval: int = 60,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.hook = PowerBIHook(conn_id=conn_id, proxies=proxies, api_version=api_version, timeout=timeout)
        self.dataset_id = dataset_id
        self.group_id = group_id
        self.wait_for_termination = wait_for_termination
        self.conn_id = conn_id
        self.timeout = timeout
        self.check_interval = check_interval

    @classmethod
    def run_async(cls, future: Any) -> Any:
        return asyncio.get_event_loop().run_until_complete(future)

    def execute(self, context: Context):
        """Refresh the Power BI Dataset."""
        self.log.info("Executing Dataset refresh.")
        refresh_id = self.run_async(
            self.hook.trigger_dataset_refresh(
                dataset_id=self.dataset_id,
                group_id=self.group_id,
            )
        )

        # Push Dataset Refresh ID to Xcom regardless of what happen during the refresh
        self.xcom_push(context=context, key="powerbi_dataset_refresh_id", value=refresh_id)

        if self.wait_for_termination:
            end_time = time.time() + self.timeout
            self.defer(
                trigger=PowerBITrigger(
                    conn_id=self.conn_id,
                    group_id=self.group_id,
                    dataset_id=self.dataset_id,
                    dataset_refresh_id=refresh_id,
                    end_time=end_time,
                    timeout=self.timeout,
                    check_interval=self.check_interval,
                    wait_for_termination=self.wait_for_termination,
                ),
                method_name=self.execute_complete.__name__,
            )

        # Retrieve refresh details after triggering refresh
        refresh_details = self.run_async(
            self.hook.get_refresh_details_by_refresh_id(
                dataset_id=self.dataset_id, group_id=self.group_id, refresh_id=refresh_id
            )
        )

        status = str(refresh_details.get(PowerBIDatasetRefreshFields.STATUS.value))
        error = str(refresh_details.get(PowerBIDatasetRefreshFields.ERROR.value))

        # Xcom Integration
        self.xcom_push(context=context, key="powerbi_dataset_refresh_status", value=status)
        self.xcom_push(context=context, key="powerbi_dataset_refresh_error", value=error)

    def execute_complete(self, context: Context, event: dict[str, str]) -> Any:
        """
        Return immediately - callback for when the trigger fires.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event:
            if event["status"] == "error":
                raise AirflowException(event["message"])
            else:
                # Push Dataset refresh status to Xcom
                self.xcom_push(context=context, key="powerbi_dataset_refresh_status", value=event["status"])
        self.log.info(event["message"])
