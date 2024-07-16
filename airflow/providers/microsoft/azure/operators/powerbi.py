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

from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from airflow.configuration import conf
from airflow.models import BaseOperator, BaseOperatorLink
from airflow.providers.microsoft.azure.hooks.powerbi import (
    PowerBIDatasetRefreshException,
    PowerBIDatasetRefreshFields,
    PowerBIDatasetRefreshStatus,
    PowerBIHook,
)

if TYPE_CHECKING:
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

    By default the operator will wait until the refresh has completed before
    exiting. The refresh status is checked every 60 seconds as a default. This
    can be changed by specifying a new value for `check_interval`.

    :param dataset_id: The dataset id.
    :param group_id: The workspace id.
    :param wait_for_termination: Wait until the pre-existing or current triggered refresh completes before exiting.
    :param force_refresh: Force refresh if pre-existing refresh found.
    :param powerbi_conn_id: Airflow Connection ID that contains the connection
        information for the Power BI account used for authentication.
    :param timeout: Time in seconds to wait for a dataset to reach a terminal status for non-asynchronous waits. Used only if ``wait_for_termination`` is True.
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
        *,  # Indicates all the following parameters must be specified using keyword arguments.
        dataset_id: str,
        group_id: str,
        wait_for_termination: bool = True,
        force_refresh: bool = False,
        powerbi_conn_id: str = PowerBIHook.default_conn_name,
        timeout: int = 60 * 60 * 24 * 7,
        check_interval: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.group_id = group_id
        self.wait_for_termination = wait_for_termination
        self.force_refresh = force_refresh
        self.powerbi_conn_id = powerbi_conn_id
        self.timeout = timeout
        self.check_interval = check_interval
        self.deferrable = deferrable

    @cached_property
    def hook(self) -> PowerBIHook:
        """Create and return an PowerBIHook (cached)."""
        return PowerBIHook(powerbi_conn_id=self.powerbi_conn_id)

    def execute(self, context: Context):
        """Refresh the Power BI Dataset."""
        self.log.info("Check if a refresh is already in progress.")
        refresh_details = self.hook.get_latest_refresh_details(
            dataset_id=self.dataset_id, group_id=self.group_id
        )

        if (
            refresh_details is None
            or refresh_details.get(PowerBIDatasetRefreshFields.STATUS.value)
            in PowerBIDatasetRefreshStatus.TERMINAL_STATUSES
        ):
            self.log.info("No pre-existing refresh found.")
            request_id = self.hook.trigger_dataset_refresh(
                dataset_id=self.dataset_id,
                group_id=self.group_id,
            )

            if self.wait_for_termination:
                self.log.info("Waiting for dataset refresh to terminate.")
                if self.hook.wait_for_dataset_refresh_status(
                    request_id=request_id,
                    dataset_id=self.dataset_id,
                    group_id=self.group_id,
                    expected_status=PowerBIDatasetRefreshStatus.COMPLETED,
                ):
                    self.log.info("Dataset refresh %s has completed successfully.", request_id)
                else:
                    raise PowerBIDatasetRefreshException(
                        f"Dataset refresh {request_id} has failed or has been cancelled."
                    )
        else:
            # If in-progress pre-existing refresh is found.
            if (
                refresh_details.get(PowerBIDatasetRefreshFields.STATUS.value)
                == PowerBIDatasetRefreshStatus.IN_PROGRESS
            ):
                request_id = str(refresh_details.get(PowerBIDatasetRefreshFields.REQUEST_ID.value))
                self.log.info("Found pre-existing dataset refresh request: %s.", request_id)

                if self.force_refresh or self.wait_for_termination:
                    self.log.info("Waiting for dataset refresh %s to terminate.", request_id)
                    if self.hook.wait_for_dataset_refresh_status(
                        request_id=request_id,
                        dataset_id=self.dataset_id,
                        group_id=self.group_id,
                        expected_status=PowerBIDatasetRefreshStatus.COMPLETED,
                        check_interval=self.check_interval,
                        timeout=self.timeout,
                    ):
                        self.log.info(
                            "Pre-existing dataset refresh %s has completed successfully.", request_id
                        )
                    else:
                        raise PowerBIDatasetRefreshException(
                            f"Pre-exisintg dataset refresh {request_id} has failed or has been cancelled."
                        )

                    if self.force_refresh:
                        self.log.info("Starting new refresh.")
                        request_id = self.hook.trigger_dataset_refresh(
                            dataset_id=self.dataset_id,
                            group_id=self.group_id,
                        )

                        if self.wait_for_termination:
                            self.log.info("Waiting for dataset refresh to terminate.")
                            if self.hook.wait_for_dataset_refresh_status(
                                request_id=request_id,
                                dataset_id=self.dataset_id,
                                group_id=self.group_id,
                                expected_status=PowerBIDatasetRefreshStatus.COMPLETED,
                            ):
                                self.log.info("Dataset refresh %s has completed successfully.", request_id)
                            else:
                                raise PowerBIDatasetRefreshException(
                                    f"Dataset refresh {request_id} has failed or has been cancelled."
                                )

        # Retrieve refresh details after triggering refresh
        refresh_details = self.hook.get_refresh_details_by_request_id(
            dataset_id=self.dataset_id, group_id=self.group_id, request_id=request_id
        )

        request_id = str(refresh_details.get(PowerBIDatasetRefreshFields.REQUEST_ID.value))
        status = str(refresh_details.get(PowerBIDatasetRefreshFields.STATUS.value))
        end_time = str(refresh_details.get(PowerBIDatasetRefreshFields.END_TIME.value))
        error = str(refresh_details.get(PowerBIDatasetRefreshFields.ERROR.value))

        # Xcom Integration
        context["ti"].xcom_push(key="powerbi_dataset_refresh_id", value=request_id)
        context["ti"].xcom_push(key="powerbi_dataset_refresh_status", value=status)
        context["ti"].xcom_push(key="powerbi_dataset_refresh_end_time", value=end_time)
        context["ti"].xcom_push(key="powerbi_dataset_refresh_error", value=error)
