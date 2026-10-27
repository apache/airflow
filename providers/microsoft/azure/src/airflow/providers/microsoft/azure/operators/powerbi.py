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

from airflow.providers.common.compat.sdk import (
    AirflowException,
    BaseOperator,
    BaseOperatorLink,
    ResumableJobMixin,
    conf,
)
from airflow.providers.microsoft.azure.hooks.powerbi import (
    PowerBIDatasetRefreshException,
    PowerBIDatasetRefreshFields,
    PowerBIDatasetRefreshStatus,
    PowerBIHook,
)
from airflow.providers.microsoft.azure.triggers.powerbi import (
    PowerBIDatasetListTrigger,
    PowerBITrigger,
    PowerBIWorkspaceListTrigger,
)

if TYPE_CHECKING:
    from msgraph_core import APIVersion
    from pydantic import JsonValue

    from airflow.providers.common.compat.sdk import TaskInstanceKey
    from airflow.sdk import Context

# Status reported for a stored refresh id that is no longer in the dataset's refresh history.
_REFRESH_NOT_IN_HISTORY = "Not in refresh history"


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


class PowerBIDatasetRefreshOperator(ResumableJobMixin, BaseOperator):
    """
    Refreshes a Power BI dataset.

    When durable, the refresh is started from the worker, and its id is kept in the task state store
    before the task defers to wait for it. A retry, for instance after a worker crash, reconnects to that
    refresh while it is still in progress, and does not start a new one once it has completed. This needs
    Airflow 3.3 or later. Otherwise the trigger starts the refresh, as in earlier versions of this
    provider: only the triggerer talks to Power BI, and every try starts a new refresh.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PowerBIDatasetRefreshOperator`

    :param dataset_id: The dataset id.
    :param group_id: The workspace id.
    :param conn_id: Airflow Connection ID that contains the connection information for the Power BI account used for authentication.
    :param timeout: Time in seconds to wait for a dataset to reach a terminal status for asynchronous waits.
    :param check_interval: Number of seconds to wait before rechecking the
        refresh status.
    :param request_body: Additional arguments to pass to the request body, as described in https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/refresh-dataset-in-group#request-body.
    :param wait_for_completion: If True, wait for the dataset refresh to complete. If False, trigger the refresh and return immediately without waiting.
    :param deferrable: This parameter is deprecated and no longer has any effect. The operator now always
        uses deferrable execution.
    :param durable: Start the refresh from the worker and reconnect to it on a retry instead of starting
        a new one (default is True). A durable refresh is not cancelled when polling it fails, so that
        the retry can reconnect to it. Set it to False to keep the behaviour of earlier versions: the
        trigger starts the refresh, every try starts a new one, and it is cancelled when polling fails.
        Needs Airflow 3.3 or later; below it the operator behaves as with ``durable=False``.
    """

    external_id_key = "powerbi_dataset_refresh_id"

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
        wait_for_completion: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if "deferrable" in kwargs or deferrable is not True:
            self.log.warning("The PowerBIDatasetRefreshOperator now always uses deferrable execution.")
        self.hook = PowerBIHook(conn_id=conn_id, proxies=proxies, api_version=api_version, timeout=timeout)
        self.dataset_id = dataset_id
        self.group_id = group_id
        self.wait_for_completion = wait_for_completion
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
        if not self.durable:
            # As in earlier versions of this provider: the trigger starts the refresh, so only the
            # triggerer talks to Power BI, and every try starts a new refresh.
            self.defer(
                trigger=PowerBITrigger(
                    conn_id=self.conn_id,
                    group_id=self.group_id,
                    dataset_id=self.dataset_id,
                    timeout=self.timeout,
                    proxies=self.proxies,
                    api_version=self.api_version,
                    check_interval=self.check_interval,
                    wait_for_termination=False,
                    request_body=self.request_body,
                ),
                method_name=self.handle_refresh.__name__,
            )
        return self.execute_resumable(context)

    def submit_job(self, context: Context) -> str:
        dataset_refresh_id = self.hook.run_sync(
            self.hook.trigger_dataset_refresh(
                dataset_id=self.dataset_id,
                group_id=self.group_id,
                request_body=self.request_body,
            )
        )
        if not dataset_refresh_id:
            raise PowerBIDatasetRefreshException("Failed to trigger the dataset refresh.")
        self.log.info("Triggered dataset refresh %s", dataset_refresh_id)
        return dataset_refresh_id

    def get_job_status(self, external_id: JsonValue, context: Context) -> str:
        refresh_histories = self.hook.run_sync(
            self.hook.get_refresh_history(dataset_id=self.dataset_id, group_id=self.group_id)
        )
        if not any(
            details[PowerBIDatasetRefreshFields.REQUEST_ID.value] == external_id
            for details in refresh_histories
        ):
            # Power BI keeps a limited refresh history; a refresh that dropped out of it is not resumable.
            return _REFRESH_NOT_IN_HISTORY
        refresh_details = self.hook.run_sync(
            self.hook.get_refresh_details_by_refresh_id(
                dataset_id=self.dataset_id, group_id=self.group_id, refresh_id=str(external_id)
            )
        )
        return refresh_details[PowerBIDatasetRefreshFields.STATUS.value]

    def is_job_active(self, status: str) -> bool:
        # Without wait_for_completion a retry does not wait for a refresh in progress either:
        # is_job_succeeded counts it as done.
        return self.wait_for_completion and status == PowerBIDatasetRefreshStatus.IN_PROGRESS

    def is_job_succeeded(self, status: str) -> bool:
        if status == PowerBIDatasetRefreshStatus.COMPLETED:
            return True
        # Without wait_for_completion the task only has to start the refresh, so one in progress counts
        # as done. The mixin then logs it as completed and counts it in resumable_job.already_succeeded,
        # but only this path pushes the refresh id: the mixin reconnects to an active job through
        # poll_until_complete alone, without get_job_result.
        return not self.wait_for_completion and status == PowerBIDatasetRefreshStatus.IN_PROGRESS

    def poll_until_complete(self, external_id: JsonValue, context: Context) -> None:
        dataset_refresh_id = str(external_id)
        if not self.wait_for_completion:
            # get_job_result pushes the refresh id, the mixin calls it next.
            self.log.info("Triggered dataset refresh %s (fire-and-forget)", dataset_refresh_id)
            return

        self._push_refresh_xcom(context, dataset_refresh_id)
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
                wait_for_termination=True,
                cancel_on_error=not self.durable,
            ),
            method_name=self.execute_complete.__name__,
        )

    def get_job_result(self, external_id: JsonValue, context: Context) -> None:
        # With wait_for_completion the mixin only gets here for a refresh an earlier try saw complete,
        # otherwise poll_until_complete defers and execute_complete pushes the id and the status.
        self._push_refresh_xcom(
            context,
            str(external_id),
            PowerBIDatasetRefreshStatus.COMPLETED if self.wait_for_completion else None,
        )

    def _push_refresh_xcom(
        self, context: Context, dataset_refresh_id: str, dataset_refresh_status: str | None = None
    ) -> None:
        context["ti"].xcom_push(key=f"{self.task_id}.powerbi_dataset_refresh_id", value=dataset_refresh_id)
        if dataset_refresh_status:
            context["ti"].xcom_push(
                key=f"{self.task_id}.powerbi_dataset_refresh_status", value=dataset_refresh_status
            )

    def handle_refresh(self, context: Context, event: dict[str, str] | None) -> None:
        """
        Handle refresh-trigger event and optionally defer again to wait for refresh completion.

        The trigger started the refresh: the task is not durable, or was deferred by an earlier version
        of this provider.

        :param context: Airflow context dictionary
        :param event: Event dict from trigger with status and dataset_refresh_id
        """
        if not event:
            return

        dataset_refresh_id = event.get("dataset_refresh_id")
        if dataset_refresh_id:
            context["ti"].xcom_push(
                key=f"{self.task_id}.powerbi_dataset_refresh_id",
                value=dataset_refresh_id,
            )

        if event["status"] == "error":
            raise AirflowException(event["message"])

        if not self.wait_for_completion:
            self.log.info("Triggered dataset refresh %s (fire-and-forget)", dataset_refresh_id)
            return

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
                wait_for_termination=True,
            ),
            method_name=self.execute_complete.__name__,
        )

    def execute_complete(self, context: Context, event: dict[str, str]) -> None:
        """
        Handle trigger completion and push results to XCom or raise an exception.

        :param context: Airflow context dictionary
        :param event: Event dict from trigger with status and dataset_refresh_id
        """
        if not event:
            return

        # Success - push both ID and status to XCom
        dataset_refresh_id = event.get("dataset_refresh_id")
        dataset_refresh_status = event.get("dataset_refresh_status")

        if dataset_refresh_id:
            context["ti"].xcom_push(
                key=f"{self.task_id}.powerbi_dataset_refresh_id",
                value=dataset_refresh_id,
            )

        if dataset_refresh_status:
            context["ti"].xcom_push(
                key=f"{self.task_id}.powerbi_dataset_refresh_status",
                value=dataset_refresh_status,
            )

        if event["status"] == "error":
            raise AirflowException(event["message"])

        self.log.info(
            "Dataset refresh %s completed with status: %s", dataset_refresh_id, dataset_refresh_status
        )


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
