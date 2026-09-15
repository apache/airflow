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
import warnings
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

from azure.core.exceptions import ResourceNotFoundError

from airflow.providers.common.compat.sdk import (
    AirflowException,
    BaseHook,
    BaseOperator,
    BaseOperatorLink,
    XCom,
    conf,
)
from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryHook,
    AzureDataFactoryPipelineRunException,
    AzureDataFactoryPipelineRunStatus,
    get_field,
)
from airflow.providers.microsoft.azure.triggers.data_factory import AzureDataFactoryTrigger
from airflow.utils.log.logging_mixin import LoggingMixin

_DURABLE_UNSET: object = object()
_MISSING_PIPELINE_RUN_STATUS: str = "Missing"


def _warn_and_disable_durable_pre_3_3(durable: Any) -> bool:
    if durable is not _DURABLE_UNSET:
        warnings.warn(
            message="`durable` has no effect on Airflow versions below 3.3.",
            category=UserWarning,
            stacklevel=3,
        )
    return False


try:
    from airflow.sdk import ResumableJobMixin
except ImportError:

    class ResumableJobMixin:  # type: ignore[no-redef]
        """Compatibility shim for Airflow versions without task state."""

        external_id_key: str = "azure_data_factory_run_id"

        def __init__(self, *, durable: Any = _DURABLE_UNSET, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.durable = _warn_and_disable_durable_pre_3_3(durable)

        def submit_job(self, context: Context) -> JsonValue:
            raise NotImplementedError

        def poll_until_complete(self, external_id: JsonValue, context: Context) -> None:
            raise NotImplementedError

        def get_job_result(self, external_id: JsonValue, context: Context) -> Any:
            raise NotImplementedError

        def execute_resumable(self, context: Context) -> Any:
            external_id: JsonValue = self.submit_job(context=context)
            self.poll_until_complete(external_id=external_id, context=context)
            return self.get_job_result(external_id=external_id, context=context)


if TYPE_CHECKING:
    from pydantic import JsonValue

    from airflow.providers.common.compat.sdk import TaskInstanceKey
    from airflow.sdk import Context


class AzureDataFactoryPipelineRunLink(LoggingMixin, BaseOperatorLink):
    """Construct a link to monitor a pipeline run in Azure Data Factory."""

    name = "Monitor Pipeline Run"

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey,
    ) -> str:
        run_id = XCom.get_value(key="run_id", ti_key=ti_key)
        conn_id = operator.azure_data_factory_conn_id  # type: ignore
        conn = BaseHook.get_connection(conn_id)
        extras = conn.extra_dejson
        subscription_id = get_field(extras, "subscriptionId") or get_field(
            extras, "extra__azure__subscriptionId"
        )
        if not subscription_id:
            raise KeyError(f"Param subscriptionId not found in conn_id '{conn_id}'")
        # Both Resource Group Name and Factory Name can either be declared in the Azure Data Factory
        # connection or passed directly to the operator.
        resource_group_name = operator.resource_group_name or get_field(  # type: ignore
            extras, "resource_group_name"
        )
        factory_name = operator.factory_name or get_field(extras, "factory_name")  # type: ignore
        url = (
            f"https://adf.azure.com/en-us/monitoring/pipelineruns/{run_id}"
            f"?factory=/subscriptions/{subscription_id}/"
            f"resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/"
            f"factories/{factory_name}"
        )

        return url


class AzureDataFactoryRunPipelineOperator(ResumableJobMixin, BaseOperator):
    """
    Execute a data factory pipeline.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureDataFactoryRunPipelineOperator`

    :param azure_data_factory_conn_id: The connection identifier for connecting to Azure Data Factory.
    :param pipeline_name: The name of the pipeline to execute.
    :param wait_for_termination: Flag to wait on a pipeline run's termination.  By default, this feature is
        enabled but could be disabled to perform an asynchronous wait for a long-running pipeline execution
        using the ``AzureDataFactoryPipelineRunSensor``.
    :param resource_group_name: The resource group name. If a value is not passed in to the operator, the
        ``AzureDataFactoryHook`` will attempt to use the resource group name provided in the corresponding
        connection.
    :param factory_name: The data factory name. If a value is not passed in to the operator, the
        ``AzureDataFactoryHook`` will attempt to use the factory name provided in the corresponding
        connection.
    :param reference_pipeline_run_id: The pipeline run identifier. If this run ID is specified the parameters
        of the specified run will be used to create a new run.
    :param is_recovery: Recovery mode flag. If recovery mode is set to `True`, the specified referenced
        pipeline run and the new run will be grouped under the same ``groupId``.
    :param start_activity_name: In recovery mode, the rerun will start from this activity. If not specified,
        all activities will run.
    :param start_from_failure: In recovery mode, if set to true, the rerun will start from failed activities.
        The property will be used only if ``start_activity_name`` is not specified.
    :param parameters: Parameters of the pipeline run. These parameters are referenced in a pipeline via
        ``@pipeline().parameters.parameterName`` and will be used only if the ``reference_pipeline_run_id`` is
        not specified.
    :param timeout: Time in seconds to wait for a pipeline to reach a terminal status for non-asynchronous
        waits. Used only if ``wait_for_termination`` is True.
    :param check_interval: Time in seconds to check on a pipeline run's status for non-asynchronous waits.
        Used only if ``wait_for_termination`` is True.
    :param deferrable: Run operator in deferrable mode.
    :param durable: When True (the default) and the operator waits synchronously, persist the pipeline
        run ID before polling so a retry can reconnect instead of submitting a duplicate. Requires
        Airflow 3.3+; this has no effect on earlier versions.
    """

    template_fields: Sequence[str] = (
        "azure_data_factory_conn_id",
        "resource_group_name",
        "factory_name",
        "pipeline_name",
        "reference_pipeline_run_id",
        "parameters",
    )
    template_fields_renderers = {"parameters": "json"}

    ui_color = "#0678d4"

    operator_extra_links = (AzureDataFactoryPipelineRunLink(),)
    external_id_key: str = "azure_data_factory_run_id"

    def __init__(
        self,
        *,
        pipeline_name: str,
        azure_data_factory_conn_id: str = AzureDataFactoryHook.default_conn_name,
        resource_group_name: str,
        factory_name: str,
        wait_for_termination: bool = True,
        reference_pipeline_run_id: str | None = None,
        is_recovery: bool | None = None,
        start_activity_name: str | None = None,
        start_from_failure: bool | None = None,
        parameters: dict[str, Any] | None = None,
        timeout: int = 60 * 60 * 24 * 7,
        check_interval: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        durable: bool | None = None,
        **kwargs: Any,
    ) -> None:
        if durable is not None:
            kwargs["durable"] = durable
        super().__init__(**kwargs)
        self.azure_data_factory_conn_id = azure_data_factory_conn_id
        self.pipeline_name = pipeline_name
        self.wait_for_termination = wait_for_termination
        self.resource_group_name = resource_group_name
        self.factory_name = factory_name
        self.reference_pipeline_run_id = reference_pipeline_run_id
        self.is_recovery = is_recovery
        self.start_activity_name = start_activity_name
        self.start_from_failure = start_from_failure
        self.parameters = parameters
        self.timeout = timeout
        self.check_interval = check_interval
        self.deferrable = deferrable
        self.run_id: str | None = None
        self._published_run_id: str | None = None

    @cached_property
    def hook(self) -> AzureDataFactoryHook:
        """Create and return an AzureDataFactoryHook (cached)."""
        return AzureDataFactoryHook(azure_data_factory_conn_id=self.azure_data_factory_conn_id)

    def execute(self, context: Context) -> None:
        self.log.info("Executing the %s pipeline.", self.pipeline_name)
        self._published_run_id = None
        if self.wait_for_termination and self.deferrable is False:
            self.execute_resumable(context=context)
            return

        run_id: str = cast("str", self.submit_job(context=context))

        if self.wait_for_termination:
            end_time = time.time() + self.timeout
            pipeline_run_status: str = self.hook.get_pipeline_run_status(
                run_id=run_id,
                resource_group_name=self.resource_group_name,
                factory_name=self.factory_name,
            )
            if pipeline_run_status not in AzureDataFactoryPipelineRunStatus.TERMINAL_STATUSES:
                self.defer(
                    timeout=self.execution_timeout,
                    trigger=AzureDataFactoryTrigger(
                        azure_data_factory_conn_id=self.azure_data_factory_conn_id,
                        run_id=run_id,
                        wait_for_termination=self.wait_for_termination,
                        resource_group_name=self.resource_group_name,
                        factory_name=self.factory_name,
                        check_interval=self.check_interval,
                        end_time=end_time,
                    ),
                    method_name="execute_complete",
                )
            elif pipeline_run_status == AzureDataFactoryPipelineRunStatus.SUCCEEDED:
                self.log.info("Pipeline run %s has completed successfully.", run_id)
            elif pipeline_run_status in AzureDataFactoryPipelineRunStatus.FAILURE_STATES:
                raise AzureDataFactoryPipelineRunException(
                    f"Pipeline run {run_id} has failed or has been cancelled."
                )
        elif self.deferrable is True:
            warnings.warn(
                "Argument `wait_for_termination` is False and `deferrable` is True , hence "
                "`deferrable` parameter doesn't have any effect",
                UserWarning,
                stacklevel=2,
            )

    def submit_job(self, context: Context) -> JsonValue:
        response = self.hook.run_pipeline(
            pipeline_name=self.pipeline_name,
            resource_group_name=self.resource_group_name,
            factory_name=self.factory_name,
            reference_pipeline_run_id=self.reference_pipeline_run_id,
            is_recovery=self.is_recovery,
            start_activity_name=self.start_activity_name,
            start_from_failure=self.start_from_failure,
            parameters=self.parameters,
        )
        run_id: str = response.run_id
        self._set_run_id(run_id=run_id, context=context)
        return run_id

    def get_job_status(self, external_id: JsonValue, context: Context) -> str:
        run_id: str = cast("str", external_id)
        self._set_run_id(run_id=run_id, context=context)
        try:
            return self.hook.get_pipeline_run_status(
                run_id=run_id,
                resource_group_name=self.resource_group_name,
                factory_name=self.factory_name,
            )
        except ResourceNotFoundError:
            return _MISSING_PIPELINE_RUN_STATUS

    def is_job_active(self, status: str) -> bool:
        return status in AzureDataFactoryPipelineRunStatus.INTERMEDIATE_STATES

    def is_job_succeeded(self, status: str) -> bool:
        return status == AzureDataFactoryPipelineRunStatus.SUCCEEDED

    def poll_until_complete(self, external_id: JsonValue, context: Context) -> None:
        run_id: str = cast("str", external_id)
        self._set_run_id(run_id=run_id, context=context)
        self.log.info("Waiting for pipeline run %s to terminate.", run_id)
        if self.hook.wait_for_pipeline_run_status(
            run_id=run_id,
            expected_statuses=AzureDataFactoryPipelineRunStatus.SUCCEEDED,
            resource_group_name=self.resource_group_name,
            factory_name=self.factory_name,
            check_interval=self.check_interval,
            timeout=self.timeout,
        ):
            self.log.info("Pipeline run %s has completed successfully.", run_id)
            return
        raise AzureDataFactoryPipelineRunException(f"Pipeline run {run_id} has failed or has been cancelled.")

    def get_job_result(self, external_id: JsonValue, context: Context) -> None:
        run_id: str = cast("str", external_id)
        self._set_run_id(run_id=run_id, context=context)

    def _set_run_id(self, *, run_id: str, context: Context) -> None:
        self.run_id = run_id
        if self._published_run_id != run_id:
            context["ti"].xcom_push(key="run_id", value=run_id)
            self._published_run_id = run_id

    def execute_complete(self, context: Context, event: dict[str, str]) -> None:
        """
        Return immediately - callback for when the trigger fires.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event:
            if event["status"] == "error":
                raise AirflowException(event["message"])
            self.log.info(event["message"])

    def on_kill(self) -> None:
        if self.run_id:
            self.hook.cancel_pipeline_run(
                run_id=self.run_id,
                resource_group_name=self.resource_group_name,
                factory_name=self.factory_name,
            )

            # Check to ensure the pipeline run was cancelled as expected.
            if self.hook.wait_for_pipeline_run_status(
                run_id=self.run_id,
                expected_statuses=AzureDataFactoryPipelineRunStatus.CANCELLED,
                check_interval=self.check_interval,
                timeout=self.timeout,
                resource_group_name=self.resource_group_name,
                factory_name=self.factory_name,
            ):
                self.log.info("Pipeline run %s has been cancelled successfully.", self.run_id)
            else:
                raise AzureDataFactoryPipelineRunException(f"Pipeline run {self.run_id} was not cancelled.")
