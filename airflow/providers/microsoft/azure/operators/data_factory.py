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

from typing import TYPE_CHECKING, Any, Sequence

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator, BaseOperatorLink, XCom
from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryHook,
    AzureDataFactoryPipelineRunException,
    AzureDataFactoryPipelineRunStatus,
    get_field,
)
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey
    from airflow.utils.context import Context


class AzureDataFactoryPipelineRunLink(LoggingMixin, BaseOperatorLink):
    """Constructs a link to monitor a pipeline run in Azure Data Factory."""

    name = "Monitor Pipeline Run"

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey,
    ) -> str:
        if not isinstance(operator, AzureDataFactoryRunPipelineOperator):
            self.log.info("The %s is not %s class.", operator.__class__, AzureDataFactoryRunPipelineOperator)
            return ""
        run_id = XCom.get_value(key="run_id", ti_key=ti_key)
        conn_id = operator.azure_data_factory_conn_id
        conn = BaseHook.get_connection(conn_id)
        extras = conn.extra_dejson
        subscription_id = get_field(extras, "subscriptionId")
        if not subscription_id:
            raise KeyError(f"Param subscriptionId not found in conn_id '{conn_id}'")
        # Both Resource Group Name and Factory Name can either be declared in the Azure Data Factory
        # connection or passed directly to the operator.
        resource_group_name = operator.resource_group_name or get_field(extras, "resource_group_name")
        factory_name = operator.factory_name or get_field(extras, "factory_name")
        url = (
            f"https://adf.azure.com/en-us/monitoring/pipelineruns/{run_id}"
            f"?factory=/subscriptions/{subscription_id}/"
            f"resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/"
            f"factories/{factory_name}"
        )

        return url


class AzureDataFactoryRunPipelineOperator(BaseOperator):
    """
    Executes a data factory pipeline.

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
        ``AzureDataFactoryHook`` will attempt to use the factory name name provided in the corresponding
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

    def __init__(
        self,
        *,
        pipeline_name: str,
        azure_data_factory_conn_id: str = AzureDataFactoryHook.default_conn_name,
        wait_for_termination: bool = True,
        resource_group_name: str | None = None,
        factory_name: str | None = None,
        reference_pipeline_run_id: str | None = None,
        is_recovery: bool | None = None,
        start_activity_name: str | None = None,
        start_from_failure: bool | None = None,
        parameters: dict[str, Any] | None = None,
        timeout: int = 60 * 60 * 24 * 7,
        check_interval: int = 60,
        **kwargs,
    ) -> None:
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

    def execute(self, context: Context) -> None:
        self.hook = AzureDataFactoryHook(azure_data_factory_conn_id=self.azure_data_factory_conn_id)
        self.log.info("Executing the %s pipeline.", self.pipeline_name)
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
        self.run_id = vars(response)["run_id"]
        # Push the ``run_id`` value to XCom regardless of what happens during execution. This allows for
        # retrieval the executed pipeline's ``run_id`` for downstream tasks especially if performing an
        # asynchronous wait.
        context["ti"].xcom_push(key="run_id", value=self.run_id)

        if self.wait_for_termination:
            self.log.info("Waiting for pipeline run %s to terminate.", self.run_id)

            if self.hook.wait_for_pipeline_run_status(
                run_id=self.run_id,
                expected_statuses=AzureDataFactoryPipelineRunStatus.SUCCEEDED,
                check_interval=self.check_interval,
                timeout=self.timeout,
                resource_group_name=self.resource_group_name,
                factory_name=self.factory_name,
            ):
                self.log.info("Pipeline run %s has completed successfully.", self.run_id)
            else:
                raise AzureDataFactoryPipelineRunException(
                    f"Pipeline run {self.run_id} has failed or has been cancelled."
                )

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
