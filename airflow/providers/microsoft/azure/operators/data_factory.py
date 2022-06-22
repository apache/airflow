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

from typing import Any, Dict, Optional

from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryHook,
    AzureDataFactoryPipelineRunException,
    AzureDataFactoryPipelineRunStatus,
)


class AzureDataFactoryRunPipelineOperator(BaseOperator):
    """
    Executes a data factory pipeline.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureDataFactoryRunPipelineOperator`

    :param azure_data_factory_conn_id: The connection identifier for connecting to Azure Data Factory.
    :type azure_data_factory_conn_id: str
    :param pipeline_name: The name of the pipeline to execute.
    :type pipeline_name: str
    :param wait_for_termination: Flag to wait on a pipeline run's termination.  By default, this feature is
        enabled but could be disabled to perform an asynchronous wait for a long-running pipeline execution
        using the ``AzureDataFactoryPipelineRunSensor``.
    :type wait_for_termination: bool
    :param resource_group_name: The resource group name. If a value is not passed in to the operator, the
        ``AzureDataFactoryHook`` will attempt to use the resource group name provided in the corresponding
        connection.
    :type resource_group_name: str
    :param factory_name: The data factory name. If a value is not passed in to the operator, the
        ``AzureDataFactoryHook`` will attempt to use the factory name name provided in the corresponding
        connection.
    :type factory_name: str
    :param reference_pipeline_run_id: The pipeline run identifier. If this run ID is specified the parameters
        of the specified run will be used to create a new run.
    :type reference_pipeline_run_id: str
    :param is_recovery: Recovery mode flag. If recovery mode is set to `True`, the specified referenced
        pipeline run and the new run will be grouped under the same ``groupId``.
    :type is_recovery: bool
    :param start_activity_name: In recovery mode, the rerun will start from this activity. If not specified,
        all activities will run.
    :type start_activity_name: str
    :param start_from_failure: In recovery mode, if set to true, the rerun will start from failed activities.
        The property will be used only if ``start_activity_name`` is not specified.
    :type start_from_failure: bool
    :param parameters: Parameters of the pipeline run. These parameters are referenced in a pipeline via
        ``@pipeline().parameters.parameterName`` and will be used only if the ``reference_pipeline_run_id`` is
        not specified.
    :type parameters: Dict[str, Any]
    :param timeout: Time in seconds to wait for a pipeline to reach a terminal status for non-asynchronous
        waits. Used only if ``wait_for_termination`` is True.
    :type timeout: int
    :param check_interval: Time in seconds to check on a pipeline run's status for non-asynchronous waits.
        Used only if ``wait_for_termination`` is True.
    :type check_interval: int
    """

    template_fields = (
        "azure_data_factory_conn_id",
        "resource_group_name",
        "factory_name",
        "pipeline_name",
        "reference_pipeline_run_id",
        "parameters",
    )
    template_fields_renderers = {"parameters": "json"}

    ui_color = "#0678d4"

    def __init__(
        self,
        *,
        pipeline_name: str,
        azure_data_factory_conn_id: str = AzureDataFactoryHook.default_conn_name,
        wait_for_termination: bool = True,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        reference_pipeline_run_id: Optional[str] = None,
        is_recovery: Optional[bool] = None,
        start_activity_name: Optional[str] = None,
        start_from_failure: Optional[bool] = None,
        parameters: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = 60 * 60 * 24 * 7,
        check_interval: Optional[int] = 60,
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

    def execute(self, context: Dict) -> None:
        self.hook = AzureDataFactoryHook(azure_data_factory_conn_id=self.azure_data_factory_conn_id)
        self.log.info(f"Executing the {self.pipeline_name} pipeline.")
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
            self.log.info(f"Waiting for pipeline run {self.run_id} to terminate.")

            if self.hook.wait_for_pipeline_run_status(
                run_id=self.run_id,
                expected_statuses=AzureDataFactoryPipelineRunStatus.SUCCEEDED,
                check_interval=self.check_interval,
                timeout=self.timeout,
                resource_group_name=self.resource_group_name,
                factory_name=self.factory_name,
            ):
                self.log.info(f"Pipeline run {self.run_id} has completed successfully.")
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
                self.log.info(f"Pipeline run {self.run_id} has been cancelled successfully.")
            else:
                raise AzureDataFactoryPipelineRunException(f"Pipeline run {self.run_id} was not cancelled.")
