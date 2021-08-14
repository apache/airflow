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

import time
from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.azure_data_factory import (
    AzureDataFactoryHook,
    AzureDataFactoryPipelineRunStatus,
)


class AzureDataFactoryRunPipelineOperator(BaseOperator):
    """
    Executes a data factory pipeline.

    :param conn_id: The connection identifier for connecting to Azure Data Factory.
    :type conn_id: str
    :param pipeline_name: The name of the pipeline to execute.
    :type pipeline_name: str
    :param resource_group_name: The resource group name. If a value is not passed in to the operator, the
        ``AzureDataFactoryHook`` will attempt to use the resource group name provided in the corresponding
        connection.
    :type resource_group_name: str
    :param factory_name: The data factory name. If a value is not passed in to the operator, the
        ``AzureDataFactoryHook`` will attempt to use the resource group name provided in the corresponding
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
    :param parameters: Parameters of the pipeline run. These parameters will be used only if the
        ``reference_pipeline_run_id`` is not specified.
    :type start_from_failure: Dict[str, Any]
    :param do_asynchronous_wait: Flag to exit after creating a pipeline run.  Typically this flag would be
        enabled to wait for a long-running pipeline execution using the
        ``AzureDataFactoryPipelineRunStatusSensor`` rather than this operator.
    :type do_asynchronous_wait: bool
    :param status_check_timeout: Time in seconds to wait for a pipeline to reach a terminal status for
        non-asynchronous waits. Use only if ``do_asynchronous_wait`` is False.
    :type status_check_timeout: int
    :param poke_interval: Time in seconds to check on a pipeline run's status for non-asynchronous waits. Use
        only if ``do_asynchronous_wait`` is False.
    :type poke_interval: int
    """

    template_fields = (
        "resource_group_name",
        "factory_name",
        "pipeline_name",
        "reference_pipeline_run_id",
        "parameters",
    )
    template_fields_renderers = {"parameters": "json"}

    def __init__(
        self,
        *,
        conn_id: str,
        pipeline_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        reference_pipeline_run_id: Optional[str] = None,
        is_recovery: Optional[bool] = None,
        start_activity_name: Optional[str] = None,
        start_from_failure: Optional[bool] = None,
        parameters: Optional[Dict[str, Any]] = None,
        do_asynchronous_wait: Optional[bool] = False,
        status_check_timeout: Optional[int] = 60 * 60 * 5,
        poke_interval: Optional[int] = 30,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.pipeline_name = pipeline_name
        self.resource_group_name = resource_group_name
        self.factory_name = factory_name
        self.reference_pipeline_run_id = reference_pipeline_run_id
        self.is_recovery = is_recovery
        self.start_activity_name = start_activity_name
        self.start_from_failure = start_from_failure
        self.parameters = parameters
        self.do_asynchronous_wait = do_asynchronous_wait
        self.status_check_timeout = status_check_timeout
        self.poke_interval = poke_interval

    def execute(self, context: Dict) -> None:
        self.hook = AzureDataFactoryHook(conn_id=self.conn_id)
        self.log.info(f"Executing the '{self.pipeline_name}' pipeline.")
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
        # Push the ``run_id`` value to XCom regardless of what happens during execution. This allows users to
        # retrieve the ``run_id`` as an output of this operator for downstream tasks especially if performing
        # an async wait.
        context["ti"].xcom_push(key="run_id", value=self.run_id)

        if not self.do_asynchronous_wait:
            self.log.info(f"Waiting for run ID {self.run_id} of pipeline '{self.pipeline_name}' to complete.")
            start_time = time.monotonic()
            pipeline_run_status = None
            while pipeline_run_status not in AzureDataFactoryPipelineRunStatus.TERMINAL_STATUSES:
                # Check to see if the pipeline-run duration has exceeded the ``status_check_timeout``
                # configured.
                if start_time + self.status_check_timeout < time.monotonic():
                    raise AirflowException(
                        f"Pipeline run {self.run_id} has not reached a terminal status after "
                        + f"{self.status_check_timeout} seconds."
                    )

                # Wait to check the status of the pipeline based on the ``poke_interval`` configured.
                time.sleep(self.poke_interval)
                self.log.info(f"Checking on the status of run ID {self.run_id}.")
                pipeline_run = self.hook.get_pipeline_run(
                    run_id=self.run_id,
                    factory_name=self.factory_name,
                    resource_group_name=self.resource_group_name,
                )
                pipeline_run_status = pipeline_run.status
                self.log.info(f"Run ID {self.run_id} is in a status of {pipeline_run_status}.")

            if pipeline_run_status == AzureDataFactoryPipelineRunStatus.CANCELLED:
                raise AirflowException(f"Pipeline run {self.run_id} has been cancelled.")

            if pipeline_run_status == AzureDataFactoryPipelineRunStatus.FAILED:
                raise AirflowException(f"Pipeline run {self.run_id} has failed.")

            self.log.info(f"Pipeline run {self.run_id} has completed successfully.")

    def on_kill(self) -> None:
        if self.run_id:
            self.hook.cancel_pipeline_run(
                run_id=self.run_id,
                resource_group_name=self.resource_group_name,
                factory_name=self.factory_name,
            )
