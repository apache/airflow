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

from typing import TYPE_CHECKING, Optional, Sequence

from azure.synapse.spark.models import SparkBatchJobOptions

from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.synapse import AzureSynapseHook, AzureSynapseSparkBatchRunStatus

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AzureSynapseRunSparkBatchOperator(BaseOperator):
    """
    Executes a Spark job on Azure Synapse.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureSynapseRunSparkBatchOperator`

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
        "azure_synapse_conn_id",
        "spark_pool",
        # "pipeline_name",
        # "reference_pipeline_run_id",
        # "parameters",
    )
    template_fields_renderers = {"parameters": "json"}

    ui_color = "#0678d4"

    # operator_extra_links = (AzureDataFactoryPipelineRunLink(),)

    def __init__(
        self,
        *,
        # pipeline_name: str,
        azure_synapse_conn_id: str = AzureSynapseHook.default_conn_name,
        wait_for_termination: bool = True,
        # resource_group_name: Optional[str] = None,
        spark_pool: Optional[str] = None,
        # payload: Dict[str, Any],
        payload: SparkBatchJobOptions,
        # reference_pipeline_run_id: Optional[str] = None,
        # is_recovery: Optional[bool] = None,
        # start_activity_name: Optional[str] = None,
        # start_from_failure: Optional[bool] = None,
        # parameters: Optional[Dict[str, Any]] = None,
        timeout: int = 60 * 60 * 24 * 7,
        check_interval: int = 60,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.azure_synapse_conn_id = azure_synapse_conn_id
        # self.pipeline_name = pipeline_name
        self.wait_for_termination = wait_for_termination
        # self.resource_group_name = resource_group_name
        self.spark_pool = spark_pool
        self.payload = payload
        # self.reference_pipeline_run_id = reference_pipeline_run_id
        # self.is_recovery = is_recovery
        # self.start_activity_name = start_activity_name
        # self.start_from_failure = start_from_failure
        # self.parameters = parameters
        self.timeout = timeout
        self.check_interval = check_interval

    def execute(self, context: "Context") -> None:
        self.hook = AzureSynapseHook(azure_synapse_conn_id=self.azure_synapse_conn_id)
        self.log.info("Executing the Synapse spark job.")
        response = self.hook.run_spark_job(payload=self.payload)
        self.log.info(response)
        print("# needs to be an int here")
        self.job_id = vars(response)["id"]
        print(type(self.job_id))
        # Push the ``run_id`` value to XCom regardless of what happens during execution. This allows for
        # retrieval the executed pipeline's ``run_id`` for downstream tasks especially if performing an
        # asynchronous wait.
        context["ti"].xcom_push(key="job_id", value=self.job_id)

        if self.wait_for_termination:
            self.log.info("Waiting for job run %s to terminate.", self.job_id)

            if self.hook.wait_for_job_run_status(
                job_id=self.job_id,
                expected_statuses=AzureSynapseSparkBatchRunStatus.SUCCESS,
                check_interval=self.check_interval,
                timeout=self.timeout,
            ):
                self.log.info("Job run %s has completed successfully.", self.job_id)
            else:
                raise Exception(f"Job run {self.job_id} has failed or has been cancelled.")

    def on_kill(self) -> None:
        if self.job_id:
            self.hook.cancel_job_run(
                job_id=self.job_id,
            )
            self.log.info("Job run %s has been cancelled successfully.", self.job_id)
