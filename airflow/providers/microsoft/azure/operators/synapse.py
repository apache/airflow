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

from typing import TYPE_CHECKING, Sequence

from azure.synapse.spark.models import SparkBatchJobOptions

from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.synapse import AzureSynapseHook, AzureSynapseSparkBatchRunStatus

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AzureSynapseRunSparkBatchOperator(BaseOperator):
    """
    Executes a Spark job on Azure Synapse.

    .. see also::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureSynapseRunSparkBatchOperator`

    :param azure_synapse_conn_id: The connection identifier for connecting to Azure Synapse.
    :param wait_for_termination: Flag to wait on a job run's termination.
    :param spark_pool: The target synapse spark pool used to submit the job
    :param payload: Livy compatible payload which represents the spark job that a user wants to submit
    :param timeout: Time in seconds to wait for a job to reach a terminal status for non-asynchronous
        waits. Used only if ``wait_for_termination`` is True.
    :param check_interval: Time in seconds to check on a job run's status for non-asynchronous waits.
        Used only if ``wait_for_termination`` is True.
    """

    template_fields: Sequence[str] = (
        "azure_synapse_conn_id",
        "spark_pool",
    )
    template_fields_renderers = {"parameters": "json"}

    ui_color = "#0678d4"

    def __init__(
        self,
        *,
        azure_synapse_conn_id: str = AzureSynapseHook.default_conn_name,
        wait_for_termination: bool = True,
        spark_pool: str = "",
        payload: SparkBatchJobOptions,
        timeout: int = 60 * 60 * 24 * 7,
        check_interval: int = 60,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job_id = None
        self.azure_synapse_conn_id = azure_synapse_conn_id
        self.wait_for_termination = wait_for_termination
        self.spark_pool = spark_pool
        self.payload = payload
        self.timeout = timeout
        self.check_interval = check_interval

    def execute(self, context: Context) -> None:
        self.hook = AzureSynapseHook(
            azure_synapse_conn_id=self.azure_synapse_conn_id, spark_pool=self.spark_pool
        )
        self.log.info("Executing the Synapse spark job.")
        response = self.hook.run_spark_job(payload=self.payload)
        self.log.info(response)
        self.job_id = vars(response)["id"]
        # Push the ``job_id`` value to XCom regardless of what happens during execution. This allows for
        # retrieval the executed job's ``id`` for downstream tasks especially if performing an
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
