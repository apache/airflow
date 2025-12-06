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
from functools import cached_property
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

from airflow.providers.common.compat.sdk import (
    AirflowException,
    BaseHook,
    BaseOperator,
    BaseOperatorLink,
    XCom,
)
from airflow.providers.microsoft.azure.hooks.synapse import (
    AzureSynapseHook,
    AzureSynapsePipelineHook,
    AzureSynapsePipelineRunException,
    AzureSynapsePipelineRunStatus,
    AzureSynapseSparkBatchRunStatus,
)

if TYPE_CHECKING:
    from azure.synapse.spark.models import SparkBatchJobOptions

    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.utils.context import Context


class AzureSynapseRunSparkBatchOperator(BaseOperator):
    """
    Execute a Spark job on Azure Synapse.

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
        self.job_id: Any = None
        self.azure_synapse_conn_id = azure_synapse_conn_id
        self.wait_for_termination = wait_for_termination
        self.spark_pool = spark_pool
        self.payload = payload
        self.timeout = timeout
        self.check_interval = check_interval

    @cached_property
    def hook(self):
        """Create and return an AzureSynapseHook (cached)."""
        return AzureSynapseHook(azure_synapse_conn_id=self.azure_synapse_conn_id, spark_pool=self.spark_pool)

    def execute(self, context: Context) -> None:
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
                raise AirflowException(f"Job run {self.job_id} has failed or has been cancelled.")

    def on_kill(self) -> None:
        if self.job_id:
            self.hook.cancel_job_run(
                job_id=self.job_id,
            )
            self.log.info("Job run %s has been cancelled successfully.", self.job_id)


class AzureSynapsePipelineRunLink(BaseOperatorLink):
    """Construct a link to monitor a pipeline run in Azure Synapse."""

    name = "Monitor Pipeline Run"

    def get_fields_from_url(self, workspace_url):
        """
        Extract the workspace_name, subscription_id and resource_group from the Synapse workspace url.

        :param workspace_url: The workspace url.
        """
        import re
        from urllib.parse import unquote, urlparse

        pattern = r"https://web\.azuresynapse\.net\?workspace=(.*)"
        match = re.search(pattern, workspace_url)

        if not match:
            raise ValueError(f"Invalid workspace URL format, expected match pattern {pattern!r}.")

        extracted_text = match.group(1)
        parsed_url = urlparse(extracted_text)
        path = unquote(parsed_url.path)
        path_segments = path.split("/")
        if (len_path_segments := len(path_segments)) < 5:
            raise ValueError(f"Workspace expected at least 5 segments, but got {len_path_segments}.")

        return {
            "workspace_name": path_segments[-1],
            "subscription_id": path_segments[2],
            "resource_group": path_segments[4],
        }

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey):
        run_id = XCom.get_value(key="run_id", ti_key=ti_key) or ""
        conn_id = operator.azure_synapse_conn_id  # type: ignore
        conn = BaseHook.get_connection(conn_id)
        self.synapse_workspace_url = conn.host

        fields = self.get_fields_from_url(self.synapse_workspace_url)

        params = {
            "workspace": f"/subscriptions/{fields['subscription_id']}"
            f"/resourceGroups/{fields['resource_group']}/providers/Microsoft.Synapse"
            f"/workspaces/{fields['workspace_name']}",
        }
        encoded_params = urlencode(params)
        base_url = f"https://ms.web.azuresynapse.net/en/monitoring/pipelineruns/{run_id}?"

        return base_url + encoded_params


class AzureSynapseRunPipelineOperator(BaseOperator):
    """
    Execute a Synapse Pipeline.

    :param pipeline_name: The name of the pipeline to execute.
    :param azure_synapse_conn_id: The Airflow connection ID for Azure Synapse.
    :param azure_synapse_workspace_dev_endpoint: The Azure Synapse workspace development endpoint.
    :param wait_for_termination: Flag to wait on a pipeline run's termination.
    :param reference_pipeline_run_id: The pipeline run identifier. If this run ID is specified the parameters
        of the specified run will be used to create a new run.
    :param is_recovery: Recovery mode flag. If recovery mode is set to `True`, the specified referenced
        pipeline run and the new run will be grouped under the same ``groupId``.
    :param start_activity_name: In recovery mode, the rerun will start from this activity. If not specified,
        all activities will run.
    :param parameters: Parameters of the pipeline run. These parameters are referenced in a pipeline via
        ``@pipeline().parameters.parameterName`` and will be used only if the ``reference_pipeline_run_id`` is
        not specified.
    :param timeout: Time in seconds to wait for a pipeline to reach a terminal status for non-asynchronous
        waits. Used only if ``wait_for_termination`` is True.
    :param check_interval: Time in seconds to check on a pipeline run's status for non-asynchronous waits.
        Used only if ``wait_for_termination`` is True.

    """

    template_fields: Sequence[str] = ("azure_synapse_conn_id",)

    operator_extra_links = (AzureSynapsePipelineRunLink(),)

    def __init__(
        self,
        pipeline_name: str,
        azure_synapse_conn_id: str,
        azure_synapse_workspace_dev_endpoint: str,
        wait_for_termination: bool = True,
        reference_pipeline_run_id: str | None = None,
        is_recovery: bool | None = None,
        start_activity_name: str | None = None,
        parameters: dict[str, Any] | None = None,
        timeout: int = 60 * 60 * 24 * 7,
        check_interval: int = 60,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.azure_synapse_conn_id = azure_synapse_conn_id
        self.pipeline_name = pipeline_name
        self.azure_synapse_workspace_dev_endpoint = azure_synapse_workspace_dev_endpoint
        self.wait_for_termination = wait_for_termination
        self.reference_pipeline_run_id = reference_pipeline_run_id
        self.is_recovery = is_recovery
        self.start_activity_name = start_activity_name
        self.parameters = parameters
        self.timeout = timeout
        self.check_interval = check_interval

    @cached_property
    def hook(self):
        """Create and return an AzureSynapsePipelineHook (cached)."""
        return AzureSynapsePipelineHook(
            azure_synapse_conn_id=self.azure_synapse_conn_id,
            azure_synapse_workspace_dev_endpoint=self.azure_synapse_workspace_dev_endpoint,
        )

    def execute(self, context) -> None:
        self.log.info("Executing the %s pipeline.", self.pipeline_name)
        response = self.hook.run_pipeline(
            pipeline_name=self.pipeline_name,
            reference_pipeline_run_id=self.reference_pipeline_run_id,
            is_recovery=self.is_recovery,
            start_activity_name=self.start_activity_name,
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
                expected_statuses=AzureSynapsePipelineRunStatus.SUCCEEDED,
                check_interval=self.check_interval,
                timeout=self.timeout,
            ):
                self.log.info("Pipeline run %s has completed successfully.", self.run_id)
            else:
                raise AzureSynapsePipelineRunException(
                    f"Pipeline run {self.run_id} has failed or has been cancelled."
                )

    def execute_complete(self, event: dict[str, str]) -> None:
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
            self.hook.cancel_run_pipeline(run_id=self.run_id)

            # Check to ensure the pipeline run was cancelled as expected.
            if self.hook.wait_for_pipeline_run_status(
                run_id=self.run_id,
                expected_statuses=AzureSynapsePipelineRunStatus.CANCELLED,
                check_interval=self.check_interval,
                timeout=self.timeout,
            ):
                self.log.info("Pipeline run %s has been cancelled successfully.", self.run_id)
            else:
                raise AzureSynapsePipelineRunException(f"Pipeline run {self.run_id} was not cancelled.")
